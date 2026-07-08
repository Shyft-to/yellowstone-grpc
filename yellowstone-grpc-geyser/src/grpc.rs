use {
    crate::{
        block_reconstruction::{BlockReconstructionState, DispatchItem, MessageId},
        config::{ConfigGrpc, GrpcAddress},
        metered::MeteredLayer,
        metrics::{
            self, incr_grpc_method_call_count, subscription_limit_exceeded_inc, DebugClientMessage,
            SubscriberMetrics,
        },
        plugin::{
            filter::{
                encoder::encode_messages,
                limits::FilterLimits,
                message::{FilteredUpdate, FilteredUpdateOneof},
                name::FilterNames,
                Filter,
            },
            message::{CommitmentLevel, Message, MessageBlockMeta, SlotStatus},
            proto::geyser_server::{Geyser, GeyserServer},
        },
        transport::{SpyIncoming, SpyIncomingConfig, DEFAULT_TRAFFIC_REPORTING_THRESHOLD},
        util::stream::{load_aware_channel, LoadAwareReceiver, LoadAwareSender},
        version::GrpcVersionInfo,
    },
    anyhow::Context,
    bytesize::ByteSize,
    log::{error, info},
    solana_clock::{Slot, MAX_RECENT_BLOCKHASHES},
    std::{
        collections::HashMap,
        net::SocketAddr,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, LazyLock, Mutex as StdMutex,
        },
    },
    tokio::{
        fs,
        net::UnixListener,
        sync::{broadcast, mpsc, oneshot, Mutex, RwLock, Semaphore},
        time::{sleep, Duration, Instant},
    },
    tokio_stream::wrappers::UnixListenerStream,
    tokio_util::{sync::CancellationToken, task::TaskTracker},
    tonic::{
        metadata::AsciiMetadataValue,
        service::interceptor,
        transport::{
            server::{Server, TcpConnectInfo, TcpIncoming, TlsConnectInfo},
            Identity, ServerTlsConfig,
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    tonic_health::{pb::health_server::HealthServer, server::health_reporter},
    yellowstone_grpc_proto::{
        prelude::{
            CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeDeshredRequest,
            SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
        },
        prost::Message as ProstMessage,
    },
};

#[derive(Debug)]
struct BlockhashStatus {
    slot: u64,
    processed: bool,
    confirmed: bool,
    finalized: bool,
}

impl BlockhashStatus {
    const fn new(slot: u64) -> Self {
        Self {
            slot,
            processed: false,
            confirmed: false,
            finalized: false,
        }
    }
}

#[derive(Debug, Default)]
struct BlockMetaStorageInner {
    blocks: HashMap<u64, Arc<MessageBlockMeta>>,
    blockhashes: HashMap<String, BlockhashStatus>,
    processed: Option<u64>,
    confirmed: Option<u64>,
    finalized: Option<u64>,
}

#[derive(Debug)]
struct BlockMetaStorage {
    read_sem: Semaphore,
    inner: Arc<RwLock<BlockMetaStorageInner>>,
}

impl BlockMetaStorage {
    fn new(
        unary_concurrency_limit: usize,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> (Self, mpsc::UnboundedSender<Message>) {
        let inner = Arc::new(RwLock::new(BlockMetaStorageInner::default()));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let storage = Arc::clone(&inner);
        task_tracker.spawn(async move {
            const KEEP_SLOTS: u64 = 3;

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("BlockMetaStorage task cancelled");
                        break;
                    },
                    maybe = rx.recv() => {
                        let Some(message) = maybe else {
                            info!("BlockMetaStorage channel closed");
                            break;
                        };
                        let mut storage = storage.write().await;
                        match message {
                            Message::Slot(msg) => {
                                match msg.status {
                                    SlotStatus::Processed => {
                                        storage.processed.replace(msg.slot);
                                    }
                                    SlotStatus::Confirmed => {
                                        storage.confirmed.replace(msg.slot);
                                    }
                                    SlotStatus::Finalized => {
                                        storage.finalized.replace(msg.slot);
                                    }
                                    _ => {}
                                }

                                if let Some(blockhash) = storage
                                    .blocks
                                    .get(&msg.slot)
                                    .map(|block| block.blockhash.clone())
                                {
                                    let entry = storage
                                        .blockhashes
                                        .entry(blockhash)
                                        .or_insert_with(|| BlockhashStatus::new(msg.slot));

                                    match msg.status {
                                        SlotStatus::Processed => {
                                            entry.processed = true;
                                        }
                                        SlotStatus::Confirmed => {
                                            entry.confirmed = true;
                                        }
                                        SlotStatus::Finalized => {
                                            entry.finalized = true;
                                        }
                                        _ => {}
                                    }
                                }

                                if msg.status == SlotStatus::Finalized {
                                    if let Some(keep_slot) = msg.slot.checked_sub(KEEP_SLOTS) {
                                        storage.blocks.retain(|slot, _block| *slot >= keep_slot);
                                    }

                                    if let Some(keep_slot) =
                                        msg.slot.checked_sub(MAX_RECENT_BLOCKHASHES as u64 + 32)
                                    {
                                        storage
                                            .blockhashes
                                            .retain(|_blockhash, status| status.slot >= keep_slot);
                                    }
                                }
                            }
                            Message::BlockMeta(msg) => {
                                storage.blocks.insert(msg.slot, msg);
                            }
                            msg => {
                                error!("invalid message in BlockMetaStorage: {msg:?}");
                            }
                        }
                    }
                }
            }
            info!("BlockMetaStorage task exiting");
        });

        (
            Self {
                read_sem: Semaphore::new(unary_concurrency_limit),
                inner,
            },
            tx,
        )
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevel, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
    }

    async fn get_block<F, T>(
        &self,
        handler: F,
        commitment: Option<i32>,
    ) -> Result<Response<T>, Status>
    where
        F: FnOnce(&MessageBlockMeta) -> Option<T>,
    {
        let commitment = Self::parse_commitment(commitment)?;
        let _permit = self.read_sem.acquire().await;
        let storage = self.inner.read().await;

        let slot = match commitment {
            CommitmentLevel::Processed => storage.processed,
            CommitmentLevel::Confirmed => storage.confirmed,
            CommitmentLevel::Finalized => storage.finalized,
        };

        match slot.and_then(|slot| storage.blocks.get(&slot)) {
            Some(block) => match handler(block) {
                Some(resp) => Ok(Response::new(resp)),
                None => Err(Status::internal("failed to build response")),
            },
            None => Err(Status::internal("block is not available yet")),
        }
    }

    async fn is_blockhash_valid(
        &self,
        blockhash: &str,
        commitment: Option<i32>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        let commitment = Self::parse_commitment(commitment)?;
        let _permit = self.read_sem.acquire().await;
        let storage = self.inner.read().await;

        if storage.blockhashes.len() < MAX_RECENT_BLOCKHASHES + 32 {
            return Err(Status::internal("startup"));
        }

        let slot = match commitment {
            CommitmentLevel::Processed => storage.processed,
            CommitmentLevel::Confirmed => storage.confirmed,
            CommitmentLevel::Finalized => storage.finalized,
        }
        .ok_or_else(|| Status::internal("startup"))?;

        let valid = storage
            .blockhashes
            .get(blockhash)
            .map(|status| match commitment {
                CommitmentLevel::Processed => status.processed,
                CommitmentLevel::Confirmed => status.confirmed,
                CommitmentLevel::Finalized => status.finalized,
            })
            .unwrap_or(false);

        Ok(Response::new(IsBlockhashValidResponse { valid, slot }))
    }
}

type BroadcastedMessage = (CommitmentLevel, Arc<Vec<(u64, Message)>>);

pub(crate) enum ReplayedResponse {
    Messages(Vec<(u64, Message)>),
    Lagged(Slot),
}

type ReplayStoredSlotsRequest = (CommitmentLevel, Slot, oneshot::Sender<ReplayedResponse>);

type SubscriptionTracker = Arc<StdMutex<HashMap<String, usize>>>;

/// Drains and broadcasts a `Processed`-commitment batch, if non-empty.
/// Shared by `geyser_dispatch` and `block_reconstruction_dispatch`, which
/// each maintain their own `processed_messages` batch/flush cadence but
/// hit the exact same encode+broadcast+reset sequence at flush time.
fn flush_processed_batch(
    broadcast_tx: &broadcast::Sender<BroadcastedMessage>,
    processed_messages: &mut Vec<(u64, Message)>,
    processed_messages_max: usize,
) {
    if processed_messages.is_empty() {
        return;
    }
    metrics::GEYSER_BATCH_SIZE.observe(processed_messages.len() as f64);
    encode_messages(processed_messages);
    let flushed = std::mem::replace(
        processed_messages,
        Vec::with_capacity(processed_messages_max),
    );
    let _ = broadcast_tx.send((CommitmentLevel::Processed, flushed.into()));
}

static CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR: LazyLock<
    StdMutex<HashMap<SocketAddr, usize>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

/// Test-only hook letting tests pause `block_reconstruction_dispatch`'s
/// per-item processing to simulate reconstruction-thread backpressure —
/// used to prove the dispatch/reconstruction decoupling Task 6b introduces
/// and to exercise reconnect-during-backpressure behavior. Entirely
/// `#[cfg(test)]`: this whole module, and the single call site that checks
/// it inside `block_reconstruction_dispatch`, are compiled out of (and
/// therefore unreachable from) any non-test build.
#[cfg(test)]
pub(crate) mod reconstruction_test_gate {
    use std::{
        cell::RefCell,
        sync::{Arc, Condvar, Mutex},
    };

    #[derive(Default)]
    pub(crate) struct Gate {
        closed: Mutex<bool>,
        cond: Condvar,
    }

    impl Gate {
        pub(crate) fn new_closed() -> Arc<Self> {
            Arc::new(Self {
                closed: Mutex::new(true),
                cond: Condvar::new(),
            })
        }

        pub(crate) fn release(&self) {
            *self.closed.lock().unwrap() = false;
            self.cond.notify_all();
        }

        fn wait_while_closed(&self) {
            let mut closed = self.closed.lock().unwrap();
            while *closed {
                closed = self.cond.wait(closed).unwrap();
            }
        }
    }

    thread_local! {
        static CURRENT: RefCell<Option<Arc<Gate>>> = const { RefCell::new(None) };
    }

    /// Installs `gate` for the *calling* OS thread only. Must be called
    /// from within the block-reconstruction thread's own spawn closure,
    /// before it starts running `block_reconstruction_dispatch`, so the
    /// thread-local is set on the same OS thread that later checks it.
    pub(crate) fn install(gate: Arc<Gate>) {
        CURRENT.with(|cell| *cell.borrow_mut() = Some(gate));
    }

    /// Blocks the calling thread if a gate was installed for it and that
    /// gate is still closed. A no-op if no gate was installed, which is
    /// the case for every real (non-test-driven) reconstruction thread.
    pub(crate) fn wait_if_installed() {
        let gate = CURRENT.with(|cell| cell.borrow().clone());
        if let Some(gate) = gate {
            gate.wait_while_closed();
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ClientSnapshotReplayError {
    #[error("gRPC connection closed")]
    ClientGrpcConnectionClosed,
    #[error("client session is cancelled by plugin")]
    Cancelled,
}

struct ClientSession {
    id: usize,
    subscriber_id: String,
    endpoint: String,
    filter: Filter,
    debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    cancellation_token: CancellationToken,
    disconnect_reason: &'static str,
    maybe_remote_peer_sk_addr: Option<SocketAddr>,
    subscription_tracker: SubscriptionTracker,
    metrics: SubscriberMetrics,
}

impl ClientSession {
    fn new(
        id: usize,
        subscriber_id: Option<String>,
        endpoint: String,
        maybe_remote_peer_sk_addr: Option<SocketAddr>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        cancellation_token: CancellationToken,
        subscription_tracker: SubscriptionTracker,
    ) -> Self {
        let filter = Filter::default();
        let subscriber_id = subscriber_id.unwrap_or("UNKNOWN".to_owned());
        let metrics = SubscriberMetrics::new(&subscriber_id);
        if let Some(remote_peer_sk_addr) = maybe_remote_peer_sk_addr {
            let mut subscriptions_per_remote_addr =
                CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR
                    .lock()
                    .expect("CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR mutex poisoned");
            let count = subscriptions_per_remote_addr
                .entry(remote_peer_sk_addr)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            metrics::set_grpc_concurrent_subscribe_per_tcp_connection(
                remote_peer_sk_addr.to_string(),
                *count as u64,
            );
        }
        metrics::update_subscriptions(&endpoint, None, Some(&filter));
        DebugClientMessage::maybe_send(&debug_client_tx, || DebugClientMessage::UpdateFilter {
            id,
            filter: Box::new(filter.clone()),
        });
        info!("client #{id} ({subscriber_id}): new");
        Self {
            id,
            subscriber_id,
            endpoint,
            filter,
            debug_client_tx,
            cancellation_token,
            disconnect_reason: "unknown",
            maybe_remote_peer_sk_addr,
            subscription_tracker,
            metrics,
        }
    }

    fn set_filter(&mut self, new_filter: Filter) {
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), Some(&new_filter));
        DebugClientMessage::maybe_send(&self.debug_client_tx, || {
            DebugClientMessage::UpdateFilter {
                id: self.id,
                filter: Box::new(new_filter.clone()),
            }
        });
        self.filter = new_filter;
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        {
            let mut tracker = self
                .subscription_tracker
                .lock()
                .expect("subscription_tracker mutex poisoned");
            if let Some(count) = tracker.get_mut(&self.subscriber_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    tracker.remove(&self.subscriber_id);
                }
            }
        }
        if let Some(remote_peer_sk_addr) = self.maybe_remote_peer_sk_addr {
            let mut subscriptions_per_remote_addr =
                CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR
                    .lock()
                    .expect("CONCURRENT_SUBSCRIPTIONS_PER_REMOTE_PEER_SK_ADDR mutex poisoned");
            if let Some(count) = subscriptions_per_remote_addr.get_mut(&remote_peer_sk_addr) {
                if *count > 1 {
                    *count -= 1;
                    metrics::set_grpc_concurrent_subscribe_per_tcp_connection(
                        remote_peer_sk_addr.to_string(),
                        *count as u64,
                    );
                } else {
                    subscriptions_per_remote_addr.remove(&remote_peer_sk_addr);
                    metrics::set_grpc_concurrent_subscribe_per_tcp_connection(
                        remote_peer_sk_addr.to_string(),
                        0,
                    );
                    metrics::remove_grpc_concurrent_subscribe_per_tcp_connection(
                        remote_peer_sk_addr.to_string(),
                    );
                }
            }
        }
        self.metrics.set_queue_size(0);
        metrics::incr_client_disconnect(&self.subscriber_id, self.disconnect_reason);
        metrics::update_subscriptions(&self.endpoint, Some(&self.filter), None);
        DebugClientMessage::maybe_send(&self.debug_client_tx, || DebugClientMessage::Removed {
            id: self.id,
        });
        info!(
            "client #{} ({}): removed ({})",
            self.id, self.subscriber_id, self.disconnect_reason
        );
        self.cancellation_token.cancel();
    }
}

enum Listener {
    Tcp(TcpIncoming),
    Unix(PathBuf, UnixListenerStream), // path needed to remove the socket file on exit
}

#[derive(Clone)]
struct XTokenInterceptor {
    x_token: Option<AsciiMetadataValue>,
}

impl interceptor::Interceptor for XTokenInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(x_token) = &self.x_token {
            match request.metadata().get("x-token") {
                Some(token) if token == x_token => Ok(request),
                _ => Err(Status::unauthenticated("No valid auth token")),
            }
        } else {
            Ok(request)
        }
    }
}

/// `JoinHandle`s for the CPU-pinned `geyser-dispatch`/`block-reconstruction`
/// thread pair, returned by `GrpcService::create` so the plugin can join
/// them cleanly at shutdown instead of leaving them to become orphaned.
/// `None` when `geyser_dispatch_cpu_core` is unset and the async
/// `geyser_loop` fallback (a `task_tracker`-managed tokio task, already
/// joined via `task_tracker`/`runtime.shutdown_timeout`) is used instead.
#[derive(Debug)]
pub struct DispatchThreadHandles {
    pub geyser_dispatch: std::thread::JoinHandle<()>,
    pub block_reconstruction: std::thread::JoinHandle<()>,
}

#[derive(Debug, Clone)]
pub struct GrpcService {
    config_snapshot_client_channel_capacity: usize,
    config_channel_capacity: usize,
    config_filter_limits: Arc<FilterLimits>,
    subscription_limit: usize,
    subscription_limit_enforce: bool,
    subscription_tracker: SubscriptionTracker,
    blocks_meta: Option<Arc<BlockMetaStorage>>,
    subscribe_id: Arc<AtomicUsize>,
    snapshot_rx: Arc<Mutex<Option<crossbeam_channel::Receiver<Box<Message>>>>>,
    broadcast_tx: broadcast::Sender<BroadcastedMessage>,
    replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
    replay_first_available_slot: Option<Arc<AtomicU64>>,
    debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    filter_name_size_limit: usize,
    filter_names_size_limit: usize,
    filter_names_cleanup_interval: Duration,
}

impl GrpcService {
    #[allow(clippy::type_complexity)]
    pub async fn create(
        config: ConfigGrpc,
        debug_clients_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        is_reload: bool,
        service_cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> anyhow::Result<(
        Option<crossbeam_channel::Sender<Box<Message>>>,
        mpsc::UnboundedSender<Message>,
        Option<DispatchThreadHandles>,
    )> {
        // Bind all configured addresses (TCP or Unix domain socket)
        let mut listeners = Vec::new();
        for addr in &config.address.inner {
            let listener = match addr {
                GrpcAddress::Tcp(addr) => {
                    let incoming = TcpIncoming::bind(*addr)?
                        .with_nodelay(Some(true))
                        .with_keepalive(Some(Duration::from_secs(20)));
                    Listener::Tcp(incoming)
                }
                GrpcAddress::Unix { path, mode } => {
                    if config.tls_config.is_some() {
                        log::warn!(
                            "TLS config is ignored for Unix domain socket: {}",
                            path.display()
                        );
                    }
                    if let Err(e) = std::fs::remove_file(path) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            return Err(e.into());
                        }
                    }
                    let uds = UnixListener::bind(path)?;
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(*mode))?;
                    Listener::Unix(path.clone(), UnixListenerStream::new(uds))
                }
            };
            listeners.push(listener);
        }

        // Snapshot channel
        let (snapshot_tx, snapshot_rx) = match config.snapshot_plugin_channel_capacity {
            Some(cap) if !is_reload => {
                let (tx, rx) = crossbeam_channel::bounded(cap);
                (Some(tx), Some(rx))
            }
            _ => (None, None),
        };

        // Blocks meta storage
        let (blocks_meta, blocks_meta_tx) = if config.unary_disabled {
            (None, None)
        } else {
            let (blocks_meta, blocks_meta_tx) = BlockMetaStorage::new(
                config.unary_concurrency_limit,
                service_cancellation_token.child_token(),
                task_tracker.clone(),
            );
            (Some(blocks_meta), Some(blocks_meta_tx))
        };

        // Messages to clients combined by commitment
        let (broadcast_tx, _) = broadcast::channel(config.channel_capacity);
        let (replay_first_available_slot, replay_stored_slots_tx, replay_stored_slots_rx) =
            if config.replay_stored_slots == 0 {
                (None, None, None)
            } else {
                let (tx, rx) = mpsc::channel(1);
                (Some(Arc::new(AtomicU64::new(u64::MAX))), Some(tx), Some(rx))
            };

        // Read TLS identity once (async, before per-listener loop)
        let tls_identity = match &config.tls_config {
            Some(tls) => {
                let (cert, key) =
                    tokio::try_join!(fs::read(&tls.cert_path), fs::read(&tls.key_path))
                        .context("failed to load tls_config files")?;
                Some(Identity::from_pem(cert, key))
            }
            None => None,
        };

        // Capture traffic reporting threshold before config is moved
        let traffic_reporting_threshold = config
            .traffic_reporting_byte_threhsold
            .unwrap_or_else(|| ByteSize::b(DEFAULT_TRAFFIC_REPORTING_THRESHOLD));

        // Save HTTP/2 settings (all Copy) for use inside spawned tasks
        let http2_adaptive_window = config.server_http2_adaptive_window;
        let http2_keepalive_interval = config.server_http2_keepalive_interval;
        let http2_keepalive_timeout = config.server_http2_keepalive_timeout;
        let initial_connection_window_size = config.server_initial_connection_window_size;
        let initial_stream_window_size = config.server_initial_stream_window_size;

        // Build the shared GeyserServer (Clone-able because GrpcService: Clone)
        let max_decoding_message_size = config.max_decoding_message_size;
        let mut service = GeyserServer::new(Self {
            config_snapshot_client_channel_capacity: config.snapshot_client_channel_capacity,
            config_channel_capacity: config.channel_capacity,
            config_filter_limits: Arc::new(config.filter_limits),
            subscription_limit: config.subscription_limit,
            subscription_limit_enforce: config.subscription_limit_enforce,
            subscription_tracker: Arc::new(StdMutex::new(HashMap::new())),
            blocks_meta: blocks_meta.map(Arc::new),
            subscribe_id: Arc::new(AtomicUsize::new(0)),
            snapshot_rx: Arc::new(Mutex::new(snapshot_rx)),
            broadcast_tx: broadcast_tx.clone(),
            replay_stored_slots_tx,
            replay_first_available_slot: replay_first_available_slot.clone(),
            debug_clients_tx,
            cancellation_token: service_cancellation_token.clone(),
            task_tracker: task_tracker.clone(),
            filter_name_size_limit: config.filter_name_size_limit,
            filter_names_size_limit: config.filter_names_size_limit,
            filter_names_cleanup_interval: config.filter_names_cleanup_interval,
        })
        .max_decoding_message_size(max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        // Run geyser message loop
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();

        // Warn if replay buffer is too small for auto-reconnect
        if config.replay_stored_slots < 150 {
            log::warn!(
                "replay_stored_slots={} may be too low for auto-reconnect; recommend >= 150",
                config.replay_stored_slots
            );
        }

        let processed_messages_max = config.processed_messages_max;
        let replay_stored_slots = config.replay_stored_slots;

        let dispatch_threads = if let Some(cpu_core) = config.geyser_dispatch_cpu_core {
            Some(Self::spawn_dispatch_threads(
                cpu_core,
                messages_rx,
                blocks_meta_tx,
                broadcast_tx,
                replay_stored_slots_rx,
                replay_first_available_slot,
                replay_stored_slots,
                processed_messages_max,
            ))
        } else {
            task_tracker.spawn(async move {
                Self::geyser_loop(
                    messages_rx,
                    blocks_meta_tx,
                    broadcast_tx,
                    replay_stored_slots_rx,
                    replay_first_available_slot,
                    replay_stored_slots,
                    processed_messages_max,
                )
                .await;
            });
            None
        };

        // Health check service
        let (health_reporter, health_service) = health_reporter();
        health_reporter.set_serving::<GeyserServer<Self>>().await;

        let x_token = config
            .x_token
            .map(|t| t.parse::<AsciiMetadataValue>())
            .transpose()
            .context("invalid x_token value")?;

        let shutdown_grpc = service_cancellation_token.child_token();

        // Spawn one server task per listener
        for listener in listeners {
            let shutdown = shutdown_grpc.clone();
            let tls_identity = tls_identity.clone();
            let x_token = x_token.clone();
            let health_service = health_service.clone();
            let service = service.clone();

            task_tracker.spawn(async move {
                if let Err(e) = GrpcService::serve_listener(
                    listener,
                    tls_identity,
                    http2_adaptive_window,
                    http2_keepalive_interval,
                    http2_keepalive_timeout,
                    initial_connection_window_size,
                    initial_stream_window_size,
                    x_token,
                    health_service,
                    service,
                    traffic_reporting_threshold,
                    shutdown.clone(),
                )
                .await
                {
                    error!("gRPC listener failed: {e}");
                    shutdown.cancel();
                }
            });
        }

        Ok((snapshot_tx, messages_tx, dispatch_threads))
    }

    /// Spawns the CPU-pinned `geyser-dispatch`/`block-reconstruction`
    /// thread pair exactly as `GrpcService::create` wires them for
    /// `geyser_dispatch_cpu_core`-configured deployments, returning their
    /// `JoinHandle`s. Extracted into its own function so `create()` and its
    /// shutdown-wiring tests exercise the identical spawn code.
    #[allow(clippy::too_many_arguments)]
    fn spawn_dispatch_threads(
        cpu_core: usize,
        messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
        processed_messages_max: usize,
    ) -> DispatchThreadHandles {
        let (reconstruction_tx, reconstruction_rx) = mpsc::unbounded_channel();

        // One shared, atomic-backed id space: geyser_dispatch mints ids for
        // raw messages, the reconstruction thread mints ids for what it
        // synthesizes (sealed Block, backfilled ancestor Slot messages) —
        // both must agree on one monotonic space for client_loop's
        // replay-path sort_by_key to stay correct now that they're
        // independent broadcasters.
        let msgid_gen = MessageId::default();
        let dispatch_msgid_gen = msgid_gen.clone();
        let dispatch_broadcast_tx = broadcast_tx.clone();

        let block_reconstruction = std::thread::Builder::new()
            .name("block-reconstruction".into())
            .spawn(move || {
                Self::block_reconstruction_dispatch(
                    reconstruction_rx,
                    broadcast_tx,
                    replay_stored_slots_rx,
                    replay_first_available_slot,
                    replay_stored_slots,
                    processed_messages_max,
                    msgid_gen,
                );
            })
            .expect("failed to spawn block-reconstruction thread");

        let geyser_dispatch = std::thread::Builder::new()
            .name("geyser-dispatch".into())
            .spawn(move || {
                if let Err(e) = crate::util::cpu_core_affinity::set_thread_affinity(&[cpu_core]) {
                    log::warn!("geyser-dispatch: failed to pin to CPU {cpu_core}: {e}");
                }
                Self::geyser_dispatch(
                    messages_rx,
                    blocks_meta_tx,
                    reconstruction_tx,
                    dispatch_broadcast_tx,
                    dispatch_msgid_gen,
                    processed_messages_max,
                );
            })
            .expect("failed to spawn geyser-dispatch thread");

        DispatchThreadHandles {
            geyser_dispatch,
            block_reconstruction,
        }
    }

    async fn geyser_loop(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
        processed_messages_max: usize,
    ) {
        let processed_messages_max = processed_messages_max.max(1);
        const PROCESSED_MESSAGES_SLEEP: Duration = Duration::from_millis(10);

        let mut state = BlockReconstructionState::new(
            blocks_meta_tx,
            replay_first_available_slot,
            replay_stored_slots,
        );
        let mut processed_messages = Vec::with_capacity(processed_messages_max);
        let processed_sleep = sleep(PROCESSED_MESSAGES_SLEEP);
        tokio::pin!(processed_sleep);
        let (_tx, rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(rx);

        loop {
            tokio::select! {
                maybe = messages_rx.recv() => {
                    let Some(message) = maybe else {
                        info!("Geyser loop: messages channel closed");
                        break;
                    };

                    for item in state.on_message(message) {
                        // geyser_loop is single-threaded and broadcasts every
                        // item (raw or derived) on Processed itself — unlike
                        // the split geyser_dispatch/block_reconstruction_dispatch
                        // pipeline, is_raw_message doesn't apply here.
                        let DispatchItem { message, confirmed_messages, finalized_messages, .. } =
                            item;
                        if matches!(&message.1, Message::Slot(_)) {
                            // processed
                            processed_messages.push(message);
                            metrics::GEYSER_BATCH_SIZE.observe(processed_messages.len() as f64);
                            encode_messages(&processed_messages);
                            let _ =
                                broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                            processed_messages = Vec::with_capacity(processed_messages_max);
                            processed_sleep
                                .as_mut()
                                .reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);

                            // confirmed
                            if let Some(confirmed_messages) = confirmed_messages {
                                let _ = broadcast_tx
                                    .send((CommitmentLevel::Confirmed, confirmed_messages.into()));
                            }

                            // finalized
                            if let Some(finalized_messages) = finalized_messages {
                                let _ = broadcast_tx
                                    .send((CommitmentLevel::Finalized, finalized_messages.into()));
                            }
                        } else {
                            processed_messages.push(message);
                            if processed_messages.len() >= processed_messages_max
                                || confirmed_messages.is_some()
                                || finalized_messages.is_some()
                            {
                                metrics::GEYSER_BATCH_SIZE.observe(processed_messages.len() as f64);
                                encode_messages(&processed_messages);
                                let _ = broadcast_tx
                                    .send((CommitmentLevel::Processed, processed_messages.into()));
                                processed_messages = Vec::with_capacity(processed_messages_max);
                                processed_sleep
                                    .as_mut()
                                    .reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);
                            }

                            if let Some(confirmed_messages) = confirmed_messages {
                                let _ = broadcast_tx
                                    .send((CommitmentLevel::Confirmed, confirmed_messages.into()));
                            }

                            if let Some(finalized_messages) = finalized_messages {
                                let _ = broadcast_tx
                                    .send((CommitmentLevel::Finalized, finalized_messages.into()));
                            }
                        }
                    }
                }
                () = &mut processed_sleep => {
                    if !processed_messages.is_empty() {
                        metrics::GEYSER_BATCH_SIZE.observe(processed_messages.len() as f64);
                        encode_messages(&processed_messages);
                        let _ = broadcast_tx.send((CommitmentLevel::Processed, processed_messages.into()));
                        processed_messages = Vec::with_capacity(processed_messages_max);
                    }
                    processed_sleep.as_mut().reset(Instant::now() + PROCESSED_MESSAGES_SLEEP);
                }
                Some((commitment, replay_slot, tx)) = replay_stored_slots_rx.recv() => {
                    state.service_replay(commitment, replay_slot, tx);
                }
                else => break,
            }
        }

        info!("Geyser loop exiting");
    }

    /// Mints a msgid for each raw incoming message and, unlike the
    /// pre-decoupling forwarder this replaces, itself batches/encodes/
    /// broadcasts these raw pass-through messages on
    /// `CommitmentLevel::Processed` directly — this is the actual latency
    /// win: Processed delivery no longer waits on the block-reconstruction
    /// thread's BTreeMap bookkeeping. Each message (already carrying the id
    /// this thread just broadcast it under) is then forwarded to the
    /// block-reconstruction thread's channel, which independently derives
    /// and broadcasts Confirmed/Finalized (for every message) and
    /// Processed-only derived messages (sealed Block, backfilled ancestor
    /// Slot messages) — never re-broadcasting the raw message itself, since
    /// this thread already did.
    ///
    /// Runs on a dedicated, CPU-pinned std::thread and spin-loops via
    /// try_recv() to avoid tokio scheduler wake latency. Batching/flush
    /// cadence mirrors the block-reconstruction thread's own Processed
    /// cadence: flush immediately on every `Message::Slot`, flush whenever
    /// try_recv() drains empty, or flush at `processed_messages_max`.
    fn geyser_dispatch(
        mut messages_rx: mpsc::UnboundedReceiver<Message>,
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        reconstruction_tx: mpsc::UnboundedSender<(u64, Message)>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        msgid_gen: MessageId,
        processed_messages_max: usize,
    ) {
        let processed_messages_max = processed_messages_max.max(1);
        let mut processed_messages = Vec::with_capacity(processed_messages_max);

        loop {
            match messages_rx.try_recv() {
                Ok(message) => {
                    if let Some(blocks_meta_tx) = &blocks_meta_tx {
                        if matches!(&message, Message::Slot(_) | Message::BlockMeta(_)) {
                            let _ = blocks_meta_tx.send(message.clone());
                        }
                    }

                    let msgid = msgid_gen.next();
                    let is_slot = matches!(&message, Message::Slot(_));

                    if reconstruction_tx.send((msgid, message.clone())).is_err() {
                        info!("Geyser dispatch: block-reconstruction channel closed");
                        break;
                    }

                    processed_messages.push((msgid, message));
                    if is_slot || processed_messages.len() >= processed_messages_max {
                        flush_processed_batch(
                            &broadcast_tx,
                            &mut processed_messages,
                            processed_messages_max,
                        );
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    flush_processed_batch(
                        &broadcast_tx,
                        &mut processed_messages,
                        processed_messages_max,
                    );
                    std::hint::spin_loop();
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    info!("Geyser dispatch: messages channel closed");
                    break;
                }
            }
        }

        info!("Geyser dispatch thread exiting");
    }

    /// Owns the `BlockReconstructionState` (BTreeMap bookkeeping, gc,
    /// sealing, replay buffer). Runs on its own dedicated std::thread (fed
    /// by `geyser_dispatch`'s forwarding channel), deliberately *not*
    /// CPU-pinned. Broadcasts Confirmed/Finalized for every `DispatchItem`
    /// (unchanged from before the decoupling), but for Processed only
    /// broadcasts the *derived* items it synthesizes itself — the sealed
    /// Block message and backfilled ancestor Slot messages
    /// (`!item.is_raw_message`) — since `geyser_dispatch` already broadcast
    /// the raw pass-through message directly. This is the live-ordering
    /// relaxation this split introduces: these derived Processed messages
    /// can now arrive arbitrarily late relative to later raw Processed
    /// messages, bounded only by this thread's channel backlog; raw
    /// messages themselves stay strictly ordered since `geyser_dispatch`
    /// remains their sole producer and broadcaster.
    #[allow(clippy::too_many_arguments)]
    fn block_reconstruction_dispatch(
        mut messages_rx: mpsc::UnboundedReceiver<(u64, Message)>,
        broadcast_tx: broadcast::Sender<BroadcastedMessage>,
        replay_stored_slots_rx: Option<mpsc::Receiver<ReplayStoredSlotsRequest>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
        processed_messages_max: usize,
        msgid_gen: MessageId,
    ) {
        let processed_messages_max = processed_messages_max.max(1);

        let mut state =
            BlockReconstructionState::new(None, replay_first_available_slot, replay_stored_slots)
                .with_msgid_gen(msgid_gen);
        let mut processed_messages = Vec::with_capacity(processed_messages_max);
        let (_dummy_tx, dummy_rx) = mpsc::channel(1);
        let mut replay_stored_slots_rx = replay_stored_slots_rx.unwrap_or(dummy_rx);

        loop {
            match messages_rx.try_recv() {
                Ok((msgid, message)) => {
                    #[cfg(test)]
                    reconstruction_test_gate::wait_if_installed();

                    for item in state.on_message_with_id(msgid, message) {
                        let DispatchItem {
                            message,
                            is_raw_message,
                            confirmed_messages,
                            finalized_messages,
                        } = item;

                        if !is_raw_message {
                            let is_slot = matches!(&message.1, Message::Slot(_));
                            processed_messages.push(message);
                            if is_slot
                                || processed_messages.len() >= processed_messages_max
                                || confirmed_messages.is_some()
                                || finalized_messages.is_some()
                            {
                                flush_processed_batch(
                                    &broadcast_tx,
                                    &mut processed_messages,
                                    processed_messages_max,
                                );
                            }
                        }

                        if let Some(confirmed_messages) = confirmed_messages {
                            let _ = broadcast_tx
                                .send((CommitmentLevel::Confirmed, confirmed_messages.into()));
                        }

                        if let Some(finalized_messages) = finalized_messages {
                            let _ = broadcast_tx
                                .send((CommitmentLevel::Finalized, finalized_messages.into()));
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    // Inbox drained — flush accumulated derived-Processed messages immediately
                    flush_processed_batch(
                        &broadcast_tx,
                        &mut processed_messages,
                        processed_messages_max,
                    );

                    // Service any pending replay requests while idle
                    if let Ok((commitment, replay_slot, tx)) = replay_stored_slots_rx.try_recv() {
                        state.service_replay(commitment, replay_slot, tx);
                    }

                    std::hint::spin_loop();
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    info!("Block reconstruction: channel closed");
                    break;
                }
            }
        }

        info!("Block reconstruction thread exiting");
    }

    #[allow(clippy::too_many_arguments)]
    async fn client_loop(
        id: usize,
        subscriber_id: Option<String>,
        endpoint: String,
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdate>>,
        mut client_rx: mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        mut snapshot_rx: Option<crossbeam_channel::Receiver<Box<Message>>>,
        mut messages_rx: broadcast::Receiver<BroadcastedMessage>,
        replay_stored_slots_tx: Option<mpsc::Sender<ReplayStoredSlotsRequest>>,
        debug_client_tx: Option<mpsc::UnboundedSender<DebugClientMessage>>,
        maybe_remote_peer_sk_addr: Option<SocketAddr>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
        subscription_tracker: SubscriptionTracker,
    ) {
        let mut session = ClientSession::new(
            id,
            subscriber_id,
            endpoint,
            maybe_remote_peer_sk_addr,
            debug_client_tx,
            cancellation_token,
            subscription_tracker,
        );
        let cancellation_token = session.cancellation_token.clone();

        if let Some(snapshot_rx) = snapshot_rx.take() {
            info!("client #{id}: snapshot requested");
            let result = Self::client_loop_snapshot(
                id,
                &session.endpoint,
                stream_tx.clone(),
                &mut client_rx,
                snapshot_rx,
                &mut session.filter,
                cancellation_token.clone(),
            )
            .await;
            match result {
                Ok(()) => {
                    info!("client #{id}: snapshot stream ended");
                }
                Err(ClientSnapshotReplayError::Cancelled) => {
                    let _ = stream_tx.try_send(Err(Status::internal(
                        "server is shutting down try again later",
                    )));
                    session.disconnect_reason = "server_shutdown";
                    return;
                }
                Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed) => {
                    info!("client #{id}: grpc connection closed");
                    session.disconnect_reason = "client_closed";
                    return;
                }
            }
        } else {
            info!("client #{id}: no snapshot requested");
        }

        'outer: loop {
            session.metrics.set_queue_size(stream_tx.queue_size());

            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("client #{id}: cancelled");
                    let _ = stream_tx.try_send(Err(Status::unavailable("server is shutting down try again later")));
                    session.disconnect_reason = "server_shutdown";
                    break 'outer;
                }
                mut message = client_rx.recv() => {
                    // forward to latest filter
                    loop {
                        match client_rx.try_recv() {
                            Ok(message_new) => {
                                message = Some(message_new);
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                message = None;
                                break;
                            }
                        }
                    }

                    match message {
                        Some(Some((from_slot, filter_new))) => {
                            session.set_filter(filter_new);
                            info!("client #{id}: filter updated");

                            if let Some(from_slot) = from_slot {
                                let Some(replay_stored_slots_tx) = &replay_stored_slots_tx else {
                                    info!("client #{id}: from_slot is not supported");
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("from_slot is not supported"))).await;
                                    });
                                    session.disconnect_reason = "from_slot_unsupported";
                                    break 'outer;
                                };

                                let (tx, rx) = oneshot::channel();
                                let commitment = session.filter.get_commitment_level();
                                if let Err(_error) = replay_stored_slots_tx.send((commitment, from_slot, tx)).await {
                                    error!("client #{id}: failed to send from_slot request");
                                    task_tracker.spawn(async move {
                                        let _ = stream_tx.send(Err(Status::internal("failed to send from_slot request"))).await;
                                    });
                                    session.disconnect_reason = "replay_error";
                                    break 'outer;
                                }

                                let mut messages = match rx.await {
                                    Ok(ReplayedResponse::Messages(messages)) => messages,
                                    Ok(ReplayedResponse::Lagged(slot)) => {
                                        info!("client #{id}: broadcast from {from_slot} is not available");
                                        task_tracker.spawn(async move {
                                            let message = format!(
                                                "broadcast from {from_slot} is not available, last available: {slot}"
                                            );
                                            let _ = stream_tx.send(Err(Status::out_of_range(message))).await;
                                        });
                                        session.disconnect_reason = "slot_unavailable";
                                        break 'outer;
                                    },
                                    Err(_error) => {
                                        error!("client #{id}: failed to get replay response");
                                        task_tracker.spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("failed to get replay response"))).await;
                                        });
                                        session.disconnect_reason = "replay_error";
                                        break 'outer;
                                    }
                                };

                                messages.sort_by_key(|msg| msg.0);
                                for (_msgid, message) in messages.iter() {
                                    if !session.filter.can_match_message(message) {
                                        continue;
                                    }
                                    for message in session.filter.get_updates(message, Some(commitment)) {
                                        let proto_size = message.encoded_len().min(u32::MAX as usize) as u32;
                                        match stream_tx.send(Ok(message)).await {
                                            Ok(()) => {
                                                session.metrics.incr_message_sent();
                                                session.metrics.incr_bytes_sent(proto_size);
                                            }
                                            Err(mpsc::error::SendError(_)) => {
                                                error!("client #{id}: stream closed");
                                                session.disconnect_reason = "client_closed";
                                                break 'outer;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Some(None) => {
                            session.disconnect_reason = "client_disconnect";
                            break 'outer;
                        },
                        None => {
                            session.disconnect_reason = "client_disconnect";
                            break 'outer;
                        }
                    }
                }
                message = messages_rx.recv() => {
                    let (commitment, messages) = match message {
                        Ok((commitment, messages)) => (commitment, messages),
                        Err(broadcast::error::RecvError::Closed) => {
                            session.disconnect_reason = "broadcast_closed";
                            break 'outer;
                        },
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("client #{id}: lagged to receive geyser messages");
                            task_tracker.spawn(async move {
                                let _ = stream_tx.send(Err(Status::internal("lagged to receive geyser messages"))).await;
                            });
                            session.disconnect_reason = "client_broadcast_lag";
                            break 'outer;
                        }
                    };

                    if commitment == session.filter.get_commitment_level() {
                        for (_msgid, message) in messages.iter() {
                            if !session.filter.can_match_message(message) {
                                continue;
                            }
                            for message in session.filter.get_updates(message, Some(commitment)) {
                                let proto_size = message.encoded_len().min(u32::MAX as usize) as u32;
                                match stream_tx.try_send(Ok(message)) {
                                    Ok(()) => {
                                        session.metrics.incr_message_sent();
                                        session.metrics.incr_bytes_sent(proto_size);
                                    }
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        error!("client #{id}: lagged to send an update");
                                        task_tracker.spawn(async move {
                                            let _ = stream_tx.send(Err(Status::internal("lagged to send an update"))).await;
                                        });
                                        session.disconnect_reason = "client_channel_full";
                                        break 'outer;
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        error!("client #{id}: stream closed");
                                        session.disconnect_reason = "client_closed";
                                        break 'outer;
                                    }
                                }
                            }
                        }
                    }

                    if commitment == CommitmentLevel::Processed && session.debug_client_tx.is_some() {
                        for message in messages.iter() {
                            if let Message::Slot(slot_message) = &message.1 {
                                DebugClientMessage::maybe_send(&session.debug_client_tx, || DebugClientMessage::UpdateSlot { id, slot: slot_message.slot });
                            }
                        }
                    }
                }
            }
        }
    }

    async fn client_loop_snapshot(
        id: usize,
        endpoint: &str,
        stream_tx: LoadAwareSender<TonicResult<FilteredUpdate>>,
        client_rx: &mut mpsc::UnboundedReceiver<Option<(Option<u64>, Filter)>>,
        snapshot_rx: crossbeam_channel::Receiver<Box<Message>>,
        filter: &mut Filter,
        cancellation_token: CancellationToken,
    ) -> Result<(), ClientSnapshotReplayError> {
        info!("client #{id}: going to receive snapshot data");

        // we start with default filter, for snapshot we need wait actual filter first
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("client #{id}: cancelled");
                    return Err(ClientSnapshotReplayError::Cancelled);
                }
                maybe = client_rx.recv() => {
                    match maybe {
                        Some(Some((_from_slot, filter_new))) => {
                            if let Some(msg) = filter_new.get_pong_msg() {
                                if stream_tx.send(Ok(msg)).await.is_err() {
                                    error!("client #{id}: stream closed");
                                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                                }
                                continue;
                            }

                            metrics::update_subscriptions(endpoint, Some(filter), Some(&filter_new));
                            *filter = filter_new;
                            info!("client #{id}: filter updated");
                            break;
                        }
                        Some(None) => {
                            return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                        }
                        None => {
                            return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                        }
                    }
                }

            }
        }

        loop {
            if cancellation_token.is_cancelled() {
                info!("client #{id}: cancelled");
                return Err(ClientSnapshotReplayError::Cancelled);
            }
            let message = match snapshot_rx.try_recv() {
                Ok(message) => {
                    metrics::message_queue_size_dec();
                    message
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    info!("client #{id}: end of startup");
                    break;
                }
            };

            if !filter.can_match_message(&message) {
                continue;
            }
            for message in filter.get_updates(&message, None) {
                if stream_tx.send(Ok(message)).await.is_err() {
                    error!("client #{id}: stream closed");
                    return Err(ClientSnapshotReplayError::ClientGrpcConnectionClosed);
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn serve_listener<H>(
        listener: Listener,
        tls_identity: Option<Identity>,
        http2_adaptive_window: Option<bool>,
        http2_keepalive_interval: Option<Duration>,
        http2_keepalive_timeout: Option<Duration>,
        initial_connection_window_size: Option<u32>,
        initial_stream_window_size: Option<u32>,
        x_token: Option<AsciiMetadataValue>,
        health_service: HealthServer<H>,
        service: GeyserServer<GrpcService>,
        traffic_reporting_threshold: ByteSize,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()>
    where
        H: tonic_health::pb::health_server::Health,
    {
        let mut builder = Server::builder();

        // TLS only applies to TCP — UDS is local IPC, no encryption needed
        if matches!(listener, Listener::Tcp(_)) {
            if let Some(identity) = tls_identity {
                builder = builder
                    .tls_config(ServerTlsConfig::new().identity(identity))
                    .context("failed to apply tls_config")?;
            }
        }

        if let Some(enabled) = http2_adaptive_window {
            builder = builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(interval) = http2_keepalive_interval {
            builder = builder.http2_keepalive_interval(Some(interval));
        }
        if let Some(timeout) = http2_keepalive_timeout {
            builder = builder.http2_keepalive_timeout(Some(timeout));
        }
        if let Some(sz) = initial_connection_window_size {
            builder = builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = initial_stream_window_size {
            builder = builder.initial_stream_window_size(sz);
        }

        let router = builder
            .layer(MeteredLayer::new())
            .layer(interceptor::InterceptorLayer::new(XTokenInterceptor {
                x_token,
            }))
            .add_service(health_service)
            .add_service(service);

        // Capture address before match consumes listener
        let addr = match &listener {
            Listener::Tcp(incoming) => incoming
                .local_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "unknown".to_owned()),
            Listener::Unix(path, _) => format!("unix://{}", path.display()),
        };

        info!("gRPC server listening on {addr}");

        let result = match listener {
            Listener::Tcp(incoming) => {
                let spy = SpyIncoming::new(
                    incoming,
                    SpyIncomingConfig {
                        traffic_reporting_threshold,
                    },
                );
                router
                    .serve_with_incoming_shutdown(spy, shutdown.cancelled())
                    .await
                    .context("TCP listener error")
            }
            Listener::Unix(path, incoming) => {
                let result = router
                    .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
                    .await
                    .context("UDS listener error");

                if let Err(e) = std::fs::remove_file(&path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        log::warn!("failed to remove socket file {}: {e}", path.display());
                    }
                }
                result
            }
        };

        info!("gRPC server on {addr} shut down with result: {result:?}");
        result
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = LoadAwareReceiver<TonicResult<FilteredUpdate>>;
    type SubscribeDeshredStream =
        LoadAwareReceiver<TonicResult<yellowstone_grpc_proto::geyser::SubscribeUpdateDeshred>>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        incr_grpc_method_call_count("subscribe");

        let subscriber_id = request
            .metadata()
            .get("x-subscription-id")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .or_else(|| request.remote_addr().map(|addr| addr.ip().to_string()));

        // Per-subscriber subscription limit: check and increment under a
        // single lock hold so no two calls can race past the limit.
        // Cleanup (decrement) is handled by `ClientSession::drop()`.
        // When subscriber_id is None (no x-subscription-id header and no
        // remote address) we skip the limit check entirely rather than
        // grouping all unidentified clients into a shared bucket.
        if let Some(id) = subscriber_id.as_deref() {
            if self.subscription_limit > 0 {
                let mut tracker = self
                    .subscription_tracker
                    .lock()
                    .expect("subscription_tracker mutex poisoned");
                let count = tracker.entry(id.to_owned()).or_insert(0);

                if *count >= self.subscription_limit {
                    subscription_limit_exceeded_inc(id);
                    if self.subscription_limit_enforce {
                        return Err(Status::resource_exhausted(
                            "max subscription limit exceeded",
                        ));
                    }
                    info!(
                        "subscriber {id:?} over limit ({count}/{}), not enforcing",
                        self.subscription_limit
                    );
                }
                *count += 1;
            }
        }

        let maybe_remote_peer_sk_addr = request
            .extensions()
            .get::<TcpConnectInfo>()
            .or_else(|| {
                request
                    .extensions()
                    .get::<TlsConnectInfo<TcpConnectInfo>>()
                    .map(|tls_info| tls_info.get_ref())
            })
            .and_then(|info| info.remote_addr());

        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        let client_cancellation_token = self.cancellation_token.child_token();
        if client_cancellation_token.is_cancelled() {
            return Err(Status::unavailable("server is shutting down"));
        }

        let x_request_snapshot = request.metadata().contains_key("x-request-snapshot");
        let snapshot_rx = if x_request_snapshot {
            self.snapshot_rx.lock().await.take()
        } else {
            None
        };

        let (stream_tx, stream_rx) = load_aware_channel(if snapshot_rx.is_some() {
            self.config_snapshot_client_channel_capacity
        } else {
            self.config_channel_capacity
        });
        let (client_tx, client_rx) = mpsc::unbounded_channel();

        let ping_stream_tx = stream_tx.clone();
        let ping_cancellation_token = client_cancellation_token.clone();
        let ping_client_cancel = client_cancellation_token.clone();
        self.task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = ping_cancellation_token.cancelled() => {
                        info!("client #{id}: ping cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        let msg = FilteredUpdate::new_empty(FilteredUpdateOneof::ping());
                        log::info!("client #{id}: sending ping");
                        if ping_stream_tx.send(Ok(msg)).await.is_err() {
                            //
                            // It's really important to send cancel ping for one edge-case where someone
                            // subscribe without any filter:
                            //
                            // When someone subscribe without any filter, this can create a "zombie" client loop that
                            // does reject every geyser event thus we never write to the HTTP/2 stream and we never detect that the client TCP connection is closed.
                            // By sending a ping every 10 seconds, we can detect if the client is still alive and if it's not,
                            // we can cancel the client loop.
                            ping_client_cancel.cancel();
                            info!("detected dead client #{id}");
                            break;
                        }
                    }
                }
            }
            info!("client #{id}: ping task exiting");
        });

        let endpoint = request
            .metadata()
            .get("x-endpoint")
            .and_then(|h| h.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "".to_owned());

        let config_filter_limits = Arc::clone(&self.config_filter_limits);
        let incoming_stream_tx = stream_tx.clone();
        let incoming_client_tx = client_tx;
        let incoming_cancellation_token = client_cancellation_token.child_token();

        let mut filter_names = FilterNames::new(
            self.filter_name_size_limit,
            self.filter_names_size_limit,
            self.filter_names_cleanup_interval,
        );
        self.task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = incoming_cancellation_token.cancelled() => {
                        info!("client #{id}: filter receiver cancelled");
                        break;
                    }
                    message = request.get_mut().message() => match message {
                        Ok(Some(request)) => {
                            filter_names.try_clean();

                            if let Err(error) = match Filter::new(&request, &config_filter_limits, &mut filter_names) {
                                Ok(filter) => {
                                    if let Some(msg) = filter.get_pong_msg() {
                                        if incoming_stream_tx.send(Ok(msg)).await.is_err() {
                                            error!("client #{id}: stream closed");
                                            let _ = incoming_client_tx.send(None);
                                            break;
                                        }
                                        continue;
                                    }
                                    match incoming_client_tx.send(Some((request.from_slot, filter))) {
                                        Ok(()) => Ok(()),
                                        Err(error) => Err(error.to_string()),
                                    }
                                },
                                Err(error) => Err(error.to_string()),
                            } {
                                let err = Err(Status::invalid_argument(format!(
                                    "failed to create filter: {error}"
                                )));
                                if incoming_stream_tx.send(err).await.is_err() {
                                    let _ = incoming_client_tx.send(None);
                                }
                            }
                        }
                        Ok(None) => {
                             // Client half-closed its send stream. Stop reading, but keep
                             // incoming_client_tx alive so client_loop continues running.
                            info!("client #{id}: client closed send stream, waiting for cancellation");
                            incoming_cancellation_token.cancelled().await;
                            break;
                        }
                        Err(_error) => {
                            let _ = incoming_client_tx.send(None);
                            break;
                        }
                    }
                }
            }
        });

        self.task_tracker.spawn(Self::client_loop(
            id,
            subscriber_id,
            endpoint,
            stream_tx,
            client_rx,
            snapshot_rx,
            self.broadcast_tx.subscribe(),
            self.replay_stored_slots_tx.clone(),
            self.debug_clients_tx.clone(),
            maybe_remote_peer_sk_addr,
            client_cancellation_token,
            self.task_tracker.clone(),
            Arc::clone(&self.subscription_tracker),
        ));

        Ok(Response::new(stream_rx))
    }

    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> TonicResult<Response<Self::SubscribeDeshredStream>> {
        incr_grpc_method_call_count("subscribe_deshred");
        Err(Status::unimplemented(
            "SubscribeDeshred is not available on this server",
        ))
    }

    async fn subscribe_first_available_slot(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        incr_grpc_method_call_count("subscribe_first_available_slot");
        let response = SubscribeReplayInfoResponse {
            first_available: self
                .replay_first_available_slot
                .as_ref()
                .map(|stored| stored.load(Ordering::Relaxed)),
        };
        Ok(Response::new(response))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        incr_grpc_method_call_count("ping");
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        incr_grpc_method_call_count("get_latest_blockhash");
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| {
                        block.block_height.map(|value| GetLatestBlockhashResponse {
                            slot: block.slot,
                            blockhash: block.blockhash.clone(),
                            last_valid_block_height: value.block_height
                                + MAX_RECENT_BLOCKHASHES as u64,
                        })
                    },
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        incr_grpc_method_call_count("get_block_height");
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| {
                        block.block_height.map(|value| GetBlockHeightResponse {
                            block_height: value.block_height,
                        })
                    },
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        incr_grpc_method_call_count("get_slot");
        if let Some(blocks_meta) = &self.blocks_meta {
            blocks_meta
                .get_block(
                    |block| Some(GetSlotResponse { slot: block.slot }),
                    request.get_ref().commitment,
                )
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn is_blockhash_valid(
        &self,
        request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        incr_grpc_method_call_count("is_blockhash_valid");
        if let Some(blocks_meta) = &self.blocks_meta {
            let req = request.get_ref();
            blocks_meta
                .is_blockhash_valid(&req.blockhash, req.commitment)
                .await
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        incr_grpc_method_call_count("get_version");
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&GrpcVersionInfo::default()).unwrap(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            plugin::{
                filter::{limits::FilterLimits, name::FilterNames, Filter},
                message::{MessageEntry, MessageSlot},
            },
            util::stream::load_aware_channel,
        },
        prost_types::Timestamp,
        solana_pubkey::Pubkey,
        std::time::SystemTime,
        yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterSlots},
    };

    fn create_filter_with_slots() -> Filter {
        let config = SubscribeRequest {
            slots: HashMap::from([("test".into(), SubscribeRequestFilterSlots::default())]),
            ..Default::default()
        };
        let mut names = FilterNames::new(64, 1024, Duration::from_secs(1));
        Filter::new(&config, &FilterLimits::default(), &mut names).unwrap()
    }

    // Simulates the incoming handler task from subscribe(). Mirrors the
    // real Ok(None) path: sends the filter, then on half-close awaits
    // cancellation to keep the sender alive.
    async fn incoming_handler(
        client_tx: mpsc::UnboundedSender<Option<(Option<u64>, Filter)>>,
        filter: Filter,
        half_close: oneshot::Receiver<()>,
        ct: CancellationToken,
    ) {
        client_tx.send(Some((None, filter))).unwrap();
        let _ = half_close.await;
        // this is the fix from #670: await cancellation instead of
        // breaking, so client_tx stays alive and client_rx remains open.
        ct.cancelled().await;
    }

    // Regression test for #662 / #670.
    //
    // #662 (91709fd) removed ping_client_tx, the clone of client_tx that
    // lived in the ping task. before that patch two senders existed for
    // client_rx: ping_client_tx and incoming_client_tx. when a client
    // half-closed its send stream (Ok(None)), the incoming task dropped its
    // sender but ping_client_tx kept client_rx open so client_loop survived.
    //
    // after #662 incoming_client_tx is the only sender. without the fix from
    // #670 (awaiting cancellation in the Ok(None) handler instead of
    // breaking), dropping it closes client_rx and tears down the connection
    // on a normal grpc half-close.
    //
    // uses current_thread runtime so yield_now deterministically sequences
    // filter processing before any broadcast.
    #[tokio::test]
    async fn test_cancellation_on_client_disconnect_after_half_close() {
        let ct = CancellationToken::new();
        let tt = TaskTracker::new();
        let st: SubscriptionTracker = Arc::new(StdMutex::new(HashMap::new()));
        let (broadcast_tx, _) = broadcast::channel::<BroadcastedMessage>(16);
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let (stream_tx, stream_rx) = load_aware_channel(16);
        let (half_close_tx, half_close_rx) = oneshot::channel();

        // mirrors the incoming handler spawned in subscribe()
        let incoming_ct = ct.child_token();
        tokio::spawn(incoming_handler(
            client_tx,
            create_filter_with_slots(),
            half_close_rx,
            incoming_ct,
        ));

        let handle = tokio::spawn(GrpcService::client_loop(
            0,
            Some("test".into()),
            "test".into(),
            stream_tx,
            client_rx,
            None,
            broadcast_tx.subscribe(),
            None,
            None,
            None,
            ct.clone(),
            tt.clone(),
            Arc::clone(&st),
        ));

        // yield so incoming_handler sends the filter and client_loop
        // processes it (only client_rx is ready, no broadcast yet)
        tokio::task::yield_now().await;

        // client half-closes its send stream
        let _ = half_close_tx.send(());
        tokio::task::yield_now().await;

        // client drops subscription rx
        drop(stream_rx);

        // broadcast so client_loop hits try_send -> Closed
        let msg = Message::Slot(MessageSlot {
            slot: 100,
            parent: Some(99),
            status: SlotStatus::Processed,
            dead_error: None,
            created_at: Timestamp::from(SystemTime::now()),
        });
        let _ = broadcast_tx.send((CommitmentLevel::Processed, Arc::new(vec![(1, msg)])));

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("client_loop did not exit")
            .expect("client_loop panicked");

        assert!(ct.is_cancelled());
    }

    #[tokio::test]
    async fn test_subscription_tracker_decrements_on_session_drop() {
        let tracker: SubscriptionTracker = Arc::new(StdMutex::new(HashMap::new()));

        // simulate what subscribe() does: increment under lock
        {
            let mut map = tracker.lock().unwrap();
            *map.entry("sub-1".to_owned()).or_insert(0) += 1;
            *map.entry("sub-1".to_owned()).or_insert(0) += 1;
        }
        assert_eq!(*tracker.lock().unwrap().get("sub-1").unwrap(), 2);

        // create a session (mirrors what client_loop does)
        {
            let _session = ClientSession::new(
                0,
                Some("sub-1".into()),
                "".into(),
                None,
                None,
                CancellationToken::new(),
                Arc::clone(&tracker),
            );
            // session alive: count unchanged
            assert_eq!(*tracker.lock().unwrap().get("sub-1").unwrap(), 2);
        }
        // session dropped: count decremented
        assert_eq!(*tracker.lock().unwrap().get("sub-1").unwrap(), 1);

        // second drop removes the entry entirely
        {
            let _session = ClientSession::new(
                1,
                Some("sub-1".into()),
                "".into(),
                None,
                None,
                CancellationToken::new(),
                Arc::clone(&tracker),
            );
        }
        assert!(tracker.lock().unwrap().get("sub-1").is_none());
    }

    #[tokio::test]
    async fn test_subscription_tracker_skips_unidentified_subscribers() {
        let tracker: SubscriptionTracker = Arc::new(StdMutex::new(HashMap::new()));

        // subscriber_id=None resolves to "UNKNOWN" inside ClientSession,
        // but subscribe() skips the limit check entirely for None.
        // The tracker should remain empty since no increment happened.
        {
            let _session = ClientSession::new(
                0,
                None,
                "".into(),
                None,
                None,
                CancellationToken::new(),
                Arc::clone(&tracker),
            );
        }
        // drop fires but "UNKNOWN" was never in the tracker, so nothing changes
        assert!(tracker.lock().unwrap().is_empty());
    }

    // Characterization tests for the block-reconstruction bookkeeping shared
    // by `geyser_dispatch` (CPU-pinned spin loop) and `geyser_loop` (async
    // fallback). These establish the *current* behavior of both functions as
    // a regression net for a future mechanical extraction/thread-split of
    // that bookkeeping: every test below is run against both implementations
    // via `DispatchKind`.
    mod geyser_bookkeeping {
        use {
            super::*,
            bytes::Bytes,
            crate::plugin::message::{
                MessageAccount, MessageAccountInfo, MessageTransaction, MessageTransactionInfo,
            },
            solana_hash::Hash,
            solana_signature::Signature,
            std::{collections::HashSet, sync::OnceLock},
            yellowstone_grpc_proto::{geyser::SubscribeUpdateBlockMeta, solana::storage::confirmed_block},
        };

        fn unique_signature(seed: u64) -> Signature {
            let mut bytes = [0u8; 64];
            bytes[..8].copy_from_slice(&seed.to_le_bytes());
            Signature::from(bytes)
        }

        #[derive(Clone, Copy)]
        enum DispatchKind {
            Loop,
            Dispatch,
        }

        /// Spawns either `geyser_loop` (on the current tokio runtime) or the
        /// `geyser_dispatch` + `block_reconstruction_dispatch` two-thread
        /// pipeline (mirroring how `GrpcService::create` wires it for
        /// CPU-pinned deployments, minus the CPU pinning itself) and returns
        /// the channels needed to drive and observe it.
        fn spawn_dispatch(
            kind: DispatchKind,
            replay_stored_slots: u64,
            processed_messages_max: usize,
        ) -> (
            mpsc::UnboundedSender<Message>,
            broadcast::Sender<BroadcastedMessage>,
            mpsc::Sender<ReplayStoredSlotsRequest>,
            Arc<AtomicU64>,
        ) {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (broadcast_tx, _rx) = broadcast::channel(1024);
            let (replay_tx, replay_rx) = mpsc::channel(8);
            let replay_first_available_slot = Arc::new(AtomicU64::new(u64::MAX));

            let broadcast_tx_bg = broadcast_tx.clone();
            let replay_slot_bg = Arc::clone(&replay_first_available_slot);

            match kind {
                DispatchKind::Loop => {
                    tokio::spawn(GrpcService::geyser_loop(
                        messages_rx,
                        None,
                        broadcast_tx_bg,
                        Some(replay_rx),
                        Some(replay_slot_bg),
                        replay_stored_slots,
                        processed_messages_max,
                    ));
                }
                DispatchKind::Dispatch => {
                    let (reconstruction_tx, reconstruction_rx) = mpsc::unbounded_channel();
                    let msgid_gen = MessageId::default();
                    let dispatch_msgid_gen = msgid_gen.clone();
                    let dispatch_broadcast_tx = broadcast_tx.clone();
                    std::thread::spawn(move || {
                        GrpcService::block_reconstruction_dispatch(
                            reconstruction_rx,
                            broadcast_tx_bg,
                            Some(replay_rx),
                            Some(replay_slot_bg),
                            replay_stored_slots,
                            processed_messages_max,
                            msgid_gen,
                        );
                    });
                    std::thread::spawn(move || {
                        GrpcService::geyser_dispatch(
                            messages_rx,
                            None,
                            reconstruction_tx,
                            dispatch_broadcast_tx,
                            dispatch_msgid_gen,
                            processed_messages_max,
                        );
                    });
                }
            }

            (messages_tx, broadcast_tx, replay_tx, replay_first_available_slot)
        }

        async fn recv_broadcast(
            rx: &mut broadcast::Receiver<BroadcastedMessage>,
        ) -> BroadcastedMessage {
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("timed out waiting for a broadcast")
                .expect("broadcast channel closed unexpectedly")
        }

        async fn try_recv_broadcast(
            rx: &mut broadcast::Receiver<BroadcastedMessage>,
            timeout: Duration,
        ) -> Option<BroadcastedMessage> {
            tokio::time::timeout(timeout, rx.recv())
                .await
                .ok()
                .map(|res| res.expect("broadcast channel closed unexpectedly"))
        }

        fn make_slot(slot: u64, parent: Option<u64>, status: SlotStatus) -> Message {
            Message::Slot(MessageSlot {
                slot,
                parent,
                status,
                dead_error: None,
                created_at: Timestamp::from(SystemTime::now()),
            })
        }

        fn make_account(slot: u64, pubkey: Pubkey, write_version: u64) -> Message {
            Message::Account(MessageAccount {
                account: Arc::new(MessageAccountInfo {
                    pubkey,
                    lamports: 0,
                    owner: Pubkey::default(),
                    executable: false,
                    rent_epoch: 0,
                    data: Bytes::new(),
                    write_version,
                    txn_signature: None,
                    pre_encoded: OnceLock::new(),
                }),
                slot,
                is_startup: false,
                created_at: Timestamp::from(SystemTime::now()),
            })
        }

        fn make_transaction(slot: u64, signature: Signature) -> Message {
            Message::Transaction(MessageTransaction {
                transaction: Arc::new(MessageTransactionInfo {
                    signature,
                    is_vote: false,
                    transaction: confirmed_block::Transaction::default(),
                    meta: confirmed_block::TransactionStatusMeta::default(),
                    index: 0,
                    account_keys: HashSet::new(),
                    pre_encoded: OnceLock::new(),
                }),
                slot,
                created_at: Timestamp::from(SystemTime::now()),
            })
        }

        fn make_entry(slot: u64, index: usize) -> Message {
            Message::Entry(Arc::new(MessageEntry {
                slot,
                index,
                num_hashes: 0,
                hash: Hash::default(),
                executed_transaction_count: 0,
                starting_transaction_index: 0,
                created_at: Timestamp::from(SystemTime::now()),
            }))
        }

        fn make_block_meta(slot: u64, executed_transaction_count: u64, entries_count: u64) -> Message {
            Message::BlockMeta(Arc::new(MessageBlockMeta {
                block_meta: SubscribeUpdateBlockMeta {
                    slot,
                    blockhash: format!("hash-{slot}"),
                    rewards: None,
                    block_time: None,
                    block_height: None,
                    parent_slot: slot.saturating_sub(1),
                    parent_blockhash: String::new(),
                    executed_transaction_count,
                    entries_count,
                },
                created_at: Timestamp::from(SystemTime::now()),
            }))
        }

        // --- 1. Dedup by write_version ------------------------------------

        async fn run_dedup_by_write_version(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            let pubkey = Pubkey::new_unique();
            tx.send(make_account(10, pubkey, 5)).unwrap();
            tx.send(make_account(10, pubkey, 2)).unwrap();
            tx.send(make_slot(10, None, SlotStatus::Confirmed)).unwrap();

            loop {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                if commitment != CommitmentLevel::Confirmed {
                    continue;
                }
                let write_versions: Vec<_> = batch
                    .iter()
                    .filter_map(|(_, m)| match m {
                        Message::Account(a) => Some(a.account.write_version),
                        _ => None,
                    })
                    .collect();
                if !write_versions.is_empty() {
                    assert_eq!(
                        write_versions,
                        vec![5],
                        "only the higher write_version account update should survive dedup"
                    );
                    break;
                }
            }
        }

        #[tokio::test]
        async fn test_dedup_by_write_version_loop() {
            run_dedup_by_write_version(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_dedup_by_write_version_dispatch() {
            run_dedup_by_write_version(DispatchKind::Dispatch).await;
        }

        // --- 2. Block sealing gating ---------------------------------------

        async fn run_block_seal_on_matching_counts(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            tx.send(make_block_meta(50, 1, 1)).unwrap();
            tx.send(make_transaction(50, unique_signature(1))).unwrap();
            tx.send(make_entry(50, 0)).unwrap();

            let mut saw_block = false;
            for _ in 0..10 {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                if commitment != CommitmentLevel::Processed {
                    continue;
                }
                if batch.iter().any(|(_, m)| matches!(m, Message::Block(_))) {
                    saw_block = true;
                    break;
                }
            }
            assert!(
                saw_block,
                "a sealed Block message should be produced once tx and entry counts match block_meta"
            );
        }

        #[tokio::test]
        async fn test_block_seal_on_matching_counts_loop() {
            run_block_seal_on_matching_counts(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_block_seal_on_matching_counts_dispatch() {
            run_block_seal_on_matching_counts(DispatchKind::Dispatch).await;
        }

        async fn run_block_seal_gated_by_mismatched_counts(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            // entries_count == 0 takes the "no entries expected" branch, so
            // only the transaction count can mismatch here.
            tx.send(make_block_meta(60, 2, 0)).unwrap();
            tx.send(make_transaction(60, unique_signature(2))).unwrap();

            for _ in 0..2 {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                assert_eq!(commitment, CommitmentLevel::Processed);
                assert!(!batch.iter().any(|(_, m)| matches!(m, Message::Block(_))));
            }

            let extra = try_recv_broadcast(&mut rx, Duration::from_millis(250)).await;
            assert!(
                extra.is_none(),
                "no Block message should ever be produced while tx count ({}) != executed_transaction_count (2), got {extra:?}",
                1
            );
        }

        #[tokio::test]
        async fn test_block_seal_gated_by_mismatched_counts_loop() {
            run_block_seal_gated_by_mismatched_counts(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_block_seal_gated_by_mismatched_counts_dispatch() {
            run_block_seal_gated_by_mismatched_counts(DispatchKind::Dispatch).await;
        }

        // --- 3. Duplicate BlockMeta detection -------------------------------
        //
        // Today's code does not reject a duplicate BlockMeta for a slot: it
        // logs an "unexpected message: BlockMeta (duplicate)" invalid-block
        // metric (not observable here without a real Prometheus registry) and
        // unconditionally overwrites the stored block_meta with the new one.
        // This test documents that overwrite behavior and confirms it does
        // not panic or corrupt bookkeeping for the slot.

        async fn run_duplicate_block_meta_overwrites(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            tx.send(make_block_meta(70, 1, 0)).unwrap(); // first: wrong count
            tx.send(make_block_meta(70, 2, 0)).unwrap(); // duplicate: correct count
            tx.send(make_transaction(70, unique_signature(3)))
                .unwrap();
            tx.send(make_transaction(70, unique_signature(4)))
                .unwrap();

            let mut saw_block = false;
            for _ in 0..10 {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                if commitment != CommitmentLevel::Processed {
                    continue;
                }
                if let Some((_, Message::Block(block))) =
                    batch.iter().find(|(_, m)| matches!(m, Message::Block(_)))
                {
                    assert_eq!(
                        block.meta.executed_transaction_count, 2,
                        "sealed block should reflect the second (duplicate) BlockMeta, not the first"
                    );
                    saw_block = true;
                    break;
                }
            }
            assert!(
                saw_block,
                "block should still seal (using the most recently stored BlockMeta) after a duplicate arrives, without panicking"
            );
        }

        #[tokio::test]
        async fn test_duplicate_block_meta_overwrites_loop() {
            run_duplicate_block_meta_overwrites(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_duplicate_block_meta_overwrites_dispatch() {
            run_duplicate_block_meta_overwrites(DispatchKind::Dispatch).await;
        }

        // --- 4. Missed-status parent-slot propagation -----------------------

        async fn run_missed_status_backfill(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            tx.send(make_slot(100, None, SlotStatus::Processed)).unwrap();
            tx.send(make_slot(101, Some(100), SlotStatus::Processed))
                .unwrap();
            tx.send(make_slot(102, Some(101), SlotStatus::Processed))
                .unwrap();
            // slot 102 gets Confirmed directly; 100 and 101 never receive
            // their own Confirmed status message.
            tx.send(make_slot(102, Some(101), SlotStatus::Confirmed))
                .unwrap();

            let mut confirmed_slots = Vec::new();
            for _ in 0..20 {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                if commitment != CommitmentLevel::Confirmed {
                    continue;
                }
                for (_, m) in batch.iter() {
                    // Note: every raw Slot message (any status) is also
                    // broadcast once under CommitmentLevel::Confirmed with
                    // itself as the sole entry (see the unconditional
                    // `confirmed_messages.push(message.clone())` below). We
                    // only care about messages whose *own* status is
                    // Confirmed, i.e. the real backfilled/synthesized ones.
                    if let Message::Slot(s) = m {
                        if s.status == SlotStatus::Confirmed {
                            confirmed_slots.push(s.slot);
                        }
                    }
                }
                if confirmed_slots.contains(&100) && confirmed_slots.contains(&101) {
                    break;
                }
            }

            assert!(
                confirmed_slots.contains(&100),
                "ancestor slot 100 should get a synthesized Confirmed status backfilled from slot 102"
            );
            assert!(
                confirmed_slots.contains(&101),
                "ancestor slot 101 should get a synthesized Confirmed status backfilled from slot 102"
            );
        }

        #[tokio::test]
        async fn test_missed_status_backfill_loop() {
            run_missed_status_backfill(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_missed_status_backfill_dispatch() {
            run_missed_status_backfill(DispatchKind::Dispatch).await;
        }

        // --- 5. Gc timing ----------------------------------------------------

        async fn run_gc_retains_until_safety_buffer(kind: DispatchKind) {
            // Mirrors the `FINALIZATION_SAFETY_BUFFER` constant inlined in
            // both `geyser_loop` and `geyser_dispatch`.
            const FINALIZATION_SAFETY_BUFFER: u64 = 10;
            let replay_stored_slots = 5u64;

            let (tx, broadcast_tx, _replay_tx, replay_first_available_slot) =
                spawn_dispatch(kind, replay_stored_slots, 1);
            let mut rx = broadcast_tx.subscribe();

            for slot in 1..=30u64 {
                tx.send(make_account(slot, Pubkey::new_unique(), 1))
                    .unwrap();
            }
            tx.send(make_slot(30, Some(29), SlotStatus::Finalized))
                .unwrap();

            // Wait until slot 30's Finalized message has been fully processed
            // by the block-reconstruction thread (which is where gc runs).
            // Post-decoupling, a Processed broadcast for this message no
            // longer proves that: for DispatchKind::Dispatch, the raw
            // message's Processed broadcast now comes from geyser_dispatch,
            // independent of (and possibly before) the reconstruction
            // thread's gc timing. Confirmed/Finalized broadcasts, however,
            // are still produced solely by the reconstruction thread, after
            // gc_finalized_slots runs — a reliable proxy under both
            // DispatchKind::Loop and DispatchKind::Dispatch.
            loop {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                if commitment == CommitmentLevel::Processed {
                    continue;
                }
                if batch.iter().any(|(_, m)| {
                    matches!(m, Message::Slot(s) if s.slot == 30 && s.status == SlotStatus::Finalized)
                }) {
                    break;
                }
            }

            let expected_earliest = 30 - (FINALIZATION_SAFETY_BUFFER + replay_stored_slots);
            assert_eq!(
                replay_first_available_slot.load(Ordering::Relaxed),
                expected_earliest,
                "earliest surviving slot should be exactly FINALIZATION_SAFETY_BUFFER + replay_stored_slots \
                 behind the finalized slot: not gc'd before that boundary, not retained past it"
            );
        }

        #[tokio::test]
        async fn test_gc_retains_until_safety_buffer_loop() {
            run_gc_retains_until_safety_buffer(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_gc_retains_until_safety_buffer_dispatch() {
            run_gc_retains_until_safety_buffer(DispatchKind::Dispatch).await;
        }

        // --- 6. Replay-buffer servicing ---------------------------------------

        async fn run_replay_buffer_servicing(kind: DispatchKind) {
            let (tx, broadcast_tx, replay_tx, _replay_slot) = spawn_dispatch(kind, 1000, 1);
            let mut rx = broadcast_tx.subscribe();

            for slot in 10..=15u64 {
                tx.send(make_slot(slot, Some(slot.saturating_sub(1)), SlotStatus::Processed))
                    .unwrap();
            }

            // Wait for the last slot message to be processed before issuing
            // the replay request, to avoid racing the dispatcher's bookkeeping.
            loop {
                let (_commitment, batch) = recv_broadcast(&mut rx).await;
                if batch
                    .iter()
                    .any(|(_, m)| matches!(m, Message::Slot(s) if s.slot == 15))
                {
                    break;
                }
            }

            // In-range request: from_slot within the retained buffer.
            let (resp_tx, resp_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, 12, resp_tx))
                .await
                .unwrap();
            match resp_rx.await.unwrap() {
                ReplayedResponse::Messages(messages) => {
                    let slots: Vec<_> = messages
                        .iter()
                        .filter_map(|(_, m)| match m {
                            Message::Slot(s) => Some(s.slot),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(slots, vec![12, 13, 14, 15]);
                }
                ReplayedResponse::Lagged(slot) => {
                    panic!("expected in-range replay messages, got Lagged({slot})")
                }
            }

            // Out-of-range request: from_slot below the earliest stored slot.
            let (resp_tx2, resp_rx2) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, 5, resp_tx2))
                .await
                .unwrap();
            match resp_rx2.await.unwrap() {
                ReplayedResponse::Lagged(earliest) => assert_eq!(earliest, 10),
                ReplayedResponse::Messages(_) => {
                    panic!("expected a Lagged response for an out-of-range from_slot")
                }
            }
        }

        #[tokio::test]
        async fn test_replay_buffer_servicing_loop() {
            run_replay_buffer_servicing(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_replay_buffer_servicing_dispatch() {
            run_replay_buffer_servicing(DispatchKind::Dispatch).await;
        }

        // --- 7a. Live ordering: no backfill ----------------------------------

        async fn run_live_ordering_no_backfill(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            let pubkey_a = Pubkey::new_unique();
            let pubkey_b = Pubkey::new_unique();
            tx.send(make_account(200, pubkey_a, 1)).unwrap();
            tx.send(make_account(200, pubkey_b, 1)).unwrap();
            tx.send(make_transaction(200, unique_signature(5)))
                .unwrap();

            let mut msgids = Vec::new();
            for _ in 0..3 {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                assert_eq!(commitment, CommitmentLevel::Processed);
                for (msgid, _) in batch.iter() {
                    msgids.push(*msgid);
                }
            }

            assert!(
                msgids.windows(2).all(|w| w[0] < w[1]),
                "live Processed messages should arrive in strictly increasing msgid order \
                 when no missed-status backfill occurs, got {msgids:?}"
            );
        }

        #[tokio::test]
        async fn test_live_ordering_no_backfill_loop() {
            run_live_ordering_no_backfill(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_live_ordering_no_backfill_dispatch() {
            run_live_ordering_no_backfill(DispatchKind::Dispatch).await;
        }

        // --- 7b. Live ordering: backfill case ---------------------------------
        //
        // Verified against the current source (not assumed): the backfill
        // loop appends synthesized ancestor messages to `messages_vec` in
        // ancestor order (closest ancestor first), assigning each a fresh,
        // increasing msgid as it goes. That vector is then broadcast via
        // `messages_vec.into_iter().rev()`, so the *last*-synthesized (and
        // therefore highest-msgid, oldest-ancestor) message is broadcast
        // *first*. Today's code therefore broadcasts backfilled ancestors in
        // msgid-decreasing order relative to each other and to the slot that
        // triggered them.

        async fn run_live_ordering_backfill(kind: DispatchKind) {
            let (tx, broadcast_tx, _replay_tx, _replay_slot) = spawn_dispatch(kind, 100, 1);
            let mut rx = broadcast_tx.subscribe();

            tx.send(make_slot(300, None, SlotStatus::Processed)).unwrap();
            tx.send(make_slot(301, Some(300), SlotStatus::Processed))
                .unwrap();
            tx.send(make_slot(302, Some(301), SlotStatus::Processed))
                .unwrap();
            tx.send(make_slot(302, Some(301), SlotStatus::Confirmed))
                .unwrap();

            let mut seen: Vec<(u64, u64)> = Vec::new(); // (msgid, slot)
            for _ in 0..20 {
                let (commitment, batch) = recv_broadcast(&mut rx).await;
                if commitment != CommitmentLevel::Confirmed {
                    continue;
                }
                for (msgid, m) in batch.iter() {
                    // As in the previous test: only messages whose own
                    // status is Confirmed are the real (raw or backfilled)
                    // Confirmed status updates; every raw Slot message also
                    // gets an (uninteresting, single-item) broadcast on this
                    // channel regardless of its own status.
                    if let Message::Slot(s) = m {
                        if s.status == SlotStatus::Confirmed && [300, 301, 302].contains(&s.slot) {
                            seen.push((*msgid, s.slot));
                        }
                    }
                }
                let slots: Vec<_> = seen.iter().map(|(_, s)| *s).collect();
                if slots.contains(&300) && slots.contains(&301) && slots.contains(&302) {
                    break;
                }
            }

            assert_eq!(
                seen.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
                vec![300, 301, 302],
                "oldest backfilled ancestor should be broadcast first, then its descendant, \
                 then the slot whose status update triggered the backfill"
            );
            let msgids: Vec<_> = seen.iter().map(|(id, _)| *id).collect();
            assert!(
                msgids.windows(2).all(|w| w[0] > w[1]),
                "backfilled ancestor broadcasts should arrive in msgid-decreasing order, got {msgids:?}"
            );
        }

        #[tokio::test]
        async fn test_live_ordering_backfill_loop() {
            run_live_ordering_backfill(DispatchKind::Loop).await;
        }

        #[tokio::test]
        async fn test_live_ordering_backfill_dispatch() {
            run_live_ordering_backfill(DispatchKind::Dispatch).await;
        }

        // --- 8. Shutdown propagation across the two-thread pipeline -----------
        //
        // Precursor check for Task 6c's full join wiring: dropping the
        // sender feeding `geyser_dispatch`'s inbound channel should make
        // `geyser_dispatch` observe a disconnected channel, exit its loop,
        // and (by dropping its own sender into the block-reconstruction
        // channel) cause `block_reconstruction_dispatch` to observe the same
        // and exit too. Neither thread should hang waiting on the other.

        #[tokio::test]
        async fn test_dispatch_shutdown_propagates_to_reconstruction_thread() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (broadcast_tx, _rx) = broadcast::channel::<BroadcastedMessage>(16);
            let (reconstruction_tx, reconstruction_rx) = mpsc::unbounded_channel();
            let msgid_gen = MessageId::default();
            let dispatch_msgid_gen = msgid_gen.clone();
            let dispatch_broadcast_tx = broadcast_tx.clone();

            let reconstruction_handle = std::thread::spawn(move || {
                GrpcService::block_reconstruction_dispatch(
                    reconstruction_rx,
                    broadcast_tx,
                    None,
                    None,
                    100,
                    1,
                    msgid_gen,
                );
            });
            let dispatch_handle = std::thread::spawn(move || {
                GrpcService::geyser_dispatch(
                    messages_rx,
                    None,
                    reconstruction_tx,
                    dispatch_broadcast_tx,
                    dispatch_msgid_gen,
                    1,
                );
            });

            // Drop the plugin-facing sender: this is the only thing that
            // keeps `geyser_dispatch`'s loop alive.
            drop(messages_tx);

            tokio::time::timeout(
                Duration::from_secs(2),
                tokio::task::spawn_blocking(move || {
                    dispatch_handle
                        .join()
                        .expect("geyser_dispatch thread panicked")
                }),
            )
            .await
            .expect("geyser_dispatch did not exit after its inbound channel closed")
            .expect("join task panicked");

            tokio::time::timeout(
                Duration::from_secs(2),
                tokio::task::spawn_blocking(move || {
                    reconstruction_handle
                        .join()
                        .expect("block_reconstruction_dispatch thread panicked")
                }),
            )
            .await
            .expect(
                "block_reconstruction_dispatch did not exit after geyser_dispatch dropped its sender",
            )
            .expect("join task panicked");
        }

        // --- 9. Decoupling proof + reconnect-during-backpressure --------------
        //
        // Task 6b's actual goal: `geyser_dispatch`'s raw-Processed broadcast
        // must not wait on the block-reconstruction thread's bookkeeping.
        // These tests gate the reconstruction thread closed via
        // `reconstruction_test_gate` (a `#[cfg(test)]`-only hook, unreachable
        // from any non-test build) to prove it.

        /// Spawns the same two-thread pipeline as
        /// `spawn_dispatch(DispatchKind::Dispatch, ..)`, except the
        /// reconstruction thread's per-item processing starts gated closed:
        /// it will pull items off its inbound channel (so they queue up
        /// there once it blocks on the first one) but do nothing with them
        /// until the returned gate is released.
        #[allow(clippy::type_complexity)]
        fn spawn_dispatch_gated(
            replay_stored_slots: u64,
            processed_messages_max: usize,
        ) -> (
            mpsc::UnboundedSender<Message>,
            broadcast::Sender<BroadcastedMessage>,
            mpsc::Sender<ReplayStoredSlotsRequest>,
            Arc<AtomicU64>,
            Arc<reconstruction_test_gate::Gate>,
        ) {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (broadcast_tx, _rx) = broadcast::channel(1024);
            let (replay_tx, replay_rx) = mpsc::channel(8);
            let replay_first_available_slot = Arc::new(AtomicU64::new(u64::MAX));

            let broadcast_tx_bg = broadcast_tx.clone();
            let replay_slot_bg = Arc::clone(&replay_first_available_slot);

            let (reconstruction_tx, reconstruction_rx) = mpsc::unbounded_channel();
            let msgid_gen = MessageId::default();
            let dispatch_msgid_gen = msgid_gen.clone();
            let dispatch_broadcast_tx = broadcast_tx.clone();

            let gate = reconstruction_test_gate::Gate::new_closed();
            let gate_for_thread = Arc::clone(&gate);

            std::thread::spawn(move || {
                reconstruction_test_gate::install(gate_for_thread);
                GrpcService::block_reconstruction_dispatch(
                    reconstruction_rx,
                    broadcast_tx_bg,
                    Some(replay_rx),
                    Some(replay_slot_bg),
                    replay_stored_slots,
                    processed_messages_max,
                    msgid_gen,
                );
            });
            std::thread::spawn(move || {
                GrpcService::geyser_dispatch(
                    messages_rx,
                    None,
                    reconstruction_tx,
                    dispatch_broadcast_tx,
                    dispatch_msgid_gen,
                    processed_messages_max,
                );
            });

            (
                messages_tx,
                broadcast_tx,
                replay_tx,
                replay_first_available_slot,
                gate,
            )
        }

        #[tokio::test]
        async fn test_decoupling_processed_flows_without_waiting_on_gated_reconstruction() {
            let (tx, broadcast_tx, _replay_tx, _replay_slot, gate) = spawn_dispatch_gated(100, 1);
            let mut rx = broadcast_tx.subscribe();

            // Slot N: a complete, sealing block. Sent while the
            // reconstruction thread is already gated closed, so none of it
            // is processed (gc/dedup/sealing) until the gate is released.
            tx.send(make_block_meta(500, 1, 1)).unwrap();
            tx.send(make_transaction(500, unique_signature(50)))
                .unwrap();
            tx.send(make_entry(500, 0)).unwrap();

            // Slot N+1: fully disjoint, including two updates to the same
            // pubkey (to check per-pubkey ordering under starvation).
            let pubkey = Pubkey::new_unique();
            tx.send(make_slot(501, Some(500), SlotStatus::Processed))
                .unwrap();
            tx.send(make_account(501, pubkey, 1)).unwrap();
            tx.send(make_account(501, pubkey, 2)).unwrap();
            tx.send(make_transaction(501, unique_signature(51)))
                .unwrap();

            // Collect Processed broadcasts until slot 501's 4 raw messages
            // have all arrived, with a bounded overall timeout — proving
            // they arrive without waiting on slot 500's sealed Block
            // message, which cannot arrive at all while the gate is closed.
            let mut msgids_501 = Vec::new();
            let mut write_versions_501 = Vec::new();
            let mut saw_block_500 = false;
            for _ in 0..40 {
                let Some((commitment, batch)) =
                    try_recv_broadcast(&mut rx, Duration::from_secs(2)).await
                else {
                    break;
                };
                if commitment != CommitmentLevel::Processed {
                    continue;
                }
                for (msgid, m) in batch.iter() {
                    match m {
                        Message::Block(b) if b.meta.slot == 500 => saw_block_500 = true,
                        Message::Slot(s) if s.slot == 501 => msgids_501.push(*msgid),
                        Message::Account(a) if a.slot == 501 && a.account.pubkey == pubkey => {
                            msgids_501.push(*msgid);
                            write_versions_501.push(a.account.write_version);
                        }
                        Message::Transaction(t) if t.slot == 501 => msgids_501.push(*msgid),
                        _ => {}
                    }
                }
                if msgids_501.len() >= 4 {
                    break;
                }
            }

            assert!(
                !saw_block_500,
                "slot 500's sealed Block message must not have arrived yet — \
                 the reconstruction thread is still gated"
            );
            assert_eq!(
                msgids_501.len(),
                4,
                "all of slot 501's raw messages should arrive on Processed while the \
                 reconstruction thread is gated, got msgids {msgids_501:?}"
            );
            assert!(
                msgids_501.windows(2).all(|w| w[0] < w[1]),
                "slot 501's raw messages should arrive in strictly increasing msgid \
                 order, got {msgids_501:?}"
            );
            assert_eq!(
                write_versions_501,
                vec![1, 2],
                "same-pubkey account updates must still arrive in write_version order \
                 while the reconstruction thread is starved"
            );

            let last_501_msgid = *msgids_501.last().unwrap();

            // Release the gate: slot 500's sealed Block should now be
            // delivered — late, but not dropped.
            gate.release();

            let mut block_500_msgid = None;
            for _ in 0..50 {
                let Some((commitment, batch)) =
                    try_recv_broadcast(&mut rx, Duration::from_secs(2)).await
                else {
                    break;
                };
                if commitment != CommitmentLevel::Processed {
                    continue;
                }
                for (msgid, m) in batch.iter() {
                    if let Message::Block(b) = m {
                        if b.meta.slot == 500 {
                            block_500_msgid = Some(*msgid);
                        }
                    }
                }
                if block_500_msgid.is_some() {
                    break;
                }
            }

            let block_500_msgid = block_500_msgid.expect(
                "slot 500's sealed Block message should eventually be delivered \
                 after the gate is released (late, not dropped)",
            );
            // What this pins down, verified empirically here (not assumed):
            // the Block message's msgid ends up *higher* than the
            // already-delivered slot 501 raw msgids, not lower. `msgid_gen`
            // is one shared, global monotonic counter; `geyser_dispatch`
            // (never gated) mints ids for every raw message — including all
            // of slot 501's, and even slot 500's own raw messages — eagerly,
            // as they arrive, regardless of reconstruction-thread progress.
            // The reconstruction thread only mints the *derived* Block
            // message's id lazily, at the moment it actually processes the
            // sealing message — which, under this gate, happens strictly
            // after dispatch has already raced ahead and consumed ids for
            // every raw message of both slots. So a message's position in
            // the global msgid order no longer reflects "which slot it's
            // about" once dispatch and the reconstruction thread are
            // decoupled and the latter genuinely falls behind: only
            // uniqueness and monotonicity (never reused, never issued out of
            // order to the same caller) are preserved, not slot affinity.
            assert!(
                block_500_msgid > last_501_msgid,
                "under this backpressure scenario the late Block message's msgid \
                 ({block_500_msgid}) is expected to be higher than slot 501's raw \
                 msgids (last: {last_501_msgid}) — see comment above"
            );
        }

        #[tokio::test]
        async fn test_decoupling_confirmed_subscriber_still_gets_correct_slot_under_gate() {
            let (tx, broadcast_tx, _replay_tx, _replay_slot, gate) = spawn_dispatch_gated(100, 1);
            let mut rx = broadcast_tx.subscribe();

            tx.send(make_block_meta(600, 1, 1)).unwrap();
            tx.send(make_transaction(600, unique_signature(60)))
                .unwrap();
            tx.send(make_entry(600, 0)).unwrap();
            tx.send(make_slot(600, Some(599), SlotStatus::Confirmed))
                .unwrap();

            // While the gate is closed, no Confirmed batch for slot 600 can
            // possibly arrive (Confirmed is only ever produced by the
            // reconstruction thread).
            let early = try_recv_broadcast(&mut rx, Duration::from_millis(200)).await;
            assert!(
                !matches!(
                    early,
                    Some((CommitmentLevel::Confirmed, ref batch))
                        if batch.iter().any(|(_, m)| m.get_slot() == 600)
                ),
                "no Confirmed batch for slot 600 should arrive while gated, got {early:?}"
            );

            gate.release();

            let mut confirmed_kinds: HashSet<&'static str> = HashSet::new();
            for _ in 0..50 {
                let Some((commitment, batch)) =
                    try_recv_broadcast(&mut rx, Duration::from_secs(2)).await
                else {
                    break;
                };
                if commitment != CommitmentLevel::Confirmed {
                    continue;
                }
                for (_, m) in batch.iter() {
                    if m.get_slot() != 600 {
                        continue;
                    }
                    match m {
                        Message::Transaction(_) => {
                            confirmed_kinds.insert("Transaction");
                        }
                        Message::Block(_) => {
                            confirmed_kinds.insert("Block");
                        }
                        Message::Slot(s) if s.status == SlotStatus::Confirmed => {
                            confirmed_kinds.insert("Slot");
                        }
                        _ => {}
                    }
                }
                if confirmed_kinds.contains("Block") {
                    break;
                }
            }

            assert!(
                confirmed_kinds.contains("Slot")
                    && confirmed_kinds.contains("Transaction")
                    && confirmed_kinds.contains("Block"),
                "a Confirmed subscriber should eventually receive the complete, \
                 correct slot-600 set (raw Slot status, raw Transaction, and the \
                 sealed Block) after the gate is released, got {confirmed_kinds:?}"
            );
        }

        // --- 10. Reconnect-during-backpressure --------------------------------
        //
        // While the reconstruction thread is gated closed, a real `from_slot`
        // replay request (via the actual `replay_stored_slots_tx`/oneshot
        // plumbing) must block — not error, not resolve early — until the
        // reconstruction thread's inbound channel drains and its idle branch
        // services it from current BTreeMap state. This relocates today's
        // existing degraded-mode behavior; it is not a new failure mode.
        // Trade-off: raw Processed latency is now decoupled/near-instant,
        // while `from_slot` replay freshness depends solely on the
        // reconstruction thread's own backlog.

        #[tokio::test]
        async fn test_reconnect_replay_blocks_until_gated_reconstruction_drains() {
            let (tx, _broadcast_tx, replay_tx, _replay_slot, gate) = spawn_dispatch_gated(100, 1);

            tx.send(make_slot(700, Some(699), SlotStatus::Processed))
                .unwrap();
            tx.send(make_account(700, Pubkey::new_unique(), 1))
                .unwrap();

            // Give geyser_dispatch time to actually forward these into the
            // (gated) reconstruction channel.
            tokio::time::sleep(Duration::from_millis(50)).await;

            let (replay_reply_tx, mut replay_reply_rx) = oneshot::channel();
            replay_tx
                .send((CommitmentLevel::Processed, 700, replay_reply_tx))
                .await
                .expect("replay_stored_slots_tx should accept the request");

            // The reconstruction thread is stuck processing (gated on) the
            // first backlogged item, so it never even looks at
            // replay_stored_slots_rx yet: the oneshot must still be pending.
            // Poll non-consumingly via try_recv() so we can keep awaiting
            // this exact same receiver below, rather than dropping it.
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                matches!(
                    replay_reply_rx.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "from_slot replay must still be pending while the reconstruction thread's \
                 channel has an unprocessed backlog"
            );

            gate.release();

            // Once released, the reconstruction thread drains its backlog,
            // then its idle branch services the now-pending replay request
            // from current (now-complete) BTreeMap state.
            let response = tokio::time::timeout(Duration::from_secs(2), replay_reply_rx)
                .await
                .expect("replay request should resolve once the reconstruction thread drains")
                .expect("oneshot sender should not be dropped");

            match response {
                ReplayedResponse::Messages(messages) => {
                    assert!(
                        messages
                            .iter()
                            .any(|(_, m)| matches!(m, Message::Slot(s) if s.slot == 700)),
                        "replay response should contain slot 700's messages once serviced \
                         from the drained BTreeMap, got {messages:?}"
                    );
                }
                ReplayedResponse::Lagged(slot) => {
                    panic!("expected in-range replay messages for slot 700, got Lagged({slot})")
                }
            }
        }

        // --- 10. Task 6c: clean shutdown of the `GrpcService::create`-level
        //         thread-spawn wiring ---------------------------------------
        //
        // Drives `GrpcService::spawn_dispatch_threads` directly — the exact
        // function `GrpcService::create` calls for CPU-pinned deployments —
        // rather than a hand-rolled mirror of it, so this test proves the
        // real production spawn/join wiring, not a reimplementation of it.

        #[tokio::test]
        async fn test_spawn_dispatch_threads_join_cleanly_on_shutdown() {
            let (messages_tx, messages_rx) = mpsc::unbounded_channel();
            let (broadcast_tx, _rx) = broadcast::channel::<BroadcastedMessage>(16);

            let handles = GrpcService::spawn_dispatch_threads(
                0,
                messages_rx,
                None,
                broadcast_tx,
                None,
                None,
                100,
                1,
            );

            // Feed a couple of in-flight messages, then simulate plugin
            // shutdown by dropping the sender that feeds geyser_dispatch's
            // inbound channel (mirrors `on_unload` dropping `grpc_channel`).
            // In-flight messages may or may not be observed before the
            // channel closes — this test only asserts a clean, bounded-time
            // exit, matching geyser_dispatch's own pre-existing (not
            // retroactively fixed) shutdown semantics.
            messages_tx
                .send(make_slot(1, None, SlotStatus::Processed))
                .unwrap();
            drop(messages_tx);

            tokio::time::timeout(
                Duration::from_secs(2),
                tokio::task::spawn_blocking(move || {
                    handles
                        .geyser_dispatch
                        .join()
                        .expect("geyser-dispatch thread panicked");
                    handles
                        .block_reconstruction
                        .join()
                        .expect("block-reconstruction thread panicked");
                }),
            )
            .await
            .expect(
                "geyser-dispatch/block-reconstruction threads did not join within the \
                 bounded timeout after shutdown",
            )
            .expect("join task panicked");
        }
    }
}
