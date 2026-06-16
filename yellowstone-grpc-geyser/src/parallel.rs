use {
    crate::plugin::{
        filter::encoder::{AccountEncoder, TransactionEncoder},
        message::{CommitmentLevel, Message},
    },
    rayon::{ThreadPool, ThreadPoolBuilder},
    std::sync::Arc,
    tokio::sync::{broadcast, mpsc, oneshot},
};

pub struct ParallelEncoder {
    tx: mpsc::UnboundedSender<BridgeRequest>,
    pool: Arc<ThreadPool>,
}

enum BridgeRequest {
    EncodeAndReply {
        batch: Vec<(u64, Message)>,
        response: oneshot::Sender<Vec<(u64, Message)>>,
    },
    /// Fire-and-forget: encode `to_encode` then broadcast Processed, then broadcast each extra.
    FireAndBroadcast {
        to_encode: Vec<(u64, Message)>,
        extras: Vec<(CommitmentLevel, Vec<(u64, Message)>)>,
        broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<(u64, Message)>>)>,
    },
}

impl ParallelEncoder {
    pub fn new(num_threads: usize) -> (Self, std::thread::JoinHandle<()>) {
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .thread_name(|i| format!("geyser-encoder-{i}"))
                .build()
                .expect("failed to create rayon pool"),
        );

        let (tx, rx) = mpsc::unbounded_channel();

        let pool_for_bridge = Arc::clone(&pool);
        let handle = std::thread::Builder::new()
            .name("geyser-encoder-bridge".into())
            .spawn(move || Self::bridge_loop(rx, pool_for_bridge))
            .expect("failed to spawn encoder bridge");

        (Self { tx, pool }, handle)
    }

    fn bridge_loop(mut rx: mpsc::UnboundedReceiver<BridgeRequest>, pool: Arc<ThreadPool>) {
        use rayon::prelude::*;

        while let Some(req) = rx.blocking_recv() {
            match req {
                BridgeRequest::EncodeAndReply { mut batch, response } => {
                    pool.install(|| {
                        batch.par_iter_mut().for_each(|(_msgid, msg)| {
                            Self::encode_message(msg);
                        });
                    });
                    let _ = response.send(batch);
                }
                BridgeRequest::FireAndBroadcast {
                    mut to_encode,
                    extras,
                    broadcast_tx,
                } => {
                    if to_encode.len() < 4 {
                        for (_, msg) in &mut to_encode {
                            Self::encode_message(msg);
                        }
                    } else {
                        pool.install(|| {
                            to_encode.par_iter_mut().for_each(|(_, msg)| {
                                Self::encode_message(msg);
                            });
                        });
                    }
                    let _ = broadcast_tx
                        .send((CommitmentLevel::Processed, Arc::new(to_encode)));
                    for (cl, msgs) in extras {
                        let _ = broadcast_tx.send((cl, Arc::new(msgs)));
                    }
                }
            }
        }

        log::info!("exiting encoder bridge loop");
    }

    pub fn encode_blocking(&self, mut batch: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
        use rayon::prelude::*;

        if batch.len() < 4 {
            for (_, msg) in &mut batch {
                Self::encode_message(msg);
            }
        } else {
            self.pool.install(|| {
                batch
                    .par_iter_mut()
                    .for_each(|(_, msg)| Self::encode_message(msg));
            });
        }
        batch
    }

    fn encode_message(msg: &Message) {
        match msg {
            Message::Transaction(tx) => {
                if tx.transaction.pre_encoded.get().is_none() {
                    TransactionEncoder::pre_encode(&tx.transaction);
                }
            }
            Message::Account(acc) => {
                if acc.account.pre_encoded.get().is_none() {
                    AccountEncoder::pre_encode(&acc.account);
                }
            }
            _ => {}
        }
    }

    pub async fn encode(&self, batch: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
        if batch.len() < 4 {
            return Self::encode_sync(batch);
        }

        let (tx, rx) = oneshot::channel();

        // move batch, don't clone
        if self
            .tx
            .send(BridgeRequest::EncodeAndReply {
                batch,
                response: tx,
            })
            .is_err()
        {
            // channel closed - this shouldn't happen in normal operation
            panic!("encoder channel closed");
        }

        rx.await.expect("encoder response failed")
    }

    fn encode_sync(mut batch: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
        for (_msgid, msg) in &mut batch {
            Self::encode_message(msg);
        }
        batch
    }

    /// Hand a batch off to the bridge for encoding and broadcasting without blocking the caller.
    /// The bridge will encode `to_encode`, broadcast it as Processed, then broadcast each item
    /// in `extras` under its respective CommitmentLevel — all off the dispatch thread.
    pub fn encode_fire_and_broadcast(
        &self,
        to_encode: Vec<(u64, Message)>,
        extras: Vec<(CommitmentLevel, Vec<(u64, Message)>)>,
        broadcast_tx: broadcast::Sender<(CommitmentLevel, Arc<Vec<(u64, Message)>>)>,
    ) {
        // Best-effort: if the bridge is gone we drop silently (shutdown path).
        let _ = self.tx.send(BridgeRequest::FireAndBroadcast {
            to_encode,
            extras,
            broadcast_tx,
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::{
            MessageAccount, MessageAccountInfo, MessageTransaction, MessageTransactionInfo,
        },
        bytes::Bytes,
        prost_types::Timestamp,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::{
            sync::{Arc, OnceLock},
            time::{Duration, SystemTime},
        },
    };

    fn create_test_transaction() -> Message {
        let tx_info = MessageTransactionInfo {
            signature: Signature::from([1u8; 64]),
            is_vote: false,
            transaction: Default::default(),
            meta: Default::default(),
            index: 0,
            account_keys: Default::default(),
            pre_encoded: OnceLock::new(),
        };
        Message::Transaction(MessageTransaction {
            transaction: Arc::new(tx_info),
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        })
    }

    fn create_test_account() -> Message {
        let acc_info = MessageAccountInfo {
            pubkey: Pubkey::new_unique(),
            lamports: 1000,
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
            data: Bytes::from(vec![1, 2, 3]),
            write_version: 1,
            txn_signature: None,
            pre_encoded: OnceLock::new(),
        };
        Message::Account(MessageAccount {
            account: Arc::new(acc_info),
            slot: 100,
            is_startup: false,
            created_at: Timestamp::from(SystemTime::now()),
        })
    }

    #[tokio::test]
    async fn test_parallel_encoder_transactions() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let batch: Vec<(u64, Message)> = (0..10).map(|i| (i, create_test_transaction())).collect();

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 10);
        for (_msgid, msg) in encoded {
            if let Message::Transaction(tx) = msg {
                assert!(
                    tx.transaction.pre_encoded.get().is_some(),
                    "transaction should be encoded"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_parallel_encoder_accounts() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let batch: Vec<(u64, Message)> = (0..10).map(|i| (i, create_test_account())).collect();

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 10);
        for (_msgid, msg) in encoded {
            if let Message::Account(acc) = msg {
                assert!(
                    acc.account.pre_encoded.get().is_some(),
                    "account should be encoded"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_small_batch_uses_sync() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        // Small batch < 4 should use sync path
        let batch: Vec<(u64, Message)> = (0..2).map(|i| (i, create_test_transaction())).collect();

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 2);
    }

    #[tokio::test]
    async fn test_mixed_batch() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let mut batch: Vec<(u64, Message)> = Vec::new();
        for i in 0..5 {
            batch.push((i * 2, create_test_transaction()));
            batch.push((i * 2 + 1, create_test_account()));
        }

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 10);
    }

    #[tokio::test]
    async fn test_encode_fire_and_broadcast() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let (broadcast_tx, mut rx) =
            broadcast::channel::<(CommitmentLevel, Arc<Vec<(u64, Message)>>)>(16);

        // batch of 5 transactions goes to Processed
        let batch: Vec<(u64, Message)> = (0..5u64).map(|i| (i, create_test_transaction())).collect();

        // one Confirmed item and one Finalized item as extras
        let confirmed_item = create_test_transaction();
        let finalized_item = create_test_account();
        let extras = vec![
            (CommitmentLevel::Confirmed, vec![(10u64, confirmed_item)]),
            (CommitmentLevel::Finalized, vec![(11u64, finalized_item)]),
        ];

        encoder.encode_fire_and_broadcast(batch, extras, broadcast_tx);

        // recv 1: Processed, 5 items, all pre_encoded set
        let (cl1, msgs1) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for Processed")
            .expect("broadcast closed");
        assert_eq!(cl1, CommitmentLevel::Processed);
        assert_eq!(msgs1.len(), 5);
        for (_, msg) in msgs1.iter() {
            if let Message::Transaction(tx) = msg {
                assert!(
                    tx.transaction.pre_encoded.get().is_some(),
                    "transaction should be pre-encoded"
                );
            }
        }

        // recv 2: Confirmed, 1 item
        let (cl2, msgs2) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for Confirmed")
            .expect("broadcast closed");
        assert_eq!(cl2, CommitmentLevel::Confirmed);
        assert_eq!(msgs2.len(), 1);

        // recv 3: Finalized, 1 item
        let (cl3, msgs3) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for Finalized")
            .expect("broadcast closed");
        assert_eq!(cl3, CommitmentLevel::Finalized);
        assert_eq!(msgs3.len(), 1);

        // no 4th actual message within 100ms (channel-closed is acceptable)
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Err(_elapsed) => {}                // timed out — no extra message
            Ok(Err(_closed)) => {}             // sender dropped after last send — fine
            Ok(Ok((cl, _))) => panic!("unexpected 4th broadcast message with level {cl:?}"),
        }
    }
}
