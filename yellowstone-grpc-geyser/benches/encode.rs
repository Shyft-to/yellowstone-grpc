use {
    criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion},
    prost::Message as _,
    prost_types::Timestamp,
    rayon::{prelude::*, ThreadPool, ThreadPoolBuilder},
    std::{
        sync::{Arc, OnceLock},
        time::{Duration, SystemTime},
    },
    yellowstone_grpc_geyser::plugin::{
        filter::{
            encoder::{encode_messages, AccountEncoder, TransactionEncoder},
            message::{
                tests::{
                    create_accounts, create_message_filters, load_predefined_blocks,
                    load_predefined_transactions,
                },
                FilteredUpdate, FilteredUpdateOneof,
            },
        },
        message::{
            Message, MessageAccount, MessageAccountInfo, MessageTransaction, MessageTransactionInfo,
        },
    },
};

fn bench_account(c: &mut Criterion) {
    let filters = create_message_filters(&["my special filter"]);

    macro_rules! bench {
        ($updates:expr, $kind:expr) => {
            c.bench_with_input(BenchmarkId::new($kind, "ref"), $updates, |b, updates| {
                b.iter(|| {
                    for update in updates.iter() {
                        update.encode_to_vec().len();
                    }
                })
            });
            c.bench_with_input(BenchmarkId::new($kind, "prost"), $updates, |b, updates| {
                b.iter(|| {
                    for update in updates.iter() {
                        update.as_subscribe_update().encode_to_vec().len();
                    }
                })
            });
        };
    }

    let updates = create_accounts()
        .into_iter()
        .map(|(msg, data_slice)| FilteredUpdate {
            filters: filters.clone(),
            message: FilteredUpdateOneof::account(&msg, data_slice),
            created_at: Timestamp::from(SystemTime::now()),
        })
        .collect::<Vec<_>>();
    bench!(&updates, "accounts");

    let updates = load_predefined_transactions()
        .into_iter()
        .map(|transaction| FilteredUpdate {
            filters: filters.clone(),
            message: FilteredUpdateOneof::transaction(&MessageTransaction {
                transaction,
                slot: 42,
                created_at: Timestamp::from(SystemTime::now()),
            }),
            created_at: Timestamp::from(SystemTime::now()),
        })
        .collect::<Vec<_>>();
    bench!(&updates, "transactions");

    let updates = load_predefined_blocks()
        .into_iter()
        .map(|block| FilteredUpdate {
            filters: filters.clone(),
            message: FilteredUpdateOneof::block(Box::new(block)),
            created_at: Timestamp::from(SystemTime::now()),
        })
        .collect::<Vec<_>>();
    bench!(&updates, "blocks");
}

/// Builds a fresh `size`-length batch of alternating transaction/account messages, each with
/// its own never-yet-set `pre_encoded` slot, so every benchmark iteration measures real encode
/// work rather than a `OnceLock`-skipped no-op.
fn build_batch(
    size: usize,
    tx_template: &MessageTransactionInfo,
    account_template: &MessageAccountInfo,
) -> Vec<(u64, Message)> {
    (0..size as u64)
        .map(|i| {
            let message = if i % 2 == 0 {
                Message::Transaction(MessageTransaction {
                    transaction: Arc::new(MessageTransactionInfo {
                        pre_encoded: OnceLock::new(),
                        ..tx_template.clone()
                    }),
                    slot: 42,
                    created_at: Timestamp::from(SystemTime::now()),
                })
            } else {
                Message::Account(MessageAccount {
                    account: Arc::new(MessageAccountInfo {
                        pre_encoded: OnceLock::new(),
                        ..account_template.clone()
                    }),
                    slot: 42,
                    is_startup: false,
                    created_at: Timestamp::from(SystemTime::now()),
                })
            };
            (i, message)
        })
        .collect()
}

/// Reproduces the pre-removal `ParallelEncoder::encode_message` dispatch (see `parallel.rs`
/// prior to this optimization): no-op for already-encoded messages, otherwise pre-encode.
fn encode_one_rayon(msg: &Message) {
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

/// Reproduces the pre-removal `ParallelEncoder::encode_blocking`: sequential below 4 messages,
/// otherwise dispatched across a rayon thread pool via `par_iter_mut`.
fn encode_blocking_rayon(pool: &ThreadPool, mut batch: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
    if batch.len() < 4 {
        for (_, msg) in &mut batch {
            encode_one_rayon(msg);
        }
    } else {
        pool.install(|| {
            batch
                .par_iter_mut()
                .for_each(|(_, msg)| encode_one_rayon(msg));
        });
    }
    batch
}

/// Compares today's sequential `encode_messages()` against the removed rayon/channel-bridge
/// `ParallelEncoder::encode_blocking()` path at batch sizes spanning the observed
/// `GEYSER_BATCH_SIZE` range.
fn bench_encode_dispatch(c: &mut Criterion) {
    let tx_template = load_predefined_transactions()
        .into_iter()
        .next()
        .expect("fixture missing at least one transaction");
    let account_template = create_accounts()
        .into_iter()
        .next()
        .map(|(msg, _data_slice)| msg.account)
        .expect("fixture missing at least one account");

    // Matches the previous `encoder_threads` config default.
    let pool = ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .expect("failed to build rayon pool");

    let mut group = c.benchmark_group("encode_dispatch");
    for size in [1usize, 4, 16, 64, 256] {
        group.bench_with_input(BenchmarkId::new("sequential", size), &size, |b, &size| {
            b.iter_batched(
                || build_batch(size, &tx_template, &account_template),
                // Return `batch` (like `encode_blocking_rayon` below) so both routines defer
                // the batch's drop to after the timed section — otherwise this closure would
                // unfairly count Vec/Message deallocation time that the other one doesn't.
                |batch| {
                    encode_messages(&batch);
                    batch
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(
            BenchmarkId::new("rayon_parallel", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || build_batch(size, &tx_template, &account_template),
                    |batch| encode_blocking_rayon(&pool, batch),
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3)) // default 3
        .measurement_time(Duration::from_secs(5)); // default 5
    targets = bench_account, bench_encode_dispatch
);
criterion_main!(benches);
