use {
    crate::plugin::{filter::encoder::encode_message, message::Message},
    rayon::{prelude::*, ThreadPool, ThreadPoolBuilder},
    std::sync::Arc,
};

/// Batches smaller than this are encoded serially: for a handful of messages the
/// rayon fork/join dispatch costs more than it saves. Mirrors the production
/// branch threshold.
const PARALLEL_ENCODE_THRESHOLD: usize = 4;

/// Pre-encodes a batch of `Message`s across a dedicated rayon thread pool.
///
/// Each message pre-encodes into its own `OnceLock` (`pre_encoded`), so the work
/// is embarrassingly parallel and needs no locking beyond the `OnceLock::set`
/// each message already uses. The pool is isolated from the tokio runtime and
/// from rayon's global pool so encoding never contends with request handling.
pub struct ParallelEncoder {
    pool: Arc<ThreadPool>,
}

impl ParallelEncoder {
    /// Build an encoder backed by `num_threads` worker threads. A value of `0`
    /// lets rayon pick a default based on available parallelism.
    pub fn new(num_threads: usize) -> Self {
        let pool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .thread_name(|i| format!("geyser-encoder-{i}"))
                .build()
                .expect("failed to create rayon encoder pool"),
        );
        Self { pool }
    }

    /// Pre-encode every `Transaction`/`Account` in `messages`, blocking until the
    /// whole batch is done. Small batches run serially on the caller's thread;
    /// larger ones fan out across the pool via `install` (which blocks the caller
    /// until the parallel work completes — same blocking contract the previous
    /// serial `encode_messages` had, just faster).
    pub fn encode_blocking(&self, messages: &[Message]) {
        if messages.len() < PARALLEL_ENCODE_THRESHOLD {
            for msg in messages {
                encode_message(msg);
            }
            return;
        }

        self.pool.install(|| {
            messages.par_iter().for_each(encode_message);
        });
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::{Message, MessageAccount, MessageAccountInfo},
        bytes::Bytes,
        prost_types::Timestamp,
        solana_pubkey::Pubkey,
        std::sync::{Arc, OnceLock},
    };

    fn make_account_msg(write_version: u64) -> Message {
        Message::Account(MessageAccount {
            account: Arc::new(MessageAccountInfo {
                pubkey: Pubkey::new_unique(),
                lamports: 42,
                owner: Pubkey::new_unique(),
                executable: false,
                rent_epoch: 0,
                data: Bytes::from_static(&[1, 2, 3, 4]),
                write_version,
                txn_signature: None,
                pre_encoded: OnceLock::new(),
            }),
            slot: 1,
            is_startup: false,
            created_at: Timestamp::default(),
        })
    }

    fn pre_encoded_len(msg: &Message) -> Option<usize> {
        match msg {
            Message::Account(acc) => acc.account.pre_encoded.get().map(Vec::len),
            _ => None,
        }
    }

    #[test]
    fn encodes_small_batch_serially() {
        let encoder = ParallelEncoder::new(2);
        let msgs: Vec<Message> = (0..2).map(make_account_msg).collect();
        encoder.encode_blocking(&msgs);
        for msg in &msgs {
            assert!(pre_encoded_len(msg).unwrap() > 0);
        }
    }

    #[test]
    fn encodes_large_batch_in_parallel() {
        let encoder = ParallelEncoder::new(4);
        let msgs: Vec<Message> = (0..64).map(make_account_msg).collect();
        encoder.encode_blocking(&msgs);
        for msg in &msgs {
            assert!(pre_encoded_len(msg).unwrap() > 0);
        }
    }

    #[test]
    fn re_encoding_is_idempotent() {
        let encoder = ParallelEncoder::new(2);
        let msgs: Vec<Message> = (0..8).map(make_account_msg).collect();
        encoder.encode_blocking(&msgs);
        let first: Vec<usize> = msgs.iter().map(|m| pre_encoded_len(m).unwrap()).collect();
        // Second pass must not re-encode (OnceLock already set) nor panic.
        encoder.encode_blocking(&msgs);
        let second: Vec<usize> = msgs.iter().map(|m| pre_encoded_len(m).unwrap()).collect();
        assert_eq!(first, second);
    }
}
