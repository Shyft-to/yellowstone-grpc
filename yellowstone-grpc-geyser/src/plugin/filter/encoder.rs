use {
    crate::plugin::{
        filter::message::{prost_bytes_encode_raw, prost_bytes_encoded_len},
        message::{Message, MessageAccountInfo, MessageTransactionInfo},
    },
    bytes::Bytes,
};

pub struct TransactionEncoder;

impl TransactionEncoder {
    pub fn pre_encode(tx: &MessageTransactionInfo) {
        let len = Self::encoded_len(tx);
        let mut buf = Vec::with_capacity(len);
        Self::encode_raw(tx, &mut buf);
        let _ = tx.pre_encoded.set(Bytes::from(buf));
    }

    fn encode_raw(tx: &MessageTransactionInfo, buf: &mut impl bytes::BufMut) {
        use prost::encoding::{encode_key, encode_varint, message, WireType};

        let index = tx.index as u64;

        encode_key(1u32, WireType::LengthDelimited, buf);
        encode_varint(tx.signature.as_ref().len() as u64, buf);
        buf.put_slice(tx.signature.as_ref());

        if tx.is_vote {
            prost::encoding::bool::encode(2u32, &tx.is_vote, buf);
        }

        message::encode(3u32, &tx.transaction, buf);
        message::encode(4u32, &tx.meta, buf);

        if index != 0u64 {
            prost::encoding::uint64::encode(5u32, &index, buf);
        }
    }

    pub fn encoded_len(tx: &MessageTransactionInfo) -> usize {
        use prost::encoding::{encoded_len_varint, key_len, message};

        let index = tx.index as u64;
        let sig_len = tx.signature.as_ref().len();

        key_len(1u32)
            + encoded_len_varint(sig_len as u64)
            + sig_len
            + if tx.is_vote {
                prost::encoding::bool::encoded_len(2u32, &tx.is_vote)
            } else {
                0
            }
            + message::encoded_len(3u32, &tx.transaction)
            + message::encoded_len(4u32, &tx.meta)
            + if index != 0u64 {
                prost::encoding::uint64::encoded_len(5u32, &index)
            } else {
                0
            }
    }
}

pub struct AccountEncoder;

impl AccountEncoder {
    pub fn pre_encode(account: &MessageAccountInfo) {
        let len = Self::encoded_len(account);
        let mut buf = Vec::with_capacity(len);

        prost_bytes_encode_raw(1u32, account.pubkey.as_ref(), &mut buf);
        if account.lamports != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &account.lamports, &mut buf);
        }
        prost_bytes_encode_raw(3u32, account.owner.as_ref(), &mut buf);
        if account.executable {
            ::prost::encoding::bool::encode(4u32, &account.executable, &mut buf);
        }
        if account.rent_epoch != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &account.rent_epoch, &mut buf);
        }
        if !account.data.is_empty() {
            prost_bytes_encode_raw(6u32, &account.data, &mut buf);
        }
        if account.write_version != 0u64 {
            ::prost::encoding::uint64::encode(7u32, &account.write_version, &mut buf);
        }
        if let Some(value) = &account.txn_signature {
            prost_bytes_encode_raw(8u32, value.as_ref(), &mut buf);
        }

        let _ = account.pre_encoded.set(Bytes::from(buf));
    }

    pub fn encoded_len(account: &MessageAccountInfo) -> usize {
        prost_bytes_encoded_len(1u32, account.pubkey.as_ref())
            + if account.lamports != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &account.lamports)
            } else {
                0
            }
            + prost_bytes_encoded_len(3u32, account.owner.as_ref())
            + if account.executable {
                ::prost::encoding::bool::encoded_len(4u32, &account.executable)
            } else {
                0
            }
            + if account.rent_epoch != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &account.rent_epoch)
            } else {
                0
            }
            + if !account.data.is_empty() {
                prost_bytes_encoded_len(6u32, &account.data)
            } else {
                0
            }
            + if account.write_version != 0u64 {
                ::prost::encoding::uint64::encoded_len(7u32, &account.write_version)
            } else {
                0
            }
            + account
                .txn_signature
                .map_or(0, |sig| prost_bytes_encoded_len(8u32, sig.as_ref()))
    }
}

/// Sequentially pre-encodes every not-yet-encoded transaction/account message in `messages`.
///
/// Messages that already have `pre_encoded` set (e.g. re-delivered/backfilled messages) are
/// left untouched; other message kinds are no-ops.
pub fn encode_messages(messages: &[(u64, Message)]) {
    for (_msgid, msg) in messages {
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
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::{
            MessageAccount, MessageAccountInfo, MessageTransaction, MessageTransactionInfo,
        },
        prost_types::Timestamp,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::{
            sync::{Arc, OnceLock},
            time::SystemTime,
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

    #[test]
    fn test_encode_messages_transactions() {
        let batch: Vec<(u64, Message)> = (0..10).map(|i| (i, create_test_transaction())).collect();

        encode_messages(&batch);

        assert_eq!(batch.len(), 10);
        for (_msgid, msg) in &batch {
            if let Message::Transaction(tx) = msg {
                assert!(
                    tx.transaction.pre_encoded.get().is_some(),
                    "transaction should be encoded"
                );
            }
        }
    }

    #[test]
    fn test_encode_messages_accounts() {
        let batch: Vec<(u64, Message)> = (0..10).map(|i| (i, create_test_account())).collect();

        encode_messages(&batch);

        assert_eq!(batch.len(), 10);
        for (_msgid, msg) in &batch {
            if let Message::Account(acc) = msg {
                assert!(
                    acc.account.pre_encoded.get().is_some(),
                    "account should be encoded"
                );
            }
        }
    }

    #[test]
    fn test_encode_messages_small_batch() {
        // Small batches (previously routed through the sync fallback) must still encode.
        let batch: Vec<(u64, Message)> = (0..2).map(|i| (i, create_test_transaction())).collect();

        encode_messages(&batch);

        assert_eq!(batch.len(), 2);
        for (_msgid, msg) in &batch {
            if let Message::Transaction(tx) = msg {
                assert!(tx.transaction.pre_encoded.get().is_some());
            }
        }
    }

    #[test]
    fn test_encode_messages_mixed_batch() {
        let mut batch: Vec<(u64, Message)> = Vec::new();
        for i in 0..5 {
            batch.push((i * 2, create_test_transaction()));
            batch.push((i * 2 + 1, create_test_account()));
        }

        encode_messages(&batch);

        assert_eq!(batch.len(), 10);
        for (_msgid, msg) in &batch {
            match msg {
                Message::Transaction(tx) => {
                    assert!(tx.transaction.pre_encoded.get().is_some())
                }
                Message::Account(acc) => assert!(acc.account.pre_encoded.get().is_some()),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_encode_messages_is_idempotent_for_already_encoded_messages() {
        let batch: Vec<(u64, Message)> =
            vec![(0, create_test_transaction()), (1, create_test_account())];

        // First pass encodes both messages.
        encode_messages(&batch);

        let (tx_before, acc_before) = match (&batch[0].1, &batch[1].1) {
            (Message::Transaction(tx), Message::Account(acc)) => (
                tx.transaction.pre_encoded.get().cloned(),
                acc.account.pre_encoded.get().cloned(),
            ),
            _ => unreachable!(),
        };
        assert!(tx_before.is_some());
        assert!(acc_before.is_some());

        // Second pass must leave already-encoded messages untouched (OnceLock::set is a no-op
        // once populated, and encode_messages skips the pre_encode call entirely).
        encode_messages(&batch);

        let (tx_after, acc_after) = match (&batch[0].1, &batch[1].1) {
            (Message::Transaction(tx), Message::Account(acc)) => (
                tx.transaction.pre_encoded.get().cloned(),
                acc.account.pre_encoded.get().cloned(),
            ),
            _ => unreachable!(),
        };

        assert_eq!(tx_before, tx_after);
        assert_eq!(acc_before, acc_after);
    }
}
