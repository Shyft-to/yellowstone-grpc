use {
    crate::{
        grpc::ReplayedResponse,
        metrics,
        plugin::message::{
            CommitmentLevel, Message, MessageBlock, MessageBlockMeta, MessageEntry, MessageSlot,
            MessageTransactionInfo, SlotStatus,
        },
    },
    log::error,
    prost_types::Timestamp,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, HashMap},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::SystemTime,
    },
    tokio::sync::{mpsc, oneshot},
};

/// Slots retained beyond replay buffer for parent chain status propagation
/// and late-arriving block_meta messages.
const FINALIZATION_SAFETY_BUFFER: u64 = 10;

#[derive(Debug, Default)]
pub(crate) struct MessageId {
    id: u64,
}

impl MessageId {
    const fn next(&mut self) -> u64 {
        self.id = self.id.checked_add(1).expect("message id overflow");
        self.id
    }
}

#[derive(Debug, Default)]
pub(crate) struct SlotMessages {
    messages: Vec<Option<(u64, Message)>>, // Option is used for accounts with low write_version
    messages_slots: Vec<(u64, Message)>,
    block_meta: Option<Arc<MessageBlockMeta>>,
    transactions: Vec<Arc<MessageTransactionInfo>>,
    accounts_dedup: HashMap<Pubkey, (u64, usize)>, // (write_version, message_index)
    entries: Vec<Arc<MessageEntry>>,
    sealed: bool,
    entries_count: usize,
    confirmed_at: Option<usize>,
    finalized_at: Option<usize>,
    parent_slot: Option<Slot>,
    confirmed: bool,
    finalized: bool,
}

impl SlotMessages {
    pub fn try_seal(&mut self, msgid_gen: &mut MessageId) -> Option<(u64, Message)> {
        if !self.sealed {
            if let Some(block_meta) = &self.block_meta {
                let executed_transaction_count = block_meta.executed_transaction_count as usize;
                let entries_count = block_meta.entries_count as usize;

                // Additional check `entries_count == 0` due to bug of zero entries on block produced by validator
                // See GitHub issue: https://github.com/solana-labs/solana/issues/33823
                if self.transactions.len() == executed_transaction_count
                    && (entries_count == 0 || self.entries.len() == entries_count)
                {
                    let transactions = std::mem::take(&mut self.transactions);
                    let mut entries = std::mem::take(&mut self.entries);
                    if entries_count == 0 {
                        entries.clear();
                    }

                    let mut accounts = Vec::with_capacity(self.messages.len());
                    for item in self.messages.iter().flatten() {
                        if let (_msgid, Message::Account(account)) = item {
                            accounts.push(Arc::clone(&account.account));
                        }
                    }

                    let message_block = Message::Block(Arc::new(MessageBlock::new(
                        Arc::clone(block_meta),
                        transactions,
                        accounts,
                        entries,
                    )));
                    let message = (msgid_gen.next(), message_block);
                    self.messages.push(Some(message.clone()));

                    self.sealed = true;
                    self.entries_count = entries_count;
                    return Some(message);
                }
            }
        }

        None
    }
}

/// A single message ready to be handed to the broadcast fan-out, together
/// with the (already BTreeMap-derived) Confirmed/Finalized batches that
/// should accompany it, if any.
#[derive(Debug)]
pub(crate) struct DispatchItem {
    pub message: (u64, Message),
    pub confirmed_messages: Option<Vec<(u64, Message)>>,
    pub finalized_messages: Option<Vec<(u64, Message)>>,
}

/// Owns the per-slot block reconstruction bookkeeping (dedup, sealing, gc,
/// missed-status backfill, replay buffer) that both `geyser_dispatch` and
/// `geyser_loop` maintain identically today.
#[derive(Debug)]
pub(crate) struct BlockReconstructionState {
    msgid_gen: MessageId,
    messages: BTreeMap<u64, SlotMessages>,
    processed_first_slot: Option<u64>,
    blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
    replay_first_available_slot: Option<Arc<AtomicU64>>,
    replay_stored_slots: u64,
}

impl BlockReconstructionState {
    pub(crate) fn new(
        blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
        replay_first_available_slot: Option<Arc<AtomicU64>>,
        replay_stored_slots: u64,
    ) -> Self {
        Self {
            msgid_gen: MessageId::default(),
            messages: BTreeMap::default(),
            processed_first_slot: None,
            blocks_meta_tx,
            replay_first_available_slot,
            replay_stored_slots,
        }
    }

    /// Applies the bookkeeping for a single incoming message (gc sweep,
    /// dedup by write_version, block sealing, missed-status ancestor
    /// backfill) and returns the messages ready to be broadcast, in the
    /// exact order they should be sent, each already carrying its
    /// Confirmed/Finalized companions (if any) as derived from the BTreeMap.
    pub(crate) fn on_message(
        &mut self,
        message: Message,
    ) -> impl Iterator<Item = DispatchItem> + '_ {
        metrics::message_queue_size_dec();
        let msgid = self.msgid_gen.next();

        if let Message::Slot(slot_message) = &message {
            metrics::update_slot_plugin_status(slot_message.status, slot_message.slot);
        }

        if let Some(blocks_meta_tx) = &self.blocks_meta_tx {
            if matches!(&message, Message::Slot(_) | Message::BlockMeta(_)) {
                let _ = blocks_meta_tx.send(message.clone());
            }
        }

        self.gc_finalized_slots(&message);

        let slot_messages = self.messages.entry(message.get_slot()).or_default();
        if let Message::Slot(msg) = &message {
            match msg.status {
                SlotStatus::Processed => {
                    slot_messages.parent_slot = msg.parent;
                }
                SlotStatus::Confirmed => {
                    slot_messages.confirmed = true;
                }
                SlotStatus::Finalized => {
                    slot_messages.finalized = true;
                }
                _ => {}
            }
        }
        if matches!(&message, Message::Slot(_)) {
            slot_messages.messages_slots.push((msgid, message.clone()));
        } else {
            slot_messages.messages.push(Some((msgid, message.clone())));

            // If we already build Block message, new message will be a problem
            if slot_messages.sealed
                && !(matches!(&message, Message::Entry(_)) && slot_messages.entries_count == 0)
            {
                let kind = match &message {
                    Message::Slot(_) => "Slot",
                    Message::Account(_) => "Account",
                    Message::Transaction(_) => "Transaction",
                    Message::Entry(_) => "Entry",
                    Message::BlockMeta(_) => "BlockMeta",
                    Message::Block(_) => "Block",
                };
                metrics::update_invalid_blocks(format!("unexpected message {kind}"));
            }
        }

        let mut sealed_block_msg = None;
        match &message {
            Message::BlockMeta(msg) => {
                if slot_messages.block_meta.is_some() {
                    metrics::update_invalid_blocks("unexpected message: BlockMeta (duplicate)");
                }
                slot_messages.block_meta = Some(Arc::clone(msg));
                sealed_block_msg = slot_messages.try_seal(&mut self.msgid_gen);
            }
            Message::Transaction(msg) => {
                slot_messages.transactions.push(Arc::clone(&msg.transaction));
                sealed_block_msg = slot_messages.try_seal(&mut self.msgid_gen);
            }
            // Dedup accounts by max write_version
            Message::Account(msg) => {
                metrics::observe_geyser_account_update_received(msg.account.data.len());
                let write_version = msg.account.write_version;
                let msg_index = slot_messages.messages.len() - 1;
                if let Some(entry) = slot_messages.accounts_dedup.get_mut(&msg.account.pubkey) {
                    if entry.0 < write_version {
                        // We can replace the message, but in this case we will lose the order
                        slot_messages.messages[entry.1] = None;
                        *entry = (write_version, msg_index);
                    } else {
                        // If the new write_version is lower than the latest one, we need to drop this message
                        // because we would have more than 1 image in slot_messages.messages
                        slot_messages.messages[msg_index] = None;
                    }
                } else {
                    slot_messages
                        .accounts_dedup
                        .insert(msg.account.pubkey, (write_version, msg_index));
                }
            }
            Message::Entry(msg) => {
                slot_messages.entries.push(Arc::clone(msg));
                sealed_block_msg = slot_messages.try_seal(&mut self.msgid_gen);
            }
            _ => {}
        }

        // Send messages to filter (and to clients)
        let mut messages_vec = Vec::with_capacity(4);
        if let Some(sealed_block_msg) = sealed_block_msg {
            messages_vec.push(sealed_block_msg);
        }
        let slot_status = if let Message::Slot(msg) = &message {
            Some((msg.slot, msg.status))
        } else {
            None
        };
        messages_vec.push((msgid, message));

        // sometimes we do not receive all statuses
        if let Some((slot, status)) = slot_status {
            self.backfill_missed_status(slot, status, &mut messages_vec);
        }

        self.build_dispatch_items(messages_vec)
    }

    /// Removes outdated block reconstruction info once a slot is finalized
    /// far enough behind the replay buffer.
    fn gc_finalized_slots(&mut self, message: &Message) {
        // On startup we can receive multiple Confirmed/Finalized slots without BlockMeta message
        // With saved first Processed slot we can ignore errors caused by startup process
        match message {
            Message::Slot(msg)
                if self.processed_first_slot.is_none() && msg.status == SlotStatus::Processed =>
            {
                self.processed_first_slot = Some(msg.slot);
            }
            Message::Slot(msg) if msg.status == SlotStatus::Finalized => {
                // keep extra 10 slots + slots for replay
                if let Some(msg_slot) = msg
                    .slot
                    .checked_sub(FINALIZATION_SAFETY_BUFFER + self.replay_stored_slots)
                {
                    loop {
                        match self.messages.keys().next().cloned() {
                            Some(slot) if slot < msg_slot => {
                                if let Some(slot_messages) = self.messages.remove(&slot) {
                                    match self.processed_first_slot {
                                        Some(processed_first) if slot <= processed_first => {
                                            continue
                                        }
                                        None => continue,
                                        _ => {}
                                    }

                                    if !slot_messages.sealed && slot_messages.finalized_at.is_some()
                                    {
                                        let mut reasons = vec![];
                                        if let Some(block_meta) = slot_messages.block_meta {
                                            let block_txn_count =
                                                block_meta.executed_transaction_count as usize;
                                            let msg_txn_count = slot_messages.transactions.len();
                                            if block_txn_count != msg_txn_count {
                                                reasons.push("InvalidTxnCount");
                                                error!("failed to reconstruct #{slot} -- tx count: {block_txn_count} vs {msg_txn_count}");
                                            }
                                            let block_entries_count =
                                                block_meta.entries_count as usize;
                                            let msg_entries_count = slot_messages.entries.len();
                                            if block_entries_count != msg_entries_count {
                                                reasons.push("InvalidEntriesCount");
                                                error!("failed to reconstruct #{slot} -- entries count: {block_entries_count} vs {msg_entries_count}");
                                            }
                                        } else {
                                            reasons.push("NoBlockMeta");
                                        }
                                        let reason = reasons.join(",");

                                        metrics::update_invalid_blocks(format!(
                                            "failed reconstruct {reason}"
                                        ));
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                    if let Some(stored) = &self.replay_first_available_slot {
                        if let Some(slot) = self.messages.keys().next().copied() {
                            stored.store(slot, Ordering::Relaxed);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Synthesizes missing ancestor status updates: e.g. if a slot jumps
    /// straight to Confirmed/Finalized without its ancestors ever receiving
    /// their own status message, backfill one for each ancestor missing it.
    fn backfill_missed_status(
        &mut self,
        slot: Slot,
        status: SlotStatus,
        messages_vec: &mut Vec<(u64, Message)>,
    ) {
        let mut slots = vec![slot];
        while let Some((parent, Some(entry))) = slots
            .pop()
            .and_then(|slot| self.messages.get(&slot))
            .and_then(|entry| entry.parent_slot)
            .map(|parent| (parent, self.messages.get_mut(&parent)))
        {
            if (status == SlotStatus::Confirmed && !entry.confirmed)
                || (status == SlotStatus::Finalized && !entry.finalized)
            {
                if status == SlotStatus::Confirmed {
                    entry.confirmed = true;
                } else if status == SlotStatus::Finalized {
                    entry.finalized = true;
                }

                slots.push(parent);
                let message_slot = Message::Slot(MessageSlot {
                    slot: parent,
                    parent: entry.parent_slot,
                    status,
                    dead_error: None,
                    created_at: Timestamp::from(SystemTime::now()),
                });
                messages_vec.push((self.msgid_gen.next(), message_slot));
                metrics::missed_status_message_inc(status);
            }
        }
    }

    /// Converts the raw `messages_vec` (already including any sealed block
    /// message and any backfilled ancestor status messages) into the final
    /// broadcast-ready order, computing the Confirmed/Finalized companions
    /// for each entry from the BTreeMap exactly as today's inlined code
    /// does. Items are produced lazily, one per already-buffered
    /// `messages_vec` entry, without collecting into an intermediate `Vec`.
    ///
    /// The `confirmed_at`/`finalized_at` BTreeMap bookkeeping is recorded
    /// eagerly, in a separate pass, before the lazy iterator is built. This
    /// keeps that mutation's effect independent of how far the caller
    /// drains the returned iterator: dropping it early (e.g. via `break` or
    /// an early return) must never leave the bookkeeping in a state that
    /// differs from a full drain.
    fn build_dispatch_items(
        &mut self,
        messages_vec: Vec<(u64, Message)>,
    ) -> impl Iterator<Item = DispatchItem> + '_ {
        self.record_slot_status_transitions(&messages_vec);

        messages_vec
            .into_iter()
            .rev()
            .map(move |message| self.dispatch_item_for(message))
    }

    /// Unconditionally records, for every Slot-status message in
    /// `messages_vec`, the index into the slot's buffered messages at which
    /// it became Confirmed/Finalized. Distinct entries in `messages_vec`
    /// within a single `on_message` call always target distinct slots, so
    /// running this eagerly (rather than interleaved with per-item
    /// `DispatchItem` construction) is order-independent.
    fn record_slot_status_transitions(&mut self, messages_vec: &[(u64, Message)]) {
        for (_, message) in messages_vec {
            let Message::Slot(slot) = message else {
                continue;
            };
            let Some(slot_messages) = self.messages.get_mut(&slot.slot) else {
                continue;
            };
            if slot_messages.sealed {
                continue;
            }
            match slot.status {
                SlotStatus::Confirmed => {
                    slot_messages.confirmed_at = Some(slot_messages.messages.len());
                }
                SlotStatus::Finalized => {
                    slot_messages.finalized_at = Some(slot_messages.messages.len());
                }
                _ => {}
            }
        }
    }

    /// Computes the Confirmed/Finalized companions (if any) for a single
    /// message and wraps it into a `DispatchItem`. Read-only: relies on
    /// `record_slot_status_transitions` having already run for this batch.
    fn dispatch_item_for(&self, message: (u64, Message)) -> DispatchItem {
        if let Message::Slot(slot) = &message.1 {
            let (mut confirmed_messages, mut finalized_messages) = match slot.status {
                SlotStatus::Processed
                | SlotStatus::FirstShredReceived
                | SlotStatus::Completed
                | SlotStatus::CreatedBank
                | SlotStatus::Dead => (Vec::with_capacity(1), Vec::with_capacity(1)),
                SlotStatus::Confirmed => {
                    let vec = self
                        .messages
                        .get(&slot.slot)
                        .map(|slot_messages| {
                            slot_messages.messages.iter().flatten().cloned().collect()
                        })
                        .unwrap_or_default();
                    (vec, Vec::with_capacity(1))
                }
                SlotStatus::Finalized => {
                    let vec = self
                        .messages
                        .get(&slot.slot)
                        .map(|slot_messages| {
                            slot_messages.messages.iter().flatten().cloned().collect()
                        })
                        .unwrap_or_default();
                    (Vec::with_capacity(1), vec)
                }
            };

            confirmed_messages.push(message.clone());
            finalized_messages.push(message.clone());
            DispatchItem {
                message,
                confirmed_messages: Some(confirmed_messages),
                finalized_messages: Some(finalized_messages),
            }
        } else {
            let mut confirmed_messages = vec![];
            let mut finalized_messages = vec![];
            if matches!(&message.1, Message::Block(_)) {
                if let Some(slot_messages) = self.messages.get(&message.1.get_slot()) {
                    if let Some(confirmed_at) = slot_messages.confirmed_at {
                        confirmed_messages.extend(
                            slot_messages.messages.as_slice()[confirmed_at..]
                                .iter()
                                .filter_map(|x| x.clone()),
                        );
                    }
                    if let Some(finalized_at) = slot_messages.finalized_at {
                        finalized_messages.extend(
                            slot_messages.messages.as_slice()[finalized_at..]
                                .iter()
                                .filter_map(|x| x.clone()),
                        );
                    }
                }
            }

            DispatchItem {
                message,
                confirmed_messages: (!confirmed_messages.is_empty()).then_some(confirmed_messages),
                finalized_messages: (!finalized_messages.is_empty()).then_some(finalized_messages),
            }
        }
    }

    /// Services a single replay-from-slot request against the current
    /// BTreeMap, identical to today's inline handling in both
    /// `geyser_dispatch` and `geyser_loop`.
    pub(crate) fn service_replay(
        &self,
        commitment: CommitmentLevel,
        replay_slot: Slot,
        tx: oneshot::Sender<ReplayedResponse>,
    ) {
        if let Some((slot, _)) = self.messages.first_key_value() {
            if replay_slot < *slot {
                let _ = tx.send(ReplayedResponse::Lagged(*slot));
                return;
            }
        }

        let mut replayed_messages = Vec::with_capacity(32_768);
        for (slot, msgs) in self.messages.iter() {
            if *slot >= replay_slot {
                replayed_messages.extend_from_slice(&msgs.messages_slots);
                if commitment == CommitmentLevel::Processed
                    || (commitment == CommitmentLevel::Finalized && msgs.finalized)
                    || (commitment == CommitmentLevel::Confirmed && msgs.confirmed)
                {
                    replayed_messages.extend(msgs.messages.iter().filter_map(|v| v.clone()));
                }
            }
        }
        let _ = tx.send(ReplayedResponse::Messages(replayed_messages));
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::{MessageAccount, MessageAccountInfo, MessageTransaction},
        bytes::Bytes,
        solana_hash::Hash,
        solana_signature::Signature,
        std::{collections::HashSet, sync::OnceLock},
        yellowstone_grpc_proto::{
            geyser::SubscribeUpdateBlockMeta, solana::storage::confirmed_block,
        },
    };

    fn unique_signature(seed: u64) -> Signature {
        let mut bytes = [0u8; 64];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Signature::from(bytes)
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

    fn new_state() -> BlockReconstructionState {
        BlockReconstructionState::new(None, None, 100)
    }

    /// Applies a message and fully drains the resulting dispatch items when
    /// the test only cares about the side effects on `state`, not the
    /// returned items themselves (mirrors how `geyser_dispatch`/`geyser_loop`
    /// always drain the iterator to completion).
    fn apply(state: &mut BlockReconstructionState, message: Message) {
        for _ in state.on_message(message) {}
    }

    // --- 1. Dedup by write_version -----------------------------------------

    #[test]
    fn dedup_by_write_version() {
        let mut state = new_state();
        let pubkey = Pubkey::new_unique();
        apply(&mut state, make_account(10, pubkey, 5));
        apply(&mut state, make_account(10, pubkey, 2));
        let items = state.on_message(make_slot(10, None, SlotStatus::Confirmed));

        let confirmed = items
            .into_iter()
            .find_map(|item| item.confirmed_messages)
            .expect("Confirmed slot message should carry a confirmed batch");
        let write_versions: Vec<_> = confirmed
            .iter()
            .filter_map(|(_, m)| match m {
                Message::Account(a) => Some(a.account.write_version),
                _ => None,
            })
            .collect();
        assert_eq!(
            write_versions,
            vec![5],
            "only the higher write_version account update should survive dedup"
        );
    }

    // --- 2. Block sealing gating ---------------------------------------------

    #[test]
    fn block_seals_when_counts_match() {
        let mut state = new_state();
        apply(&mut state, make_block_meta(50, 1, 1));
        apply(&mut state, make_transaction(50, unique_signature(1)));
        let mut items = state.on_message(make_entry(50, 0));

        assert!(
            items.any(|item| matches!(item.message.1, Message::Block(_))),
            "a sealed Block message should be produced once tx and entry counts match block_meta"
        );
    }

    #[test]
    fn block_seal_gated_by_mismatched_counts() {
        let mut state = new_state();
        // entries_count == 0 takes the "no entries expected" branch, so
        // only the transaction count can mismatch here.
        assert!(!state
            .on_message(make_block_meta(60, 2, 0))
            .any(|item| matches!(item.message.1, Message::Block(_))));
        assert!(
            !state
                .on_message(make_transaction(60, unique_signature(2)))
                .any(|item| matches!(item.message.1, Message::Block(_))),
            "no Block message should ever be produced while tx count (1) != executed_transaction_count (2)"
        );
    }

    // --- 3. Gc timing ---------------------------------------------------------

    #[test]
    fn gc_retains_until_safety_buffer() {
        let replay_stored_slots = 5u64;
        let mut state = BlockReconstructionState::new(None, None, replay_stored_slots);

        for slot in 1..=30u64 {
            apply(&mut state, make_account(slot, Pubkey::new_unique(), 1));
        }
        apply(&mut state, make_slot(30, Some(29), SlotStatus::Finalized));

        let expected_earliest = 30 - (FINALIZATION_SAFETY_BUFFER + replay_stored_slots);
        assert_eq!(
            state.messages.keys().next().copied(),
            Some(expected_earliest),
            "earliest surviving slot should be exactly FINALIZATION_SAFETY_BUFFER + replay_stored_slots \
             behind the finalized slot: not gc'd before that boundary, not retained past it"
        );
    }

    // --- 4. Missed-status parent-slot propagation -----------------------------

    #[test]
    fn missed_status_backfill_populates_ancestors() {
        let mut state = new_state();
        apply(&mut state, make_slot(100, None, SlotStatus::Processed));
        apply(&mut state, make_slot(101, Some(100), SlotStatus::Processed));
        apply(&mut state, make_slot(102, Some(101), SlotStatus::Processed));
        let items = state.on_message(make_slot(102, Some(101), SlotStatus::Confirmed));

        let confirmed_slots: Vec<_> = items
            .filter_map(|item| match &item.message.1 {
                Message::Slot(s) if s.status == SlotStatus::Confirmed => Some(s.slot),
                _ => None,
            })
            .collect();

        assert!(
            confirmed_slots.contains(&100),
            "ancestor slot 100 should get a synthesized Confirmed status backfilled from slot 102"
        );
        assert!(
            confirmed_slots.contains(&101),
            "ancestor slot 101 should get a synthesized Confirmed status backfilled from slot 102"
        );
        assert!(confirmed_slots.contains(&102));
    }

    // --- 5. Dispatch bookkeeping independent of iterator consumption -----------

    #[test]
    fn dispatch_bookkeeping_unaffected_by_partial_iterator_drain() {
        let pubkey = Pubkey::new_unique();

        let mut fully_drained = new_state();
        apply(&mut fully_drained, make_account(20, pubkey, 1));
        apply(&mut fully_drained, make_slot(20, None, SlotStatus::Confirmed));

        let mut zero_pulled = new_state();
        apply(&mut zero_pulled, make_account(20, pubkey, 1));
        // Drop the iterator without pulling any items at all.
        drop(zero_pulled.on_message(make_slot(20, None, SlotStatus::Confirmed)));

        assert_eq!(
            fully_drained.messages.get(&20).unwrap().confirmed_at,
            zero_pulled.messages.get(&20).unwrap().confirmed_at,
            "confirmed_at bookkeeping must be recorded even if the dispatch iterator is never pulled"
        );

        let mut one_pulled = new_state();
        apply(&mut one_pulled, make_account(20, pubkey, 1));
        let mut items = one_pulled.on_message(make_slot(20, None, SlotStatus::Confirmed));
        items.next();
        drop(items);

        assert_eq!(
            fully_drained.messages.get(&20).unwrap().confirmed_at,
            one_pulled.messages.get(&20).unwrap().confirmed_at,
            "confirmed_at bookkeeping must match a full drain after pulling only one item"
        );

        // Same check for the Finalized transition.
        let mut fully_drained_final = new_state();
        apply(&mut fully_drained_final, make_account(21, pubkey, 1));
        apply(
            &mut fully_drained_final,
            make_slot(21, None, SlotStatus::Finalized),
        );

        let mut zero_pulled_final = new_state();
        apply(&mut zero_pulled_final, make_account(21, pubkey, 1));
        drop(zero_pulled_final.on_message(make_slot(21, None, SlotStatus::Finalized)));

        assert_eq!(
            fully_drained_final.messages.get(&21).unwrap().finalized_at,
            zero_pulled_final.messages.get(&21).unwrap().finalized_at,
            "finalized_at bookkeeping must be recorded even if the dispatch iterator is never pulled"
        );
    }

    // --- 6. Replay-buffer servicing --------------------------------------------

    #[test]
    fn replay_servicing_returns_in_range_messages_and_lagged_for_out_of_range() {
        let mut state = new_state();
        for slot in 10..=15u64 {
            apply(
                &mut state,
                make_slot(slot, Some(slot.saturating_sub(1)), SlotStatus::Processed),
            );
        }

        let (tx, mut rx) = oneshot::channel();
        state.service_replay(CommitmentLevel::Processed, 12, tx);
        match rx.try_recv().expect("service_replay should have replied") {
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

        let (tx2, mut rx2) = oneshot::channel();
        state.service_replay(CommitmentLevel::Processed, 5, tx2);
        match rx2.try_recv().expect("service_replay should have replied") {
            ReplayedResponse::Lagged(earliest) => assert_eq!(earliest, 10),
            ReplayedResponse::Messages(_) => {
                panic!("expected a Lagged response for an out-of-range from_slot")
            }
        }
    }
}
