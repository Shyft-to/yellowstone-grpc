# Latency optimizations ported from `master` onto `i2` (production)

## Background

Production (`i2`) had already independently reduced end-to-end gRPC fan-out
latency by ~800us through its own work (CPU-pinned spin-loop dispatch,
per-subscriber metrics caching, a filter message-type bitmask fast-path).
The open-source `master` branch, diverging from the same base commit
(`d2648e0`), closed the remaining gap to ~30us through a different set of
changes. This document records what was ported from `master` onto `i2`,
adapted to preserve `i2`'s CPU-pinned architecture rather than replacing it
with `master`'s fully-async design, with exact code references to the
current state of the tree.

Done via the `/implement` multi-agent harness as run
`implement/port-master-latency-opts`, in 8 validated tasks. Full planning
history, round-by-round evaluator feedback, and per-task validator findings
are preserved in that branch's `.claude/state/run_log.md` (3 planning
rounds, since the first two were rejected over real correctness gaps — see
"What the review process caught" below).

Four changes were ported. One (jemalloc) was subsequently reverted after a
production incident — see [`problem.md`](./problem.md) for that investigation.
The other three are live on `i2`.

---

## 1. Removed the `ParallelEncoder` rayon/channel bridge

**Status: retained.**

**Before:** encoding pre-serialized message bytes went through a rayon
thread pool behind an `mpsc`+`oneshot` bridge (`parallel.rs`, now deleted).
For batches ≥4, this crossed into a separate, non-pinned rayon worker pool
via `ThreadPool::install(...)`, even though the actual per-message work
(`OnceLock::set` on a pre-serialized buffer) is cheap.

**After:** a direct, synchronous free function, called inline on whichever
thread is already doing the batching:

```rust
// yellowstone-grpc-geyser/src/plugin/filter/encoder.rs:133
pub fn encode_messages(messages: &[(u64, Message)]) {
```

Called from both dispatch paths — `geyser_dispatch` (the CPU-pinned
spin-loop) via `flush_processed_batch`, and the async `geyser_loop`
fallback — in place of the old `parallel_encoder.encode_blocking(...)` /
`.encode(...).await` calls.

**Why this is a net win, not just simpler code:** a Criterion benchmark
added as part of this task (`yellowstone-grpc-geyser/benches/encode.rs`,
`bench_encode_dispatch`) compared the two paths at batch sizes 1/4/16/64/256.
`processed_messages_max` defaults to 31
(`yellowstone-grpc-geyser/src/config.rs`, `processed_messages_max_default`),
which sits in the range where sequential encoding wins decisively
(1.25×–2.35×+ faster in validator re-testing); the rayon path only pulls
ahead at batch 256, a size that requires an explicit non-default
`processed_messages_max` override — structurally unreachable in the default
deployment this change targets.

**Also removed:** the `encoder_threads` config field (a breaking,
`deny_unknown_fields`-enforced config change, called out in `CHANGELOG.md`),
and the `rayon` runtime dependency (kept only as a `[dev-dependencies]`
entry, used exclusively by the benchmark to reproduce the old path for
comparison — not linked into the shipped plugin).

Reference: master's `1453f2c`.

---

## 2. jemalloc as global allocator

**Status: reverted.** Caused a heap-corruption-shaped crash on first live
validator load (`hashbrown::RawTable::reserve_rehash` requesting ~9.28
petabytes inside `MessageTransactionInfo::from_geyser`). Full root-cause
analysis in [`problem.md`](./problem.md): the `disable_initial_exec_tls`
feature flag needed to avoid a dlopen/TLS crash trades that crash for a
different, upstream-acknowledged risk (two allocators coexisting in one
process), and this fork's CPU-pinned busy-spin dispatch architecture exposes
that risk far more than master's cooperatively-scheduled tokio tasks do.
`tikv-jemallocator` is no longer a dependency of `yellowstone-grpc-geyser`
on `i2`.

Reference: master's `1453f2c`.

---

## 3. Filter foldhash + per-connection `FilterNames`

**Status: retained.**

**Part A — faster hashing on the per-message filter-matching hot path.**
`FilterAccounts`'s account/owner reverse-index lookup tables (queried once
per incoming `Message::Account`, the highest-volume message type) and the
per-message match-set builder were retyped from std `HashMap`/`HashSet`
(SipHash — DoS-resistant but slower, unnecessary here since pubkeys aren't
adversarial input) to `foldhash`:

```rust
// yellowstone-grpc-geyser/src/plugin/filter/filter.rs:22
foldhash::{HashMap as FoldHashMap, HashSet as FoldHashSet},
```

```rust
// yellowstone-grpc-geyser/src/plugin/filter/filter.rs:317-322
nonempty_txn_signature_required: FoldHashSet<FilterName>,
account: FoldHashMap<Pubkey, FoldHashSet<FilterName>>,
account_required: FoldHashSet<FilterName>,
account_cuckoo: FoldHashMap<FilterName, Arc<CuckooFilter<[u8; 32]>>>,
owner: FoldHashMap<Pubkey, FoldHashSet<FilterName>>,
owner_required: FoldHashSet<FilterName>,
```

and the per-message match-set fields at `filter.rs:568-572`. This is a pure
hasher swap, not master's `72cf363` `FilterAccountAggregate`/reverse-index
algorithmic restructuring (explicitly not adopted — `i2` already has its own
`msg_type_mask` bitmask fast-path serving a similar purpose via a different
mechanism).

**Part B — removed a shared, cross-connection lock.** `GrpcService` used to
hold `filter_names: Arc<Mutex<FilterNames>>`, locked on every incoming
subscribe/filter-update message from *any* connection. Now each connection
constructs its own instance:

```rust
// yellowstone-grpc-geyser/src/grpc.rs:558-560 (GrpcService fields)
filter_name_size_limit: usize,
filter_names_size_limit: usize,
filter_names_cleanup_interval: Duration,
```

```rust
// yellowstone-grpc-geyser/src/grpc.rs:1648-1651 (per-connection, in the
// incoming-filter task spawned by Geyser::subscribe)
let mut filter_names = FilterNames::new(
    self.filter_name_size_limit,
    self.filter_names_size_limit,
    self.filter_names_cleanup_interval,
);
```

**Trade-off, stated explicitly (matches master's `72cf363` precedent):**
`FilterNames` is a cross-connection string-interning pool
(`plugin/filter/name.rs`, `is_uniq()`/`Arc::strong_count`-based) — the lock
being removed was only ever held on the per-connection subscribe path, never
the per-message hot path, so this is a memory trade-off (loses cross-client
name deduplication), not a latency fix in itself. Documented and tested
explicitly rather than left implicit.

Reference: master's `72cf363`.

---

## 4. Block reconstruction moved off the Processed fan-out hot path

**Status: retained. This is the change that actually closes most of the
remaining latency gap.**

### The problem this solves

Before this change, `geyser_dispatch` (and `geyser_loop`) did *all*
block-assembly bookkeeping — a `BTreeMap<u64, SlotMessages>` tracking gc,
account dedup by write_version, block sealing, missed-status ancestor
backfill — synchronously, inline, before every `Processed`-commitment
broadcast. That bookkeeping is irrelevant to the majority of low-latency
subscribers, who only care about raw `Processed` updates arriving as fast as
possible, yet it directly gated their delivery.

This was done as five separate, individually-validated tasks (4, 5, 6a, 6b,
6c) specifically because it's the highest-risk change in the port — see
"What the review process caught" below for the real bugs this staging
caught before they shipped.

### The resulting architecture

```rust
// yellowstone-grpc-geyser/src/grpc.rs:536
pub struct DispatchThreadHandles {
```

`GrpcService::create`'s CPU-pinned branch (`spawn_dispatch_threads`,
`grpc.rs:783`) now spawns **two** `std::thread`s connected by a channel:

1. **`geyser_dispatch`** (`grpc.rs:969`) — still CPU-pinned via
   `sched_setaffinity`, still a busy-spin `try_recv()` loop, but now does
   almost nothing per message:

   ```rust
   // yellowstone-grpc-geyser/src/grpc.rs:988-994
   let msgid = msgid_gen.next();
   let is_slot = matches!(&message, Message::Slot(_));

   if reconstruction_tx.send((msgid, message.clone())).is_err() {
       info!("Geyser dispatch: block-reconstruction channel closed");
       break;
   }

   processed_messages.push((msgid, message));
   ```

   It assigns an id, forwards the message to the reconstruction thread, and
   batches/encodes/broadcasts the **raw `Processed` message itself,
   directly** — no BTreeMap, no gc, no sealing. This is the actual latency
   win: raw `Processed` delivery no longer waits on any of that bookkeeping.

2. **`block_reconstruction_dispatch`** (`grpc.rs:1039`) — a new,
   deliberately **non**-CPU-pinned thread. It owns the entire
   `BlockReconstructionState` (the relocated `BTreeMap`, gc, dedup, sealing,
   missed-status backfill, and `from_slot` replay-buffer servicing — see
   below) and broadcasts `Confirmed`/`Finalized` unconditionally for every
   item, but on `Processed` broadcasts **only** the items it synthesizes
   itself (the sealed `Block` message, backfilled ancestor `Slot` messages)
   — never the raw pass-through message, since `geyser_dispatch` already
   broadcast that one directly:

   ```rust
   // yellowstone-grpc-geyser/src/block_reconstruction.rs:120-129
   pub is_raw_message: bool,
   /// True iff `message` is exactly the raw input passed to `on_message`/
   /// `on_message_with_id` for this call — as opposed to something
   /// `BlockReconstructionState` synthesized itself (the sealed `Block`
   /// message, or a backfilled ancestor `Slot` message). Distinguished by
   /// comparing the item's msgid against the input message's msgid: ids
   /// are minted from one monotonic, never-reused space, so this
   /// comparison is exact regardless of the item's position in the
   /// dispatch sequence.
   ```

Both threads mint ids from one **shared, atomic-backed** generator — required
because after this split, two independent code paths mint ids into the same
monotonic space that `client_loop`'s replay-path `sort_by_key` depends on:

```rust
// yellowstone-grpc-geyser/src/block_reconstruction.rs:36-44
pub(crate) struct MessageId {
    id: Arc<AtomicU64>,
}

impl MessageId {
    pub(crate) fn next(&self) -> u64 {
        let prev = self.id.fetch_add(1, Ordering::Relaxed);
        prev.checked_add(1).expect("message id overflow")
    }
}
```

`replay_stored_slots_rx`/`replay_first_available_slot` (backing the
production `from_slot` auto-reconnect feature) moved with the `BTreeMap` to
the reconstruction thread, matching master's `2146785` precedent — the
`GrpcService` struct's own `Arc<AtomicU64>` clone (read by the
`subscribe_first_available_slot` RPC handler) is unaffected, since it's a
separate clone of the same atomic, not the moved original.

The async `geyser_loop` fallback (used only when no CPU core is configured)
deliberately keeps its original single-threaded, inline bookkeeping —
splitting it the same way was scoped out as a non-goal.

### The live-ordering relaxation this introduces

Explicitly tested, not just asserted: the sealed `Block` message and
backfilled ancestor `Slot` messages can now arrive at a live `Processed`
subscriber arbitrarily late relative to raw `Processed` messages for *later*
slots, bounded only by the reconstruction thread's channel backlog. What does
**not** relax: raw pass-through messages stay strictly ordered (dispatch
remains their sole producer/broadcaster); per-pubkey write_version ordering
is unaffected (the raw broadcast push happens before any dedup-nulling, which
only ever mutates the stored/Confirmed/Finalized snapshot); `Confirmed`/
`Finalized` content is never wrong, only potentially late.

A `from_slot` replay request issued while the reconstruction thread is
backlogged now blocks on that thread's own backlog rather than the (now
much shorter) `geyser_dispatch` backlog — a stated, tested trade-off, not a
new failure mode: previously both were coupled to one thread's queue depth;
now raw `Processed` latency is decoupled and near-instant, while `from_slot`
freshness depends solely on the reconstruction thread's independent lag.

Reference: master's `f7087d3` (introduced `block_reconstruction.rs`/
`BlockMachineStorage`) and `2146785` (split the dispatch loop this way) —
adapted to this fork's synchronous, CPU-pinned thread model rather than
master's `tokio::runtime::Builder::new_current_thread()`-per-thread pattern.

---

## What was deliberately not ported

- Master's `yellowstone_block_machine`/`BlockMachineStorage` external crate —
  full rewrite of block assembly, independent of the threading change; this
  fork keeps its existing `BTreeMap<u64, SlotMessages>` shape, just relocated.
- Master's tokio-runtime-per-thread pattern for the reconstruction thread —
  a plain `std::thread` is used instead (`mpsc::Receiver::try_recv()`/`recv()`
  and `oneshot::Sender::send()` need no active runtime, proven by
  `geyser_dispatch` already doing exactly this).
- Splitting the async `geyser_loop` fallback path the same way.
- Master's `72cf363` `FilterAccountAggregate` reverse-index restructuring —
  only the hasher swap was adopted.
- Master's `Bytes`→`Vec<u8>`/`encoded_len` const-generic changes, bundled
  into the same master commit as the encoder change but unrelated to it.
- CPU-pinning the new reconstruction thread.
- Making the new dispatch→reconstruction channel bounded (deliberately
  unbounded, mirroring the existing plugin→dispatch channel's accepted
  trade-off — this is the mechanism that makes the decoupling real:
  reconstruction-thread lag can never stall dispatch's send).
- A reconstruction-channel-depth monitoring gauge — flagged as a natural
  follow-up, not scoped into this work.

---

## What the review process caught

Every task went through an independent executor → validator cycle (the
`/implement` harness); several real issues were caught before landing:

- **Planning round 1 → rejected**: the thread-split task was under-decomposed
  and never explained what happens to `from_slot` replay when the `BTreeMap`
  moves threads.
- **Planning round 2 → rejected**: still didn't address `replay_stored_slots_rx`/
  `replay_first_available_slot` ownership — closed in round 3, verified
  against master's own `2146785` having solved the identical problem.
- **Task 5 (extraction refactor), attempt 1**: introduced an extra
  per-message heap allocation (`Vec<DispatchItem>` collected eagerly) that
  didn't exist in the original inlined code — fixed by returning an iterator
  instead.
- **Task 5, attempt 2**: the iterator fix deferred a real `BTreeMap` mutation
  (`confirmed_at`/`finalized_at`) into its lazy `.map()` closure, making
  correctness depend on the caller fully draining the iterator — "safe by
  convention, not by construction," and exactly the kind of thing the
  upcoming thread-split (Task 6a/6b) could silently violate. Fixed by
  splitting into an eager mutation pass
  (`block_reconstruction.rs:453`, `record_slot_status_transitions`) and a
  provably read-only lazy stage (`dispatch_item_for` now takes `&self`,
  enforced by the borrow checker).
- **Task 6b (the decoupling)**: the plan's own spec for the decoupling-proof
  test predicted a "lower msgid" for a late-delivered `Block` message;
  empirically it's *higher*. Independently re-derived and confirmed as a
  deterministic, structural consequence of the design (a shared monotonic
  counter with lazy derived-id-minting cannot simultaneously guarantee
  "arrives late" and "always has a lower id") rather than a masked bug.

Full detail on every round and task, including validator agent findings, is
in `implement/port-master-latency-opts`'s `.claude/state/run_log.md`.

---

## Commit references (on `implement/port-master-latency-opts`, merged to `i2`)

| Task | What | Commit |
|---|---|---|
| 1 | Remove `ParallelEncoder`, direct synchronous `encode_messages()` | `7b7867b` |
| 2 | jemalloc as global allocator | `17d7290` (**reverted**, see `problem.md`) |
| 3 | Filter foldhash + per-connection `FilterNames` | `4700fbd` |
| 4 | Characterization tests (regression net for 5/6) | `4c0cf7f` |
| 5 | Extract block-reconstruction bookkeeping | `67e4e11` |
| 6a | Spawn reconstruction thread, relocate BTreeMap/replay ownership | `17c73fa` |
| 6b | The decoupling — raw Processed broadcast directly from dispatch | `9d34d48` |
| 6c | Shutdown/join wiring for the reconstruction thread | `b4e804b` |
