# Optimization Candidates

**Source of truth for the `/project:optimize` harness.**  
The harness reads this file at runtime. Do not add free-form notes — use the structured format below.  
Update `status` and `experiment_ref` after each run.

Format per candidate:
```
### <ID>: <title> [<status>]
tier: <1|2|3>
experiment_ref: <timestamp in experiments.jsonl, or blank>
```

Status values: `OPEN` | `TESTING` | `PASS` | `FAIL` | `BLOCKED` | `RULED_OUT`

---

## Tier 1 — High Impact (>100ms expected win at 500+ subs)

### C2: filter.get_updates() message-type fast-path [PASS]
tier: 1
experiment_ref: 2026-06-15T10:37:56Z

**Bottleneck:** In `client_loop` (grpc.rs:1637), `filter.get_updates(message, commitment)` is called
for EVERY message for EVERY subscriber. At 50k msg/s × 500 subscribers = 25M evaluations/sec.
Most are wasted: a transaction-only subscriber's filter evaluates (and discards) every Account
and Slot message. The filter walks pubkey sets, owner sets, data-slice ranges, CuckooFilter lookups
before returning empty.

**Fix:** Add a `msg_type_mask: u8` field to `Filter` (computed once at filter construction from the
`SubscribeRequest` fields). Add `Filter::can_match_message(&Message) -> bool` that does a single
bitwise AND. Call it in client_loop BEFORE calling `get_updates`. Skip `get_updates` entirely
if the mask says no match possible.

**Key files:**
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs` — add mask field + method
- `yellowstone-grpc-geyser/src/grpc.rs:1637` — add fast-path check in client_loop hot loop

**Expected win:** 70-90% skip rate on typical workloads (txn subs receiving account updates, etc).
At 500 subs this saves ~1-4 CPU cores of filter work → drains the broadcast queue faster → lower p99.

**Constraints:** Do NOT change `get_updates` internals. Do NOT add any HashMap or allocation to the fast path.

---

### C3: Replace tokio::broadcast with lock-free per-subscriber queue [PASS]
tier: 1
experiment_ref: 2026-06-15T12:00:00Z

**Bottleneck:** `tokio::broadcast::send()` (grpc.rs:1023, 1033, 1038) acquires an internal Mutex,
writes to a ring buffer, then calls `wake()` on ALL N subscriber Wakers sequentially.
At 700 subscribers this is O(N) Mutex-hold time + O(N) Waker invocations on EVERY message batch.
The Mutex serializes the dispatch thread against subscriber task scheduling.

**Fix:** Replace the single broadcast channel with a per-subscriber `crossbeam_queue::ArrayQueue`
(bounded, lock-free SPSC). Each subscriber owns one queue. The dispatcher pushes to all N queues
without holding any lock (each push is a CAS on that subscriber's queue). Wake subscribers via
stored `Arc<tokio::sync::Notify>` — one `notify_one()` per subscriber after push.

**Key files:**
- `yellowstone-grpc-geyser/src/grpc.rs` — replace `broadcast_tx/rx` with `SubscriberQueue` type,
  update `geyser_dispatch`, `client_loop`, subscription setup and teardown
- `Cargo.toml` — add `crossbeam-queue = "0.3"`

**Expected win:** O(1) per-subscriber push cost (CAS on their own queue) instead of a shared Mutex
held for the entire send. At 700 subs × 50k msg/s the serialized Mutex hold time is the dominant
dispatch overhead. Expected 200-500ms p99 reduction at 700 subs.

**Constraints:** Must maintain ordering guarantees. The `Arc<Vec<(u64, Message)>>` shared payload
structure can stay (cheap Arc::clone per subscriber). Handle subscriber registration/deregistration
safely (use `Arc<Mutex<Vec<SubscriberHandle>>>` for the subscriber list, updated rarely).
Bounded queue: if full, the subscriber is considered "lagged" (same semantics as broadcast `Lagged`).

---

### C4: Inline encode + direct write for single-filter subscribers [PASS]
tier: 1
experiment_ref: 2026-06-15T12:00:00Z

**Bottleneck:** The path from `geyser_dispatch` to the TCP socket has 4 async hops:
`broadcast` → `client_loop` tokio task wake → `stream_tx.try_send` → tonic codec encodes and writes.
Each hop adds scheduling jitter. For a single high-priority subscriber (e.g. a latency-critical client),
eliminating the async hops saves 50-200µs per message.

**Fix:** Add a "fast subscriber" mode (config opt-in). For subscribers marked fast:
- Register a direct `tokio::io::AsyncWrite` handle during subscription setup
- In `geyser_dispatch`, after building the batch, encode + write to fast-subscriber handles INLINE
  (before broadcasting to the rest)
- Use a `tokio::sync::mpsc::channel(1)` to hand off frames to a dedicated I/O task per fast-subscriber
  (keeps the dispatch thread from blocking on socket writes)

**Key files:**
- `yellowstone-grpc-geyser/src/grpc.rs` — add FastSubscriberSet, call inline encode path in dispatch
- `yellowstone-grpc-geyser/src/config.rs` — add `fast_subscriber_ids: Vec<String>` config field
- `yellowstone-grpc-geyser/src/plugin/filter/message.rs` — expose FilteredUpdate encoding

**Expected win:** Removes scheduler jitter for latency-priority subscribers. From our race bench,
the gap vs Triton when we receive tx first is ~6ms — this path aims to cut that to <1ms.

**Constraints:** Must not block geyser_dispatch on I/O. The inline path is only for subscribers
whose filter is known statically (set at connection time, never changes). Fall back to normal
broadcast path for all others. Do not touch ParallelEncoder.

---

## Tier 2 — Medium Impact (10-100ms potential)

### C5: Remove `encoded_len()` from hot path [PASS]
tier: 2
experiment_ref: 2026-06-15T12:01:00Z

**Bottleneck:** grpc.rs:1638 — `let proto_size = message.encoded_len()` is called for every
FilteredUpdate sent to every subscriber. `encoded_len()` traverses the entire proto tree
(same computational work as serialization, minus byte output). At 50k msg/s × 500 subs = 25M
traversals/sec, consuming 1-2 CPU cores purely for a bytes-sent metric.

**Fix:** Remove `message.encoded_len()` and `session.metrics.incr_bytes_sent(proto_size)`.
The MeteredLayer transport already tracks outbound bytes at the tonic level (per metrics.rs:613).
Per-subscriber byte count becomes approximate but the correctness of message delivery is unaffected.

**Key files:** `yellowstone-grpc-geyser/src/grpc.rs:1638-1640` — remove two lines.

**Expected win:** Frees 1-2 cores of pure metrics overhead at 500+ subs.

---

### C6: SmallVec for FilteredUpdates [RULED_OUT]
tier: 2
experiment_ref: 2026-06-15T12:02:00Z

**Bottleneck:** `FilteredUpdates` is `Vec<FilteredUpdate>` (plugin/filter/message.rs).
`filter.get_updates()` creates a new empty Vec on every call and typically pushes 0 or 1 items.
At 25M calls/sec, this is 25M heap allocations/sec — each alloc takes ~20-30ns + GC pressure.

**Fix:** Change `type FilteredUpdates = Vec<FilteredUpdate>` to
`type FilteredUpdates = smallvec::SmallVec<[FilteredUpdate; 1]>`.
Add `smallvec = { version = "1", features = ["union"] }` to `Cargo.toml`.
Update all construction sites — `Vec::new()` → `SmallVec::new()`, `vec![]` → `smallvec![]`.

**Key files:**
- `yellowstone-grpc-geyser/Cargo.toml` — add smallvec dep
- `yellowstone-grpc-geyser/src/plugin/filter/message.rs` — change type alias + constructors
- Any file using `FilteredUpdates::new()` or `vec![]` in that context

**Expected win:** Eliminates heap allocation for 0- and 1-item results (the overwhelmingly common case).
25M alloc/sec × 20ns = 500ms CPU/sec saved across subscriber cores.

---

### C7: Skip broadcast for message types with no active subscribers [OPEN]
tier: 2
experiment_ref:

**Bottleneck:** `geyser_dispatch` calls `broadcast_tx.send(...)` for every message batch regardless
of whether any subscriber actually has a filter that could match. If all 700 subscribers are
transaction-only and an Account message arrives, the dispatcher still broadcasts it, waking all 700
tasks for them to call `filter.get_updates()` and return empty.

**Fix:** Track an `Arc<AtomicU8>` bitmask of active subscription types in `GrpcService`.
When a subscriber's filter is set or updated, update the bitmask (increment per-type counter,
recompute bitmask). In `geyser_dispatch`, before `broadcast_tx.send(msg)`, check if
`active_mask & message_type_bit == 0`. If so, skip the send entirely.

**Key files:**
- `yellowstone-grpc-geyser/src/grpc.rs` — add `ActiveSubscriptionMask`, update on filter change,
  check before broadcast in geyser_dispatch and geyser_loop
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs` — expose a `msg_type_mask()` method
  (can reuse the C2 mask if C2 is implemented first)

**Expected win:** If typical prod workload is 80% txn-only subscribers and 20% of messages are
account updates, this skips 20% × 700 subscriber wakeups = 140 wakeups × 150ns = 21µs saved
per such message. At high message rates this compounds significantly.

**Dependency note:** C7 benefits from C2 being implemented first (can reuse the mask concept).
Implement C2 before C7 for cleaner code.

---

### C8: Tokio worker thread count + CPU affinity tuning [OPEN]
tier: 2
experiment_ref:

**Bottleneck:** With 700 subscriber tasks, default tokio worker thread count (= num_cpus) may create
contention between subscriber tasks and geyser_dispatch's rayon encoder threads.

**Fix:** Config option `tokio_worker_threads: usize`. Set explicitly. Pin tokio workers to CPU cores
that DON'T overlap with geyser_dispatch CPU or encoder rayon pool CPUs.

**Key files:** `yellowstone-grpc-geyser/src/config.rs`, `yellowstone-grpc-geyser/src/lib.rs`

**Expected win:** Reduced scheduler interference. Marginal but measurable at 700 subs.

---

## Tier 3 — Micro-optimizations (<10ms)

### C9: Stack-allocate messages_vec in dispatch loop [OPEN]
tier: 3
experiment_ref:

`Vec::with_capacity(4)` at grpc.rs:942 and grpc.rs:1280 allocates per message in the dispatch loop.
Use `arrayvec::ArrayVec<(u64, Message), 4>` instead.

### C10: Dedup set_queue_size when unchanged [OPEN]
tier: 3
experiment_ref:

`session.metrics.set_queue_size(stream_tx.queue_size())` at grpc.rs:1516 fires every select iteration.
Add `last_queue_size: u64` to avoid the atomic store when value hasn't changed.

---

## Ruled out

### C6: SmallVec for FilteredUpdates [RULED_OUT]
**Reason (2026-06-15):** Already implemented in the codebase — `FilteredUpdates` is `SmallVec<[FilteredUpdate; 2]>` in plugin/filter/message.rs, `smallvec` is a workspace dep. No change needed.

### C1: Shard broadcast by commitment level [RULED_OUT]
**Reason (2026-06-15):** ~90% of messages are `CommitmentLevel::Processed`. Sharding into 3 channels
would give at most ~3% wake reduction in practice (only confirmed/finalized subs benefit).
The code complexity outweighs the marginal gain.
