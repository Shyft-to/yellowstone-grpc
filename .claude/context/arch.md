# Architecture Reference

## Message flow (grpc.rs)

### geyser_dispatch (grpc.rs:1126)
- `std::thread`, pinned to dedicated CPU core (config: `geyser_dispatch_cpu`)
- Spin-loop: `messages_rx.try_recv()` → process → `parallel_encoder.encode_blocking()` → `broadcast_tx.send()`
- Flush condition: inbox-empty OR `processed_messages.len() >= 31` OR `Message::Slot` arrives
- **No 10ms sleep** — immediately flushes on empty inbox (key win vs async `geyser_loop`)

### broadcast channel
- `tokio::broadcast::Sender<(CommitmentLevel, Arc<Vec<(u64, Message)>>)>`
- Capacity: default tokio broadcast (ring buffer size from `config`)
- Each `send()` wakes **all N subscriber tasks** — O(N) waker invocations
- Messages tagged with CommitmentLevel; each client filters to its level at line 1635

### client_loop (grpc.rs:1456)
- One tokio task per subscriber
- `messages_rx.recv()` → if `commitment == session.filter.get_commitment_level()` → `filter.get_updates(msg)` → `stream_tx.try_send(FilteredUpdate)`
- `session.metrics.set_queue_size()` called once per select iteration (not per message — OK)
- `message.encoded_len()` called per sent message for byte metrics (traverses proto tree)

### ParallelEncoder (parallel.rs)
- For batch < 4: runs `encode_message()` inline on calling thread
- For batch >= 4: `pool.install(par_iter_mut)` on rayon pool (blocks until done)
- `encode_message` only does work if `pre_encoded.get().is_none()` (OnceLock idempotent)
- In steady-state, pre_encoded is already set → effectively a no-op

### SubscriberMetrics (metrics.rs:484)
- Pre-resolved at subscription setup: `GRPC_MESSAGE_SENT.with_label_values(...)` → `IntCounter`
- Hot path: `self.grpc_message_sent.inc()` — lock-free atomic increment, no hash lookup

## Ruled-out / tested optimizations

| Lever | Result |
|-------|--------|
| Metric handle caching (opt1) | Already done (SubscriberMetrics). Was NOT the bottleneck per bench. |
| Relay sharded fan-out (opt2) | Worsened low-sub latency (extra async hop). Reverted. |
| Lever 1: encode FilteredUpdate body once + per-sub frame | bench shows ~0 gain; `pre_encoded` already covers it |
| HTTP/2 flow-control window increase | No improvement. Not the wall. |
| Adaptive batching | Tried; reverted (see git log). Didn't help. |
| Null-metrics A/B | readings2.md shows same win rate → metrics NOT bottleneck |
| CPU pinning (geyser_dispatch) | HUGE win at 500-700 subs (p50 queue: 33s → 4ms). KEPT. |

## Data
- readings.md: internal geyser bench JSON (us units, latency in µs)
  - 100 subs: p50≈1ms, p99≈12ms (unoptimized ≈ optimized)
  - 500 subs: optimized p50=2-4ms; unoptimized p50=33s (diverging queue)
  - 700 subs: optimized p50=4ms stable; unoptimized p50→60s
- readings2.md: real-world Triton vs New race (10k txns, 15 runs)
  - New wins ~7-10% of txns at p50=6ms gap (when we win, Triton is 6ms behind)
  - Triton wins ~90-93% at p50=0ms (they receive tx from geyser first — network advantage)
  - Conclusion: 6ms difference is likely network, not our processing overhead
