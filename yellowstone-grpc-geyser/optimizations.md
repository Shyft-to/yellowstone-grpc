# Streaming latency vs. subscriber count — analysis & optimization targets

## Symptom

The geyser plugin gets slower and streaming latency increases as the number of
subscribers grows, and it degrades over time.

## Verdict

This is evident in the code. It is not a single bug but **several compounding
`O(number_of_subscribers)` costs that all sit on the critical path of every
message**.

> **Update (2026-05-27): the verdict above is the original *a-priori hypothesis*,
> and measurement has since falsified most of it.** None of the optimizations
> built from it produced an observable end-to-end latency win, and the headline
> factor (#1, the `broadcast` wake-all) turned out **not** to be the dominant
> cost. See **[Results & post-mortem](#results--post-mortem)** at the end of this
> document for every attempt and why each failed. The factor analysis below is
> retained as the original investigation record.

## Architecture (relevant part)

`geyser_loop` (`src/grpc.rs:741`) is a **single task** that ingests all geyser
messages, pre-encodes each batch **once** (`parallel_encoder.encode`,
`src/grpc.rs:999`), and fans them out through one `tokio::sync::broadcast`
channel (`src/grpc.rs:605`; sends at `:1001` / `:1010` / `:1015`). Each
subscriber gets its own `client_loop` task (spawned at `src/grpc.rs:1669`) that
receives from `broadcast_tx.subscribe()` (`:1676`) and independently filters
every message via `session.filter.get_updates(...)` (`src/grpc.rs:1281`).

The pre-encode-once design is good and deliberately avoids per-subscriber
encoding. But everything *downstream* of it scales with subscriber count.

## Why it slows down as subscribers grow

### 1. `tokio::sync::broadcast` wakes *every* receiver on *every* send — O(N) on the producer's critical path
This is the dominant factor. tokio's broadcast channel, on each `send`, locks
its internal tail mutex and drains/wakes the entire waiter list. With N parked
`client_loop` receivers, each of the three sends per message batch
(Processed/Confirmed/Finalized, `src/grpc.rs:999-1015`) wakes all N tasks under
that lock. The single `geyser_loop` therefore spends time linear in subscriber
count *per message*. As N rises, send latency rises, messages back up in the
unbounded `messages_rx`, and end-to-end streaming latency grows.

### 2. All three commitment levels are broadcast to all clients — ~3× wake amplification
There is one broadcast channel carrying Processed + Confirmed + Finalized. A
client subscribed at `processed` is still woken for confirmed/finalized batches
and only then discards them via the cheap
`commitment == session.filter.get_commitment_level()` check
(`src/grpc.rs:1279`). The filtering is skipped, but the per-task wakeup/recv
cost (factor #1) is paid regardless.

### 3. Per-subscriber filtering competes for a fixed 8-thread runtime
`get_updates` (`src/plugin/filter/filter.rs:236`) — match + build
`FilteredUpdate` + `encoded_len()` (`src/grpc.rs:1282`) — runs once per
subscriber per message. Total filtering CPU is O(N × message_rate), but
`config.json` pins `worker_threads: 8` with core affinity. Once N × rate exceeds
what 8 threads sustain, per-client mpsc queues (capacity 100_000) fill,
`queue_size` climbs, latency rises, and eventually clients are dropped with
`"client_channel_full"` (`src/grpc.rs:1288-1294`). Note: `encoded_len()` is cheap
for pre-encoded accounts but is **recomputed fully per subscriber** whenever a
`data_slice` is set (`src/plugin/filter/message.rs:531-537`), since data-slicing
bypasses the shared pre-encoded bytes.

### 4. Prometheus `with_label_values` runs twice per message per subscriber
For every message sent, the loop calls
`incr_grpc_message_sent_counter(&subscriber_id)` and
`incr_grpc_bytes_sent(&subscriber_id, …)` (`src/grpc.rs:1285-1286`,
`:1239-1240`), each doing `with_label_values(&[subscriber_id])`
(`src/metrics.rs:473-483`) — a label hash plus a read-lock on a shared
`IntCounterVec` map. Plus `set_subscriber_queue_size` every loop iteration
(`src/grpc.rs:1160`). At high message rates × many subscribers this is
significant per-message CPU and shared-map lock traffic.

### 5. The "over time" part: unbounded per-subscriber metric cardinality
The per-`subscriber_id` metrics — `GRPC_MESSAGE_SENT`, `GRPC_BYTES_SENT`,
`GRPC_SUBSCRIBER_QUEUE_SIZE`, `GRPC_SUBSCRIBER_SEND_BANDWIDTH_LOAD`,
`GRPC_CLIENT_DISCONNECTS` — are **never removed**. On disconnect,
`ClientSession::drop` only sets queue size to 0 (`src/grpc.rs:483`); it does not
call `remove_label_values`. Only the per-IP traffic and per-TCP-connection
metrics are actually removed (`src/metrics.rs:618`, `:631-634`). So as distinct
subscribers churn through over time, these label maps grow without bound —
increasing memory, lookup cost on the hot path (#4), and `/metrics` scrape cost.
This is the mechanism by which the plugin degrades *over time*, not just *with
concurrent N*.

## Summary

| Factor | Cost | On critical path? |
|---|---|---|
| broadcast wake-all per send | O(N) per message, single producer task | Yes — serializes all output |
| 3 commitment levels to all clients | ~3× wakeups | Yes |
| per-subscriber filtering | O(N), capped by 8 worker threads | Yes |
| `with_label_values` ×2 per msg/sub | per-message hashing + shared lock | Yes |
| per-subscriber metric label leak | grows unbounded over time | Yes (degrades #4 + scrapes) |

The single-producer broadcast (#1) is the architectural ceiling; the
metric-label leak (#5) best explains the "gets worse over time" symptom
specifically.

## Optimization directions

- **Shard the fan-out.** Replace the single broadcast with multiple broadcast
  channels / dedicated sender tasks, or group subscribers by commitment so each
  receiver only gets its own commitment level (kills factors #1 and #2).
- **Cache per-subscriber metric handles.** Resolve the `IntCounter`/`IntGauge`
  once per `ClientSession` instead of calling `with_label_values` per message
  (factor #4).
- **Clean up metrics on disconnect.** Call `remove_label_values` for all
  per-`subscriber_id` metrics in `ClientSession::drop` (factor #5).
- **Reconsider the fixed thread pool** sizing relative to expected
  subscriber × message-rate load (factor #3).

## Implementation

The changes land in three layers so the optimizations can be A/B tested against
a baseline: **(1) latency profiling**, **(2) optimization 1**, **(3)
optimization 2**, each a separate commit. Layer 1 is wired into the *unoptimized*
code so the current latencies can be recorded first; layers 2 and 3 are then
measured the same way and compared.

### Layer 1 — hot-path latency profiling (`src/latency.rs`)

Lock-free atomic histograms record four hot-path measure points; a background
reporter emits one JSON line per metric every `latency_metrics_interval_seconds`
to the `yellowstone_latency` log target. Recording short-circuits on a single
atomic load when disabled (`latency_metrics_interval_seconds = 0`), so it is safe
to leave compiled into both the unoptimized and optimized builds.

| metric | unit | where | meaning |
|---|---|---|---|
| `end_to_end` | microseconds | client loop | geyser `created_at` → just before client send |
| `producer_send` | microseconds | geyser loop | time in one broadcast send (the wake-all cost, factor #1) |
| `filter_encode` | microseconds | client loop | `Filter::get_updates` per message (factor #3) |
| `client_queue_depth` | items | client loop | outbound queue depth (per-client backpressure) |

Log line schema (NDJSON — one object per metric per window):

```json
{"ts_ms":1716480000000,"metric":"end_to_end","unit":"microseconds","window_secs":10.0,"count":12345,"p50":120,"p90":480,"p99":2000,"max":15000,"mean":210.50}
```

Capture for offline rendering:

```sh
grep yellowstone_latency plugin.log | sed 's/.*yellowstone_latency[^{]*//' > latency.ndjson
```

The renderer reads `ts_ms` as the x-axis and plots `p50`/`p90`/`p99`/`max` per
`metric`. Run the **unoptimized** build first to capture the baseline, then the
optimized build, and compare the same metrics.

Config:

```jsonc
"grpc": {
  "latency_metrics_interval_seconds": 10  // 0 disables latency logging
}
```

Tests (`src/latency.rs`): histogram percentile correctness, max-capping,
overflow, reset-between-windows, enabled/disabled gating, future-timestamp
clamping, JSON shape.

### Layer 2 — optimization 1: cached metric handles + bounded cardinality (factors #4 + #5)

`metrics::SubscriberMetrics` resolves the `IntCounter`/`IntGauge` handles once
per session instead of calling `with_label_values` (label hash + shared-map
read-lock) on every message and on every loop iteration. On the last session for
a `subscriber_id`, the per-subscriber series are removed via
`remove_label_values`, bounding metric cardinality across subscriber churn (the
"degrades over time" mechanism, factor #5). A refcount handles ids shared across
concurrent connections; handles for identical labels point at the same
underlying metric, so increments are byte-for-byte what the free helpers
produced — pure win, no semantic change.

Tradeoff: removing a counter series on last-disconnect resets it if the
subscriber reconnects (`rate()` tolerates this); that is the accepted cost of
bounding cardinality.

Tests (`src/metrics.rs`): cached increments match the old free helpers,
last-session drop removes the series, series survives until the last shared
session drops.

### Layer 3 — optimization 2: relay sharded fan-out (factors #1 + #2)

The single `broadcast` channel is replaced with one channel per commitment
(`BroadcastProducer` / `BroadcastSubscriber`). The hot `processed` commitment is
further split: the producer sends once to a *source* channel that wakes only
`processed_fanout_shards` relay tasks (O(shards) on the producer's critical
path); each relay re-broadcasts to its slice of subscribers on its own task, so
the O(N) wake-all is parallelized across cores instead of serialized on
`geyser_loop`. `confirmed`/`finalized` stay single channels (sparse
populations). Each client subscribes only to its commitment's channel and
resubscribes if it switches commitment, so the old per-message
`commitment == filter.commitment` discard (factor #2) is gone.

Because ~90% of subscribers sit on `processed`, splitting by commitment alone
would cut wakeups only ~10%; the relay shards are what actually parallelize the
dominant `processed` fan-out and keep producer-side latency flat as concurrent
subscribers grow.

Config:

```jsonc
"grpc": {
  "processed_fanout_shards": 4  // 1 = a single transparent relay
}
```

Semantics notes:
- A commitment switch resubscribes at the new channel's tail — identical to
  before for a client that is keeping up; the only divergence is a client
  simultaneously *lagging and switching commitment* (it skips that channel's
  backlog), a bounded, rare edge.
- Under extreme runtime starvation a relay could lag the source and skip a few
  messages for its shard; this is surfaced by the
  `yellowstone_grpc_fanout_relay_lagged_total{shard}` counter. In normal
  operation relays only do an `Arc` bump + send and never fall behind.

Tests (`src/grpc.rs`): fan-out delivers every commitment's batches to all of its
subscribers in order with no cross-commitment leakage (3 shards), single-shard
ordering, and the existing half-close cancellation regression test (ported to
the fan-out API).

### How to compare

1. Build and run the **layer-1** commit (unoptimized + profiling); capture the
   `yellowstone_latency` lines under representative subscriber load.
2. Build and run the **layer-3** tip (both optimizations + profiling) under the
   same load; capture again.
3. Compare `end_to_end` and `producer_send` percentiles (and
   `client_queue_depth`) between the two runs. Optionally check the **layer-2**
   commit in isolation to attribute the gain between the two optimizations.

---

## Results & post-mortem

*What we actually tried, and why each failed — updated 2026-05-27.*

The hypothesis at the top of this document drove the optimizations below.
**Measurement falsified most of it.** None of the attempts produced an observable
end-to-end latency improvement; opt2 made latency *worse* at low-to-moderate
subscriber counts (tested up to ~700). The symptom we were chasing throughout:
the per-client outbound queue depth **creeps up and does not drain fast** as
subscriber count grows.

### Every attempt at a glance

| Attempt | What it changed | Outcome | Why it failed |
|---|---|---|---|
| **opt1** — cached metric handles + bounded cardinality (factors #4, #5) | Resolve `IntCounter`/`IntGauge` handles once per session instead of `with_label_values` per message; `remove_label_values` on last disconnect. | No measurable latency change. | The `with_label_values` overhead was never on the latency-critical path. Legitimate **hygiene** (it does fix the unbounded per-subscriber metric-cardinality leak, factor #5), but not a latency fix. |
| **opt2** — relay sharded fan-out (factors #1, #2) | Replace the single `broadcast` with per-commitment channels; shard the hot `processed` wake-all across `processed_fanout_shards` relay tasks. | **Worse** at ≤700 subscribers; no win at any tested N. | (1) Adds an async hop `producer → relay → subscriber` = pure added latency where there is no O(N) wake to amortize. (2) The O(N) broadcast wake it targets is **not** the dominant cost — this directly **falsifies factor #1**. (3) It *relocates* fan-out work to relay tasks rather than reducing it; total wakeups are unchanged. (4) Unblocking the producer pushes more downstream, so per-client queues fill *faster*. |
| **Lever 1** — encode-once / shared-body fan-out | Pre-encode the whole `FilteredUpdateOneof` body once per message, then per-subscriber only frame filter-names + memcpy the shared body. | Ruled out by microbenchmark **before** touching the system. | The existing `pre_encoded` cache (`message.rs:496`) already dedups the body for the dominant warm account/tx path (no `data_slice`). The residual per-subscriber cost is the **body memcpy into each connection's own buffer**, which is unavoidable per-connection and identical with or without lever 1. Bench (`benches/encode.rs::bench_fanout`, warm cache, 400 subs): 165 B acct 1.58×, but 0 B acct and txn slightly *slower*, 2 MB acct 0.98× (no change) — i.e. tens-of-nanoseconds noise either way. |
| **Lever 3** — HTTP/2 flow-control windows | Enlarge `server_initial_stream_window_size` / `server_initial_connection_window_size`; enable `server_http2_adaptive_window` (config-only, no code). | No improvement. | The per-client queue drain is not gated by the HTTP/2 window size in this setup → flow-control is not the wall. |

(A separate, non-optimization diagnostic — relocating the `end_to_end` measure
point from the client loop to the outbound-queue *dequeue* — confirmed that the
outbound-queue **wait** is a real leg that the original `end_to_end` did not
capture.)

### What is now eliminated as the bottleneck

- **Metric overhead** — opt1 was negligible for latency.
- **The `broadcast` wake-all (factor #1, the original "dominant" factor)** —
  opt2 sharded exactly this and it did not help. Falsified.
- **Per-subscriber serialization** — lever-1 bench shows ~90 ns/sub for normal
  messages; the only material residual is unavoidable per-connection memcpy.
- **HTTP/2 flow-control window size** — lever-3 config A/B showed no change.

### What the data does show

- The encode/serialize step is cheap (~tens-to-~100 ns per subscriber for normal
  messages). The unavoidable per-connection **body memcpy** dominates for large
  payloads: a 2 MB account fanned to 400 subscribers ≈ **800 MB copied per
  message**.
- The per-client outbound queue capacity is **250,000** (`channel_capacity`,
  reused for the outbound channel at `grpc.rs:1567`). A slow-draining client
  therefore buffers up to 250k messages before the `client_channel_full`
  disconnect — the "queue creeps up" symptom is this buffer filling.

### Still open — next probe (not yet done)

Two hypotheses remain, and they are distinguished by a single observation —
**per-core CPU under high-N load**, which has not yet been captured:

1. **CPU/scheduler-bound consumer fan-out** — N `client_loop` task wakeups +
   `get_updates` (filter match) + memcpy per message saturate the worker pool
   (cores pegged ~100%). → pursue reducing per-subscriber work / a shared
   refcounted-`Bytes` fan-out (the only thing that removes the N× memcpy, and
   only when subscribers share identical filter output).
2. **Serial `geyser_loop` ceiling** — the single ingest/encode/broadcast task
   cannot keep up (cores have idle headroom while queues grow). → pursue the
   producer side (e.g. `message_queue_size` growth, `client_broadcast_lag`).

Resolve with a flamegraph + `mpstat -P ALL 1` during a high-N run **before**
attempting any further optimization.

### Lesson

opt1, opt2, lever-1, and lever-3 all targeted a leg of the pipeline that was
not the actual bottleneck. The recurring mistake was **building the fix before
measuring the leg**. The encode bench (which killed lever 1 in minutes, with no
system change) and the lever-3 config A/B (zero code) are the model to follow:
isolate and measure the suspected leg cheaply, and only implement once the data
confirms it.
