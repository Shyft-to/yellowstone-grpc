# Session State

Last updated: 2026-06-15

## Current branch
`production-optimization`

## What we know from this session

### Internal geyser bench (readings.md) summary
- 100/200 subs: p50≈1-2ms, p99≈12ms. CPU pinning makes no difference here — not subscriber-limited.
- 500 subs: unoptimized diverges (p50 grows to 33s). Optimized holds at p50=2-4ms.
- 700 subs: unoptimized diverges (p50→60s). Optimized holds at p50=4-8ms mostly, some spikes.
- **CPU pinning solved the scalability problem.**

### Real-world race bench (readings2.md) summary
- Triton wins ~90-93% at p50=0ms (receives tx from blockchain faster — network position)
- New_all_metrics: ~8% win rate at p50=6.5ms when we win
- New_nil_metrics: ~8% win rate — identical to all_metrics → **metrics are NOT the bottleneck**
- The 6ms when we win is the Triton latency when they receive tx after us, not our overhead

### Key insight from readings2.md
Disabling all metrics (nil_metrics) gives identical win rates vs all_metrics.
This confirms: the CPU time spent on metrics is NOT limiting factor in the current setup.
The remaining gap is either network position OR architectural overhead in the message path.

## Next candidates to try
See `.claude/context/candidates.md` — Tier 1 items C1, C2, C3 are the highest leverage.

## Pending questions
1. How does `tokio::broadcast` scale with N? What's the actual wake cost at 700 subs?
2. What's the typical message type distribution? (txn vs account vs slot)
3. Is there CPU contention between tokio worker threads and geyser_dispatch thread?
   (check if they share a CPU core despite pinning)
