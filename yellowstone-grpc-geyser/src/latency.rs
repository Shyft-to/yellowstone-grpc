//! End-to-end fan-out latency profiling.
//!
//! Measures the time between the geyser callback stamping a message's
//! `created_at` (in `plugin/entry.rs`, every `Message*::from_geyser`) and the
//! moment tonic pulls the message off the per-client outbound queue — i.e.
//! the full in-plugin path **including** the outbound-queue wait. The
//! recording site is the [`EndToEndTimedStream`] wrapper around the per-client
//! `LoadAwareReceiver` returned to tonic; that's the last point inside the
//! plugin before tonic encodes and writes to the socket.
//!
//! When disabled (`e2e_latency_interval_seconds = 0`, the default), every
//! `record_*` call short-circuits on a single `Relaxed` atomic load, so the
//! wrapper is safe to leave compiled in for all builds and both feature
//! variants (`no-metrics` on or off — recording is independent of the
//! prometheus layer).

use {
    crate::{plugin::filter::message::FilteredUpdate, util::stream::LoadAwareReceiver},
    futures::Stream,
    log::info,
    prost_types::Timestamp,
    std::{
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::time::{interval as tokio_interval, MissedTickBehavior},
    tokio_util::{sync::CancellationToken, task::TaskTracker},
    tonic::Result as TonicResult,
};

/// 32 log₂-spaced buckets covering `[1µs, 2³¹µs)` ≈ 1 µs … ~36 min. Anything
/// larger lands in the final bucket. Worst-case resolution is ≈ 50 % relative;
/// fine for comparing distributions across A/B runs and locating which leg of
/// the pipeline contributes to the tail.
const NUM_BUCKETS: usize = 32;

/// Log target the periodic reporter writes its NDJSON lines to; pair with
/// `grep yellowstone_e2e <log>` to extract them.
pub const LATENCY_LOG_TARGET: &str = "yellowstone_e2e";

#[derive(Debug)]
pub struct LatencyMetrics {
    enabled: AtomicBool,
    buckets: [AtomicU64; NUM_BUCKETS],
    sum_us: AtomicU64,
    count: AtomicU64,
    max_us: AtomicU64,
}

#[derive(Debug, Default, Clone)]
struct Snapshot {
    counts: [u64; NUM_BUCKETS],
    sum_us: u64,
    count: u64,
    max_us: u64,
}

impl LatencyMetrics {
    /// Build a new histogram. `enabled = false` makes every `record_*` call a
    /// single `Relaxed` load + early return (sub-nanosecond when off).
    pub fn new(enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(enabled),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        })
    }

    /// Record one end-to-end sample, anchored at the message's
    /// geyser-callback timestamp. Cheap-path when disabled (~1 ns); when
    /// enabled, three `Relaxed` atomic ops + a single `SystemTime::now()`.
    pub fn record_end_to_end(&self, created_at: &Timestamp) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        // Malformed timestamps (negative or out-of-range fields) are dropped
        // rather than coerced — coercing would silently bias the histogram.
        let Ok(secs) = u64::try_from(created_at.seconds) else {
            return;
        };
        let Ok(nanos) = u32::try_from(created_at.nanos) else {
            return;
        };
        let Some(created) = UNIX_EPOCH.checked_add(Duration::new(secs, nanos)) else {
            return;
        };
        // `duration_since` returns Err if `now < created` (NTP step back, or
        // a slightly future-stamped message). Clamp to 0 so we still count
        // the sample.
        let elapsed = SystemTime::now()
            .duration_since(created)
            .unwrap_or_default();
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let idx = bucket_for(us);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Spawn the periodic reporter task. Idempotent w.r.t. `interval_seconds
    /// == 0` (no task is spawned). Use as `Arc::clone(&latency).spawn_reporter(...)`.
    pub fn spawn_reporter(
        self: Arc<Self>,
        interval_seconds: u64,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) {
        if interval_seconds == 0 {
            return;
        }
        let window = Duration::from_secs(interval_seconds);
        task_tracker.spawn(async move {
            let mut ticker = tokio_interval(window);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            ticker.tick().await; // drop the immediate first tick
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => break,
                    _ = ticker.tick() => {
                        let snap = self.snapshot_and_reset();
                        if snap.count == 0 {
                            continue;
                        }
                        let line = format_window_json(window.as_secs_f64(), &snap);
                        info!(target: LATENCY_LOG_TARGET, "{line}");
                    }
                }
            }
        });
    }

    /// Atomically drain and reset every counter so percentile math is
    /// consistent even if a recording thread interleaves mid-snapshot.
    fn snapshot_and_reset(&self) -> Snapshot {
        let mut counts = [0u64; NUM_BUCKETS];
        for (slot, atomic) in counts.iter_mut().zip(self.buckets.iter()) {
            *slot = atomic.swap(0, Ordering::Relaxed);
        }
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum_us = self.sum_us.swap(0, Ordering::Relaxed);
        let max_us = self.max_us.swap(0, Ordering::Relaxed);
        Snapshot {
            counts,
            sum_us,
            count,
            max_us,
        }
    }
}

/// Bucket assignment:
///   `0µs → 0`, `[1,2)µs → 0`, `[2,4)µs → 1`, `[4,8)µs → 2`, …
///   any value `≥ 2³¹µs` lands in bucket `NUM_BUCKETS-1`.
fn bucket_for(us: u64) -> usize {
    if us == 0 {
        return 0;
    }
    let msb = 63 - us.leading_zeros() as usize;
    msb.min(NUM_BUCKETS - 1)
}

/// Conservative percentile: walks buckets in order, returns the **upper
/// bound** of the bucket that crosses the `frac * total` threshold, capped at
/// the observed max so we never overstate the tail. Returns 0 if no samples.
fn percentile_us(snap: &Snapshot, frac: f64) -> u64 {
    if snap.count == 0 {
        return 0;
    }
    let threshold = ((snap.count as f64) * frac).ceil() as u64;
    let mut cum = 0u64;
    for (i, &c) in snap.counts.iter().enumerate() {
        cum = cum.saturating_add(c);
        if cum >= threshold {
            // Upper bound of bucket i is 2^(i+1) - 1; the last bucket has no
            // real upper bound so report the observed max instead.
            let upper = if i + 1 >= NUM_BUCKETS {
                snap.max_us
            } else {
                (1u64 << (i + 1)).saturating_sub(1)
            };
            return upper.min(snap.max_us.max(1));
        }
    }
    snap.max_us
}

fn format_window_json(window_secs: f64, snap: &Snapshot) -> String {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let p50 = percentile_us(snap, 0.50);
    let p95 = percentile_us(snap, 0.95);
    let p99 = percentile_us(snap, 0.99);
    let mean = if snap.count == 0 {
        0.0
    } else {
        snap.sum_us as f64 / snap.count as f64
    };
    format!(
        "{{\"ts_ms\":{ts_ms},\"metric\":\"end_to_end\",\"unit\":\"microseconds\",\
         \"window_secs\":{window_secs:.1},\"count\":{count},\"p50\":{p50},\
         \"p95\":{p95},\"p99\":{p99},\"max\":{max},\"mean\":{mean:.2}}}",
        count = snap.count,
        max = snap.max_us,
    )
}

/// Per-client outbound stream wrapper that records each delivered item's
/// `created_at`-anchored end-to-end latency at the moment tonic pulls it off
/// the queue. This is the **last point inside the plugin** before the
/// encode-and-socket-write step we don't control. Errors are passed through
/// without recording (an error has no meaningful end-to-end latency).
pub struct EndToEndTimedStream {
    inner: LoadAwareReceiver<TonicResult<FilteredUpdate>>,
    latency: Arc<LatencyMetrics>,
}

impl EndToEndTimedStream {
    pub fn new(
        inner: LoadAwareReceiver<TonicResult<FilteredUpdate>>,
        latency: Arc<LatencyMetrics>,
    ) -> Self {
        Self { inner, latency }
    }
}

impl Stream for EndToEndTimedStream {
    type Item = TonicResult<FilteredUpdate>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = Stream::poll_next(Pin::new(&mut this.inner), cx);
        if let Poll::Ready(Some(Ok(update))) = &polled {
            this.latency.record_end_to_end(&update.created_at);
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_timestamp(seconds_ago: u64, nanos_extra: u32) -> Timestamp {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let secs = now.as_secs().saturating_sub(seconds_ago);
        Timestamp {
            seconds: secs as i64,
            nanos: (now.subsec_nanos() + nanos_extra) as i32 % 1_000_000_000,
        }
    }

    #[test]
    fn bucket_for_log2_boundaries() {
        // Edge cases at the bottom.
        assert_eq!(bucket_for(0), 0);
        assert_eq!(bucket_for(1), 0); // [1,2)
        assert_eq!(bucket_for(2), 1); // [2,4)
        assert_eq!(bucket_for(3), 1);
        assert_eq!(bucket_for(4), 2); // [4,8)
        assert_eq!(bucket_for(7), 2);
        assert_eq!(bucket_for(8), 3); // [8,16)
        // Mid-range.
        assert_eq!(bucket_for(1_000), 9); // [512,1024)
        assert_eq!(bucket_for(1_000_000), 19); // [524288,1048576)
        // Saturating top.
        assert_eq!(bucket_for(1u64 << 31), NUM_BUCKETS - 1);
        assert_eq!(bucket_for(u64::MAX), NUM_BUCKETS - 1);
    }

    #[test]
    fn record_is_noop_when_disabled() {
        let m = LatencyMetrics::new(false);
        for _ in 0..100 {
            m.record_end_to_end(&make_timestamp(0, 0));
        }
        let snap = m.snapshot_and_reset();
        assert_eq!(snap.count, 0);
        assert_eq!(snap.sum_us, 0);
        assert_eq!(snap.max_us, 0);
        assert!(snap.counts.iter().all(|&c| c == 0));
    }

    #[test]
    fn record_counts_and_max_track() {
        let m = LatencyMetrics::new(true);
        for _ in 0..50 {
            m.record_end_to_end(&make_timestamp(0, 0)); // ~0 µs
        }
        // Older timestamp produces a larger us.
        m.record_end_to_end(&make_timestamp(1, 0)); // ~1_000_000 µs
        let snap = m.snapshot_and_reset();
        assert_eq!(snap.count, 51);
        assert!(snap.max_us >= 1_000_000);
        // The one ~1s sample sits in bucket 19 or 20 (depending on exact us);
        // there are 50 small samples plus the one large; the bulk of the
        // count is at the low end.
        let low_count: u64 = snap.counts.iter().take(8).sum();
        assert!(low_count >= 50);
    }

    #[test]
    fn snapshot_resets_state() {
        let m = LatencyMetrics::new(true);
        m.record_end_to_end(&make_timestamp(0, 0));
        let _ = m.snapshot_and_reset();
        let snap = m.snapshot_and_reset();
        assert_eq!(snap.count, 0);
        assert_eq!(snap.sum_us, 0);
        assert_eq!(snap.max_us, 0);
        assert!(snap.counts.iter().all(|&c| c == 0));
    }

    #[test]
    fn malformed_timestamp_is_dropped() {
        let m = LatencyMetrics::new(true);
        m.record_end_to_end(&Timestamp {
            seconds: -1,
            nanos: 0,
        });
        m.record_end_to_end(&Timestamp {
            seconds: 0,
            nanos: -1,
        });
        let snap = m.snapshot_and_reset();
        assert_eq!(snap.count, 0);
    }

    #[test]
    fn future_timestamp_clamps_to_zero_not_dropped() {
        let m = LatencyMetrics::new(true);
        let future = {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            Timestamp {
                seconds: (now.as_secs() + 10) as i64,
                nanos: now.subsec_nanos() as i32,
            }
        };
        m.record_end_to_end(&future);
        let snap = m.snapshot_and_reset();
        assert_eq!(snap.count, 1);
        assert_eq!(snap.max_us, 0);
        assert_eq!(snap.counts[0], 1);
    }

    #[test]
    fn percentile_capped_by_observed_max() {
        let mut snap = Snapshot {
            count: 1,
            max_us: 5, // value lands in bucket 2 = [4,8); upper bound = 7
            sum_us: 5,
            counts: [0; NUM_BUCKETS],
        };
        snap.counts[2] = 1;
        // Without capping we'd return 7; with capping we return 5.
        assert_eq!(percentile_us(&snap, 0.99), 5);
    }

    #[test]
    fn percentile_for_uniform_distribution() {
        // 100 samples spread across buckets 0..10: 10 each. p50 should be at
        // bucket 4 (cum 50 at end of bucket 4), p99 at bucket 9.
        let mut snap = Snapshot {
            count: 100,
            sum_us: 0,
            max_us: 1024,
            counts: [0; NUM_BUCKETS],
        };
        for b in 0..10 {
            snap.counts[b] = 10;
        }
        let p50 = percentile_us(&snap, 0.50);
        let p99 = percentile_us(&snap, 0.99);
        assert!(p50 >= 16 && p50 < 64, "p50={p50}");
        assert!(p99 >= 512 && p99 <= 1024, "p99={p99}");
    }

    #[test]
    fn percentile_empty_returns_zero() {
        let snap = Snapshot::default();
        assert_eq!(percentile_us(&snap, 0.99), 0);
    }

    #[test]
    fn format_window_json_has_required_fields() {
        let mut snap = Snapshot {
            count: 3,
            sum_us: 30,
            max_us: 20,
            counts: [0; NUM_BUCKETS],
        };
        snap.counts[3] = 3; // [8,16)
        let line = format_window_json(10.0, &snap);
        for key in [
            "\"ts_ms\":",
            "\"metric\":\"end_to_end\"",
            "\"unit\":\"microseconds\"",
            "\"window_secs\":10.0",
            "\"count\":3",
            "\"p50\":",
            "\"p95\":",
            "\"p99\":",
            "\"max\":20",
            "\"mean\":10.00",
        ] {
            assert!(line.contains(key), "missing {key:?} in {line}");
        }
    }

    #[test]
    fn format_window_json_empty_window() {
        let snap = Snapshot::default();
        let line = format_window_json(10.0, &snap);
        assert!(line.contains("\"count\":0"));
        assert!(line.contains("\"p99\":0"));
        assert!(line.contains("\"mean\":0.00"));
    }
}
