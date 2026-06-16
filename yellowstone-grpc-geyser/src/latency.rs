use {
    lazy_static::lazy_static,
    prometheus::{Histogram, HistogramOpts, Registry},
    std::{cell::Cell, time::Instant},
};

lazy_static! {
    pub static ref DISPATCH_LATENCY_US: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "geyser_dispatch_latency_us",
            "End-to-end latency from geyser message receipt to broadcast_tx.send() \
             (CommitmentLevel::Processed), in microseconds",
        )
        .buckets(vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0,
            10_000.0,
        ])
    )
    .unwrap();
}

thread_local! {
    static BATCH_START: Cell<Option<Instant>> = const { Cell::new(None) };
}

pub fn register(registry: &Registry) {
    registry
        .register(Box::new(DISPATCH_LATENCY_US.clone()))
        .expect("DISPATCH_LATENCY_US can't be registered");
}

/// Call once per message received in `geyser_dispatch`.
/// Records the start time of the current batch if not already set.
#[inline]
pub fn on_message_received() {
    BATCH_START.with(|cell| {
        if cell.get().is_none() {
            cell.set(Some(Instant::now()));
        }
    });
}

/// Call immediately after each `broadcast_tx.send((CommitmentLevel::Processed, ...))`.
/// Observes the elapsed time since the first message in the batch was received.
#[inline]
pub fn on_batch_dispatched() {
    BATCH_START.with(|cell| {
        if let Some(start) = cell.take() {
            DISPATCH_LATENCY_US.observe(start.elapsed().as_micros() as f64);
        }
    });
}
