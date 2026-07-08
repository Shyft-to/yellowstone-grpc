# Implementation Run Log

## Meta
- Task: Port four latency-reducing optimizations from the open-source `master` branch onto this branch's dispatch/filter architecture. Production's `geyser_dispatch` CPU-pinned spin-loop design must be preserved, not replaced with master's async design. The four changes:
  1. Decouple block reconstruction from the Processed-commitment fan-out hot path: move slot/block bookkeeping (BTreeMap<u64,SlotMessages> tracking, dedup, sealing, Confirmed/Finalized/Block message construction) off the `geyser_dispatch` spin-loop thread and into a separate dedicated thread, so Processed-commitment messages are broadcast without waiting on block-assembly bookkeeping. Reference master commits `f7087d3` (introduces `block_reconstruction.rs`/`BlockMachineStorage`) and `2146785` (splits geyser_loop into geyser_loop + block_reconstruction_loop) for the target shape — but adapt the handoff mechanism to this branch's synchronous CPU-pinned spin-loop thread rather than copying master's tokio-runtime-per-thread approach verbatim.
  2. Remove the `ParallelEncoder` rayon/channel bridge (`yellowstone-grpc-geyser/src/parallel.rs`) and replace it with a direct synchronous `encode_messages()` call in the hot path, per master's `1453f2c`.
  3. In `yellowstone-grpc-geyser/src/plugin/filter/filter.rs`, replace std `HashMap`/`HashSet` with `foldhash` for the account/owner filter-matching lookup tables, and remove the shared `Arc<Mutex<FilterNames>>` in favor of a per-connection `FilterNames` instance, per master's `72cf363`.
  4. Wire up `tikv-jemallocator` as the `#[global_allocator]` in `yellowstone-grpc-geyser/src/lib.rs`, per master's `1453f2c`.

  Goal: reduce end-to-end and tail latency of gRPC fan-out without regressing correctness of block reconstruction (duplicate slot handling, gc timing, confirmed/finalized ordering) or existing config/API compatibility.
- Base branch: i2
- Feature branch: implement/port-master-latency-opts
- Status: PLANNING
- Started: 2026-07-08
- Last updated: 2026-07-08

## Approved Plan
<pending>

## Planning History
<pending>

## Task Progress
| # | Task | Status | Attempts | Last Verdict | Commit |
|---|------|--------|----------|---------------|--------|

## Blockers
<none>
