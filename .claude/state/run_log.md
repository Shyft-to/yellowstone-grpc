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
### Round 1
- Planner output (agent id aae8f132a562ea20e):

  Reordered execution: 2 (drop ParallelEncoder) → 4 (jemalloc) → 3 (foldhash + per-connection FilterNames) → 1 (block-reconstruction split, as tasks 4/5/6: characterization tests, pure extraction refactor, thread split). Flagged as an assumption for the evaluator; content unaffected by renumbering.

  Verified ground truth: `geyser_dispatch` (sync spin-loop) and `geyser_loop` (async fallback) in `grpc.rs` both inline identical BTreeMap<u64,SlotMessages> bookkeeping; `GrpcService` holds `filter_names: Arc<Mutex<FilterNames>>`; `parallel.rs` has rayon+mpsc+oneshot bridge; `filter.rs` uses std HashMap/HashSet plus this branch's own `msg_type_mask` bitmask fast-path (separate from master's `72cf363` aggregate rewrite, not adopted here); no jemalloc/foldhash deps present; zero existing unit tests on the BTreeMap bookkeeping; baseline `cargo test -p yellowstone-grpc-geyser` = 56 passed.

  **Task 1** — Remove `ParallelEncoder` bridge: add `encode_messages()` free fn in `encoder.rs`, replace 5 call sites in `geyser_dispatch`/`geyser_loop`, drop the `parallel_encoder` param from `GrpcService::create`/both loop fns, update `plugin/entry.rs` caller, delete `parallel.rs` + `pub mod parallel` + `encoder_threads` config, drop unused `rayon` dep if orphaned. Tests: port the 4 existing parallel.rs tests into encoder.rs, add idempotency test. Risk: low.

  **Task 2** — jemalloc: add `tikv-jemallocator` (`disable_initial_exec_tls` feature) as direct dep of `yellowstone-grpc-geyser`, `#[global_allocator]` in `lib.rs`. Tests: build + full suite must still pass (allocator swap has no test-visible behavior); flagged risk: cdylib + custom global allocator interaction with host validator process is unverifiable outside a live validator. Risk: low.

  **Task 3** — Filter foldhash + per-connection FilterNames: add `foldhash` workspace dep; retype `FilterAccounts.account/.account_required/.account_cuckoo/.owner/.owner_required/.nonempty_txn_signature_required` and `FilterAccountsMatch`'s five `HashSet<&str>` fields to foldhash (does NOT adopt master's aggregate/reverse-index restructuring, per task's explicit carve-out). Remove `Arc<Mutex<FilterNames>>` from `GrpcService`, store size-limit primitives instead, construct `FilterNames` locally per connection in the incoming-filter task. Tests: existing suite must pass unchanged; add one new test proving two concurrent connections' `FilterNames` don't interact. Risk: low-medium.

  **Task 4** — Characterization tests (test-only) for current `geyser_dispatch` bookkeeping: dedup by write_version, block sealing gating, duplicate-BlockMeta detection, missed-status parent-slot propagation, gc timing (`FINALIZATION_SAFETY_BUFFER=10 + replay_stored_slots`), replay-buffer servicing. This is the regression net for tasks 5/6. Risk: low (test-only).

  **Task 5** — Pure extraction refactor: new `block_reconstruction.rs` module with `SlotMessages`, `MessageId`, and a `BlockReconstructionState` struct exposing `on_message(...)` reproducing today's bookkeeping verbatim; rewire `geyser_dispatch`/`geyser_loop` to call it, no threading change yet. Task 4's tests must pass unchanged before/after as proof of behavior preservation; add direct unit tests against the new struct. Risk: medium (large mechanical diff, no concurrency risk yet).

  **Task 6** — Thread split (the actual latency win): shared thread-safe `MessageIdGen` (both threads must draw from one monotonic id space — required for `client_loop`'s replay `sort_by_key` correctness); spawn a plain blocking `std::thread` (NOT master's tokio-runtime-per-thread pattern — deliberate deviation, justified by consistency with this codebase's existing non-async-thread idioms) owning `BlockReconstructionState`, fed via a new `mpsc::UnboundedSender<(u64,Message)>` from `geyser_dispatch`. `geyser_dispatch` becomes: try_recv → assign id → forward to reconstruction thread → batch+encode+broadcast Processed for raw messages only, no BTreeMap/gc/sealing touched directly. Reconstruction thread handles Confirmed/Finalized broadcasts plus derived Processed messages (sealed Block, synthesized missed-status Slot) — no re-encoding needed (shared Arc+OnceLock). Shutdown: sender drop propagates channel closure; new JoinHandle joined in `plugin/entry.rs` alongside existing encoder_handle-style join. `geyser_loop` (async fallback) explicitly NOT split. Tests: full 2-thread pipeline correctness (Processed/Confirmed/Finalized all correct/ordered), a decoupling proof (inject reconstruction-thread backpressure, assert Processed for unrelated slots still flows promptly), duplicate-slot/gc/ordering regressions carried through the real pipeline, clean shutdown test. Risk: high — explicitly the class of change the standing objective flags as must-not-regress; mitigated by tasks 4-5 building the regression net first.

  Explicit non-goals: master's `yellowstone_block_machine`/`BlockMachineStorage` crate (full external rewrite, out of scope), tokio-runtime-per-thread pattern, splitting the async `geyser_loop` fallback, master's `72cf363` FilterAccounts aggregate restructuring, CPU-pinning the new thread, any already-settled latency work from memory.

  Open questions raised for evaluator: (a) task-order deviation (2,4,3,1a-c vs literal 1,2,3,4) — content unaffected either way; (b) plain std::thread vs tokio-runtime-per-thread for reconstruction thread — task explicitly left as "your call, justify it," planner chose std::thread for idiom consistency; (c) should async `geyser_loop` fallback get the same split for consistency — currently a non-goal, flagged as a minor maintainability cost; (d) jemalloc/cdylib/host-validator-process interaction unverifiable outside live validator.

- Evaluator verdict: REJECTED (agent id ae2bc63f32d33d071)
- Evaluator feedback:
  1. Task 6 is one task pretending to be atomic — bundles shared id-generator, new thread+channel infra, moving Confirmed/Finalized/derived-Processed ownership, and shutdown/join wiring. Split into: **6a** spawn reconstruction thread+channel but route ALL broadcasts (incl. Processed) through it initially, verify pipe/ordering against task-4/5 regression net with zero latency win yet claimed; **6b** the actual decoupling — geyser_dispatch broadcasts raw Processed directly instead of waiting on reconstruction thread (this is the one change that earns the latency win, needs dedicated ordering/race tests); **6c** shutdown/join wiring.
  2. The shared `MessageIdGen` does NOT solve cross-thread live-ordering and the plan doesn't acknowledge this. `client_loop`'s live path (broadcast::Sender recv, ~lines 1700-1745) trusts raw broadcast arrival order — only the replay/reconnect path (~line 1668, `sort_by_key`) re-sorts by id. Once geyser_dispatch (raw Processed) and the reconstruction thread (derived Block/missed-status Slot/Confirmed/Finalized) both independently `broadcast_tx.send()` for the same commitment, live delivery order is governed by broadcast-channel send-race, not msgid. Master's own 2146785/f7087d3 has this same two-writer shape but master dropped msgid/sort_by_key entirely (BroadcastedMessage has no per-message id there) — this branch still has replay depending on it, so must explicitly reconcile. Round 2 must: (a) state what ordering guarantee is relaxed for live Processed delivery, (b) confirm no downstream consumer needs strict cross-slot ordering there (only per-pubkey write_version / per-slot causal order matters), (c) add a concurrency test backpressuring the reconstruction thread asserting specific invariants (e.g. client never sees Confirmed/Finalized for a slot before the correct raw Processed set for that slot).
  3. Task 4's characterization tests don't cover live-broadcast delivery-order guarantee at all (only bookkeeping-state correctness). Add a pre-split baseline test: "a live Processed subscriber receives messages in strictly increasing msgid order" so later tasks can honestly report whether Task 6 preserves/weakens/breaks it.
  4. Minor factual correction: Task 1 call-site count is 6, not 5 (encode() x3 in geyser_loop @1036/1077/1101, encode_blocking() x3 in geyser_dispatch @1428/1469/1491).
  5. Task 1 is a throughput change, not just correctness-preserving refactor: today's geyser_dispatch parallelizes batches >=4 via rayon `pool.install`; master's `encode_messages()` is fully sequential. Plausibly a net win but currently rated "risk: low" on correctness grounds only — add a throughput note/microbench comparing sequential vs today's rayon-batch encode at realistic batch sizes.
  6. Task 3's FilterNames de-sharing trade-off (loses cross-connection string-interning dedup, a memory trade-off not a fan-out-latency fix — verified FilterNames is only locked in the per-connection subscribe path, not per-message hot path) should be stated explicitly with a characterization note/test for many-connections-same-filter-name, even if the answer is "acceptable, matches master's precedent."
  7. No objection to Task 1 before Task 6 sequencing (geyser_dispatch already calls sync encode_blocking, so Task 6 gets one clean sync encode entry point).

### Round 2
- Planner output: (pending)
- Evaluator verdict: (pending)
- Evaluator feedback: (pending)

## Task Progress
| # | Task | Status | Attempts | Last Verdict | Commit |
|---|------|--------|----------|---------------|--------|

## Blockers
<none>
