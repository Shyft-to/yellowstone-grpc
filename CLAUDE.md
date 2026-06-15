# yellowstone-grpc-geyser — AI Harness

## Goal
Reduce end-to-end latency sending blockchain tx updates to gRPC subscribers.
Bold changes welcome. >100ms win in geyser bench = worth implementing.

## Key files
- `yellowstone-grpc-geyser/src/grpc.rs` — geyser_dispatch (spin-loop, CPU-pinned) + client_loop fan-out
- `yellowstone-grpc-geyser/src/parallel.rs` — ParallelEncoder (rayon pool, pre_encoded OnceLock)
- `yellowstone-grpc-geyser/src/metrics.rs` — SubscriberMetrics (pre-resolved handles, lock-free per-sub)
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs` — Filter::get_updates hot path
- `yellowstone-grpc-geyser/src/plugin/filter/message.rs` — FilteredUpdate, FilteredUpdates types

## Architecture (condensed)
```
geyser_callbacks → mpsc::UnboundedSender<Message>
  → geyser_dispatch (std::thread, CPU-pinned, spin-loop, grpc.rs:1126)
      → ParallelEncoder.encode_blocking (rayon)
      → tokio::broadcast::Sender<(CommitmentLevel, Arc<Vec<(u64,Message)>>)>
          → N × client_loop (tokio tasks, grpc.rs:1456)
              filter.get_updates(msg) → stream_tx.try_send(FilteredUpdate)
                  → tonic → HTTP/2 → TCP
```
~90% of messages are CommitmentLevel::Processed. Batch flush: inbox-empty OR batch>=31 OR Slot msg.

## Harness commands
```
/project:optimize [N]    # run next N best OPEN candidates (default 1)
bash .claude/scripts/status.sh   # session startup: branch + bench + open candidates
python3 .claude/scripts/parse_bench.py readings.md
python3 .claude/scripts/parse_race.py readings2.md
python3 .claude/scripts/compare_bench.py before.md after.md --subs 700
```

## Context & state
- `.claude/context/candidates.md` — ranked optimization candidates (source of truth for harness)
- `.claude/context/arch.md` — architecture + ruled-out optimizations
- `.claude/state/experiments.jsonl` — log of all experiment runs (PASS/BLOCKED/iterations)
- `.claude/state/session.md` — ephemeral notes from last session
- `HARNESS_README.md` — full documentation
