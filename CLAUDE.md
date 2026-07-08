# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Yellowstone Dragon's Mouth — a Geyser-based gRPC interface for Solana validators, maintained by Triton One. A Solana Geyser plugin (Rust) streams slots, blocks, transactions, accounts, and entries to subscribers over gRPC, with sample clients in Rust, TypeScript, Go, and Python.

## Commands

From the repo root (workspace):

- `cargo build` — build all workspace crates
- `cargo test --all-features` — full test suite (what CI runs)
- `cargo test -p yellowstone-grpc-geyser <test_name>` — run a single test (substring match) in one crate; add `-- --nocapture` for println/log output
- `cargo +nightly fmt --all -- --check` — formatting check; CI uses `nightly` specifically for this (see `rustfmt.toml`: `imports_granularity`/`group_imports = "One"`)
- `cargo clippy --workspace --all-targets` — must be clean; `ci/cargo-build-test.sh` additionally builds/tests with `RUSTFLAGS="-D warnings"`
- `cargo check -p <crate> --all-targets` — CI checks `yellowstone-grpc-client`, `yellowstone-grpc-client-simple` (the `examples/rust` package), `yellowstone-grpc-geyser`, and `yellowstone-grpc-proto` (also with `--all-features`) individually
- `cargo run --bin config-check -- --config yellowstone-grpc-geyser/config.json` — validate a plugin config file without starting a validator
- `make install-hooks` — points `core.hooksPath` at `.githooks/`; the pre-commit hook requires `commit.gpgsign=true` and warns (non-blocking) if `cargo fmt --all -- --check` fails, including in the `napi` subcrate if it has staged changes

Rust toolchain version is pinned in `rust-toolchain.toml`. The `napi` subcrate (`yellowstone-grpc-client-nodejs/napi`) is excluded from the main workspace and has its own `Cargo.toml`/toolchain — build/lint it from within that directory (`make solana-encoding-napi-clippy` runs its clippy).

Run a validator against the plugin: `solana-validator --geyser-plugin-config yellowstone-grpc-geyser/config.json`.

## Workspace layout

- `yellowstone-grpc-geyser` — the plugin + gRPC server; the core of this repo (see Architecture below)
- `yellowstone-grpc-proto` — protobuf definitions (`proto/geyser.proto`, `proto/solana-storage.proto`) and generated Rust types, plus a cuckoo-filter implementation (`src/cuckoo/`) used for large-set account filter matching
- `yellowstone-grpc-client` — Rust client library (`GeyserGrpcClient`), reconnect logic, dedup helpers
- `yellowstone-grpc-client-nodejs` — Node.js/TypeScript client, including the `napi` Rust subcrate
- `examples/{rust,typescript,golang,python}` — sample clients per language

## Architecture: the geyser → gRPC fan-out pipeline

This is the part that requires reading several files together, and most work in this repo touches it.

1. **Plugin entrypoint** (`plugin/entry.rs`): implements Agave's `GeyserPlugin` trait. Every Geyser callback (`update_account`, `notify_transaction`, `update_slot_status`, `notify_block_metadata`, `notify_entry`, ...) converts the Agave type into this crate's internal `Message` enum (`plugin/message.rs`: `Message::{Account,Slot,Transaction,Entry,BlockMeta,Block}`) and pushes it onto an unbounded `mpsc` channel — the only point of contact between the synchronous Geyser callback world and the async gRPC server.

2. **Dispatch loop** (`grpc.rs`, `GrpcService::geyser_loop` / `geyser_dispatch`): drains that channel, batches messages (batch size is config-tunable via `processed_messages_max`), reconstructs full blocks from the individual account/transaction/entry/blockmeta messages for a slot (`SlotMessages`, tracked in a `BTreeMap<slot, SlotMessages>` — this is what the README's "Block reconstruction" section and the `invalid_full_blocks_total` metric refer to), then broadcasts each commitment level (`Processed`/`Confirmed`/`Finalized`) over a `tokio::broadcast` channel. `geyser_dispatch` is an alternate, synchronous `std::thread` implementation of the same loop that busy-polls via `try_recv()` instead of `.await`ing the channel, and can be pinned to a CPU core (`geyser_dispatch_cpu_core` in config) — it replaces the async `geyser_loop` when that config field is set. Encoding messages to protobuf bytes is offloaded to a small rayon thread pool bridged in via `ParallelEncoder` (`parallel.rs`) so the dispatch loop doesn't block on serialization.

3. **Per-client fan-out**: each `Subscribe` gRPC call spawns its own task that subscribes to the broadcast channel and, for every batch, calls `Filter::get_updates()` (`plugin/filter/filter.rs`) to produce only the messages that client's subscription actually matches (accounts by pubkey/owner/data filters, transactions by account include/exclude/required, etc.), then sends them down that client's stream. `plugin/filter/encoder.rs` pre-encodes accounts/transactions to protobuf bytes once (cached via `OnceLock`) so fan-out to many subscribers doesn't re-serialize the same message per client. `plugin/filter/name.rs` and `limits.rs` handle filter-name bookkeeping and per-request filter limits (`ConfigGrpc.filters`, documented in the README).

4. **Config** (`config.rs`): a single `Config` struct covering Geyser-side tuning (`ConfigTokio`, batching/dispatch knobs) and gRPC server setup (`ConfigGrpc`: addresses/TLS/compression/filter limits/Prometheus). `bin/config-check.rs` validates a config file standalone, without running a validator.

5. **Metrics/transport**: `metrics.rs` exposes Prometheus counters/gauges (subscriber counts, queue sizes, invalid-block counts, etc.); `metered.rs` and `transport.rs` are tower/tonic layers wrapping the gRPC transport for bandwidth accounting and connection-level instrumentation.

This pipeline is latency-sensitive — data must reach subscribed clients as fast as possible after a Geyser callback fires. Changes to `grpc.rs`, `parallel.rs`, or `plugin/filter/*` should be evaluated for their effect on the hot path (allocations, locks, clones, blocking calls incurred per message per subscriber), not just correctness.

## Structured changes via `/implement`

This repo has a multi-agent harness for implementing changes end-to-end (planner → plan-evaluator → executor → validator, resumable state, small per-task commits on a dedicated branch) — see `.claude/commands/implement.md`. It only runs when explicitly invoked with `/implement`; it is not part of normal request handling.
