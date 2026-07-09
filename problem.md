# Incident: jemalloc + dlopen'd Geyser plugin crash

## Symptom

After merging the `port-master-latency-opts` work into `i2` and loading the
plugin in a live validator, the process crashed on the first transaction
notification:

```
loading plugin: yellowstone-grpc-geyser-13.1.0+7effccc
[...] INFO  yellowstone_grpc_geyser::metrics] start prometheus server: 0.0.0.0:11001
[...] INFO  solana_core::repair::cluster_slot_state_verifier] check_slot_agrees_with_cluster() ...
[...] INFO  yellowstone_grpc_geyser::grpc] gRPC server listening on 0.0.0.0:11000
memory allocation of 9288674231451664 bytes failed
stack backtrace:
   0: std::alloc::rust_oom
   1: __rustc::__rust_alloc_error_handler
   2: alloc::alloc::handle_alloc_error
   3: hashbrown::raw::Fallibility::alloc_err
   4: hashbrown::raw::RawTable<T,A>::reserve_rehash
   5: yellowstone_grpc_geyser::plugin::message::MessageTransactionInfo::from_geyser
   6: yellowstone_grpc_geyser::plugin::message::MessageTransaction::from_geyser
   7: <yellowstone_grpc_geyser::plugin::entry::Plugin as agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin>::notify_transaction
   8: <solana_geyser_plugin_manager::transaction_notifier::TransactionNotifierImpl as solana_rpc::transaction_notifier_interface::TransactionNotifier>::notify_transaction
   9: solana_rpc::transaction_status_service::TransactionStatusService::write_transaction_status_batch
```

A "memory allocation of 9288674231451664 bytes failed" inside
`hashbrown::RawTable::reserve_rehash`, on a **freshly constructed**
`HashSet<Pubkey>` (`MessageTransactionInfo::from_geyser` builds
`account_keys` via `.collect()` from borrowed transaction data), is the
signature of heap corruption, not a genuine request for that much memory —
something wrote garbage into the table's capacity/control-byte bookkeeping
earlier, and this allocation is just the first innocent bystander to read it.

## Why the port's own changes are ruled out

The crash is in `MessageTransactionInfo::from_geyser`, called from
`notify_transaction` — a callback the validator's own
`TransactionStatusService` thread invokes *before* the message ever reaches
`geyser_dispatch` or the reconstruction thread. None of the port's logic
(`ParallelEncoder` removal, filter foldhash, the block-reconstruction thread
split) runs upstream of this point, so all of that is architecturally ruled
out by the crash location alone. `account_keys` is a plain
`std::collections::HashSet`, untouched by the foldhash retyping (which only
touched `FilterAccounts`/`FilterAccountsMatch` in `filter.rs`).

That leaves jemalloc (Task 2 of the port) as the only change that touches
process-wide allocator behavior — and it's the one change whose review
explicitly flagged an unverifiable-outside-a-live-validator risk.

## The mechanism

**1. Why jemalloc normally helps.** jemalloc splits the heap into multiple
arenas and gives each thread a thread-local cache (tcache) for small
allocations, avoiding the contention glibc's malloc can hit under heavy
concurrent allocation — the reason master adopted it for the fan-out path.

**2. The dlopen problem jemalloc has.** jemalloc's tcache/arena-assignment
state lives in thread-local storage. The fastest TLS access model on Linux
(`initial-exec`) assumes the dynamic linker reserved that thread's TLS block
*at process startup*, based on every shared library linked in at that time.
A Geyser plugin isn't linked at startup — it's `dlopen()`'d later, after the
validator is already running. If jemalloc's TLS variables try to use
`initial-exec` from a library loaded that way, glibc can refuse outright with
"cannot allocate memory in static TLS block." This is a well-documented,
long-standing jemalloc issue
([jemalloc#1237](https://github.com/jemalloc/jemalloc/issues/1237),
[Debian bug #951704](https://bugs.debian.org/951704)).

**3. What `disable_initial_exec_tls` trades away.** The feature flag enabled
in both master and this port avoids the crash in (2) by forcing jemalloc onto
the slower `global-dynamic` TLS model, which is safe to use from a `dlopen`'d
library. But jemalloc's own maintainers are explicit about what this costs,
from the project's mailing list:

> "If the `--disable-initial-exec-tls` workaround is used, there's a serious
> risk: if a pointer leaks from one implementation to the other through a
> realloc call or free, there would be some really weird and confusing
> crashes or deadlocks."
>
> "Upstream recommends avoiding shipping with `--disable-initial-exec-tls`...
> shared libraries should not define their own malloc independent of the
> process malloc."
>
> — [jemalloc-discuss mailing list, "jemalloc initialization in a shared library"](https://jemalloc.net/mailman/jemalloc-discuss/2016-September/001323.html)

This flag doesn't make jemalloc-in-a-dlopen'd-library *safe* — it converts a
loud, obvious startup crash into a quieter, deferred one. With it enabled,
jemalloc becomes a second allocator implementation coexisting in the process
alongside whatever the host binary (the validator) itself uses, instead of
being the process's one true allocator (how jemalloc is designed and tested
to run). If any allocation's ownership ever crosses that boundary — freed or
resized on the wrong side — the result is corrupted allocator bookkeeping,
not a clean segfault. A hashmap reading a garbage capacity value and
requesting a 9.28-petabyte allocation is exactly that failure shape.

## Why this surfaced on this fork and not on master

One alternative explanation was checked and ruled out: jemalloc's background
maintenance threads (periodic purge/decay) are **runtime-disabled by
default** in `tikv-jemallocator` and were never enabled here, so CPU-pinning
starving a background thread isn't the mechanism.

The real differentiator is *when and how many independent OS threads make
their first-ever allocation through the plugin's jemalloc at once* — exactly
where jemalloc's dlopen/TLS fragility lives:

- Master's dispatch work runs as `async fn`s on cooperatively-scheduled tokio
  tasks. Even master's own extra thread (`block_reconstruction_loop`) spins
  up with its own dedicated single-threaded tokio runtime — a materially
  gentler, more centrally-coordinated startup than a raw thread.
- This fork's `geyser_dispatch` is a `std::thread` that busy-spins
  (`std::hint::spin_loop()` + `try_recv()`) pinned to a specific core via
  `sched_setaffinity`, and after this port, a second new thread
  (`block_reconstruction`) joins it — both start allocating aggressively the
  instant the plugin loads.
- Meanwhile, the validator's own pre-existing threads (like the
  `TransactionStatusService` worker in the crash trace) are, for many of
  them, making their **first-ever call into this freshly-`dlopen`'d `.so`**
  around the same moment — the first `notify_transaction` on a thread that's
  never touched the plugin's code before is precisely a first-touch
  TLS/allocator initialization event.

Master never produces this dense a cluster of independent threads all hitting
jemalloc's dlopen-fragile init path at nearly the same instant. This fork's
CPU-pinned, busy-spinning architecture does, by design — minimizing
scheduling latency also means minimizing the gradual, staggered warm-up a
cooperative scheduler would naturally provide.

## A gap worth naming honestly

Verification of the jemalloc change in the original port review (confirming
the built artifact linked jemalloc symbols, clean release build) was done on
**macOS**, producing a `.dylib`. macOS's dynamic linker (`dyld`) has a
completely different TLS/dlopen model than Linux's glibc+ELF — this bug class
is Linux/ELF-specific and cannot be exercised on macOS at all. Nothing short
of loading the actual `.so` into an actual Linux validator would have caught
this, which is exactly what happened.

## Confidence level

This diagnosis is well-supported by elimination (the crash is upstream of
everything else the port changed), by directly matching jemalloc's own
maintainer-acknowledged risk for this exact flag, and by architectural
reasoning for why this fork exposes it and master doesn't. It has **not**
been confirmed with a core dump or a debug-allocator run (jemalloc's
`--enable-debug`/redzone build, or a tool like Valgrind) to pinpoint the
exact corrupting write — so the mechanism class is well-evidenced, not the
precise faulting line of code.

## Resolution

Reverted jemalloc as the `#[global_allocator]`:

- `fix/revert-jemalloc-global-allocator` (forked from `i2` HEAD)
  - `b3c2bdb` — removes `tikv-jemallocator` as a direct dependency of
    `yellowstone-grpc-geyser` and the `#[global_allocator]` wiring in
    `lib.rs`. Verified: release build succeeds, built `.dylib` has zero
    jemalloc symbols linked (down from 438 with it present).
  - `905f843` — unrelated pre-existing test-compile break fixed in the same
    branch (missing `token_accounts` field on
    `SubscribeRequestFilterTransactions` in 7 test fixtures, introduced by a
    separate merge). 90/90 tests pass.

All other changes from the latency-optimization port (`ParallelEncoder`
removal, filter foldhash + per-connection `FilterNames`, the
block-reconstruction thread split) are retained — none of them are
implicated in this crash.

## If jemalloc's benefit is wanted back later

The standard production-safe pattern is `LD_PRELOAD`-ing jemalloc for the
whole validator process, making it the one true process-wide allocator from
the start and sidestepping the coexistence problem entirely — rather than
wiring it as a Rust `#[global_allocator]` inside just the plugin. That's an
operational/deployment decision, not something this repo controls.

## Sources

- [jemalloc 5.0.1 TLS error: cannot allocate memory in static TLS block · Issue #1237](https://github.com/jemalloc/jemalloc/issues/1237)
- [jemalloc initialization in a shared library — jemalloc-discuss mailing list](https://jemalloc.net/mailman/jemalloc-discuss/2016-September/001323.html)
- [Debian Bug #951704 — jemalloc cannot be dynamically loaded after startup (dlopen)](https://bugs.debian.org/951704)
- [static TLS errors from jemalloc 5.0.0 built on CentOS 6 · Issue #937](https://github.com/jemalloc/jemalloc/issues/937)
- [tikv-jemalloc-sys — crates.io](https://crates.io/crates/tikv-jemalloc-sys)
