# CPU Pinning Configuration

CPU pinning locks specific threads to dedicated CPU cores so the OS scheduler never migrates them. This keeps CPU caches warm and eliminates contention with Tokio worker threads, reducing end-to-end latency on the hot path.

## Why it helps

A Solana validator handles a continuous stream of transactions. The gRPC plugin has three compute-heavy threads on the critical path from receiving a message to broadcasting it to subscribers:

```
[Validator] → [geyser-dispatch] → [geyser-encoder-bridge] → [geyser-encoder-0..N] → [broadcast]
```

Without pinning, the OS freely migrates these threads between cores. Each migration invalidates L1/L2 cache data the thread was working with, forcing expensive reloads from L3 or RAM. By pinning, each thread stays on its core, keeping its working data hot in cache across consecutive operations.

## Config fields

All fields live under the `grpc` key in your plugin config JSON.

| Field | Type | Default | Description |
|---|---|---|---|
| `geyser_dispatch_cpu_core` | `int \| null` | `null` | Core for the main dispatch thread. When set, enables spin-loop mode (eliminates the 10ms batch timer). |
| `encoder_threads` | `int` | `4` | Number of rayon worker threads for parallel protobuf pre-encoding. |
| `encoder_cpu_cores` | `[int] \| null` | `null` | One core per encoder worker. Length must equal `encoder_threads`. |
| `encoder_bridge_cpu_core` | `int \| null` | `null` | Core for the bridge thread that dispatches encode batches to the rayon pool. |

`encoder_cpu_cores` and `encoder_bridge_cpu_core` are independent — you can pin the bridge without pinning workers or vice versa.

## Example config

```json
{
  "libpath": "...",
  "grpc": {
    "address": "0.0.0.0:10000",
    "encoder_threads": 2,
    "geyser_dispatch_cpu_core": 0,
    "encoder_bridge_cpu_core": 1,
    "encoder_cpu_cores": [2, 3]
  }
}
```

## Selecting the right cores

### Step 1 — see the physical layout

```bash
lscpu --all --extended
```

Look at the `CORE` column. Hyperthreaded machines show two logical CPUs per physical core. **Avoid putting two pinned threads on sibling logical CPUs** (same physical core) — they share L1/L2 and compete for execution units.

```
CPU  NODE  SOCKET  CORE  ...
0    0     0       0       ← physical core 0, thread 0
1    0     0       0       ← physical core 0, thread 1  ← sibling of CPU 0
2    0     0       1       ← physical core 1, thread 0
3    0     0       1       ← physical core 1, thread 1  ← sibling of CPU 2
...
```

In this layout, use CPUs `0, 2, 4, 6, ...` (or `1, 3, 5, 7, ...`) — one logical CPU per distinct physical core.

### Step 2 — identify cores already in use

```bash
# See what the validator process is using
taskset -cp $(pgrep -f agave-validator)

# See all thread affinities for the validator
ps -eLo pid,tid,psr,comm | grep agave-validator | head -30
```

The Tokio runtime threads are typically on the cores listed in the `tokio.affinity` config field (if set). Pick cores outside that set for the pinned threads.

### Step 3 — check NUMA topology (multi-socket machines)

```bash
numactl --hardware
```

Keep all pinned threads on the **same NUMA node** as the main validator process. Cross-NUMA memory access is ~2x slower than local access.

```bash
# Which NUMA node is the validator on?
numactl --show
```

### Step 4 — verify pinning took effect

After startup, confirm each thread landed on the right core:

```bash
# List all threads with their current CPU
ps -eLo pid,tid,psr,comm | grep -E "geyser-dispatch|geyser-encoder"

# Or watch in real time
watch -n1 'ps -eLo pid,tid,psr,comm | grep geyser'
```

The `PSR` column is the CPU the thread is currently running on. With pinning, it should always show the configured core.

## Recommended starting point

On a 16-core validator where Tokio is pinned to cores `4-15`:

```json
{
  "geyser_dispatch_cpu_core": 0,
  "encoder_bridge_cpu_core": 1,
  "encoder_threads": 2,
  "encoder_cpu_cores": [2, 3]
}
```

This leaves cores `4-15` entirely for Tokio (subscriber fan-out) and reserves cores `0-3` exclusively for the encoding pipeline.

Start with `encoder_threads: 2`. Only increase if profiling shows encoding is still the bottleneck — more threads means more cores consumed, which shrinks the pool available for Tokio.
