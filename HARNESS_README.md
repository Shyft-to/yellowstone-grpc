# yellowstone-grpc Optimization Harness

AI-driven optimization harness for reducing geyser fan-out latency. Each optimization runs in
an Executor → Evaluator agent loop on an isolated git branch, with results persisted to a state log.

---

## Quick start

```bash
# See current state before starting
bash .claude/scripts/status.sh

# Run the next 1 best optimization from candidates.md
/project:optimize

# Run the next 3 best optimizations sequentially
/project:optimize 3

# Implement any open-ended task with the 4-agent loop
/project:implement decouple geyser_dispatch from the encoder
```

`/project:optimize` works from the pre-specified candidate list in `candidates.md`.
`/project:implement` accepts any free-form task description and figures out the plan itself.

---

## Two commands, two use cases

| Command | Use when | Task source |
|---------|----------|-------------|
| `/project:optimize [N]` | Running pre-specified optimizations from `candidates.md` | Human-written candidate descriptions |
| `/project:implement <task>` | Implementing any open-ended task from a description | Arbitrary natural-language request |

---

## `/project:optimize` — How it works

### Candidate selection

Candidates live in `.claude/context/candidates.md`. The harness reads this file at runtime,
finds all candidates with status `[OPEN]`, filters out those already in `experiments.jsonl`,
and selects the top N by tier (Tier 1 first, then Tier 2, then Tier 3).

**To re-prioritize**: edit the tier number or reorder entries in `candidates.md`.  
**To add a new candidate**: add an entry in the correct Tier section with status `[OPEN]`.  
**To skip a candidate**: change its status to `[RULED_OUT]` with a reason.

### Agent loop (per candidate)

```
Main Claude (orchestrator)
  │
  ├─ Creates git branch: opt/<ID>-<slug> from current branch
  │
  ├─ Spawns EXECUTOR AGENT ─────────────────────────────────────┐
  │   • Reads candidate description from candidates.md           │
  │   • Reads architecture context from arch.md                 │
  │   • Reads all source files involved                         │
  │   • Writes numbered implementation plan                     │
  │   • Implements code changes (Edit tool)                     │
  │   • Commits to opt/<ID>-<slug>                              │
  │   • Returns: implementation report                          │
  │                                                  ◄──────────┘
  ├─ Spawns EVALUATOR AGENT ─────────────────────────────────────┐
  │   • Reads candidate description                              │
  │   • Reads changed files on the branch                       │
  │   • Generates PASS/FAIL checklist from candidate spec       │
  │   • Evaluates each criterion with code evidence             │
  │   • Returns: PASS or FAIL + actionable feedback             │
  │                                                  ◄──────────┘
  │
  ├─ If FAIL and iterations < 3:
  │   └─ Spawn FRESH EXECUTOR with original spec + evaluator feedback
  │       └─ Re-evaluate with FRESH EVALUATOR
  │
  ├─ Append result to .claude/state/experiments.jsonl
  ├─ Update candidate status in candidates.md
  └─ git checkout <base-branch>
```

### Evaluation includes build + test verification

The Executor runs `cargo build -p yellowstone-grpc-geyser` and `cargo test -p yellowstone-grpc-geyser`
before committing. The Evaluator independently re-runs both to confirm. A build or test failure is
an automatic FAIL verdict. The Evaluator additionally reasons qualitatively:
- Does the implementation match the stated fix?
- Are there logic errors, wrong types, missing cases, data races?
- Is the performance reasoning sound (right bottleneck targeted, right mechanism)?
- Is scope discipline maintained (no unrelated changes)?

### Branch lifecycle

Each candidate gets its own branch `opt/<ID>-<slug>` forked from the current base branch.
On PASS: branch is left as-is for you to review and merge manually.
On BLOCKED: branch contains the last iteration's attempt for inspection.
No PRs are opened automatically.

---

## `/project:implement` — How it works

The 4-agent loop for open-ended tasks:

```
/project:implement <task description>
  │
  ├─ [Inner loop, max 3 rounds]
  │   ├─ PLANNER AGENT
  │   │   • Reads arch.md + relevant source files
  │   │   • Decomposes task into ordered sub-tasks
  │   │   • Each sub-task: goal, files, steps, verification
  │   │
  │   └─ PLAN EVALUATOR AGENT
  │       • Reads listed files (verifies paths exist)
  │       • Generates 6-8 PASS/FAIL plan criteria
  │       • APPROVED → proceed | NEEDS_REVISION → Planner gets feedback, loops
  │
  ├─ Creates git branch: impl/T<N>-<slug>
  │
  ├─ IMPLEMENTOR AGENT
  │   • Executes sub-tasks in order
  │   • After each: verify (build/test), commit
  │   • Commit format: impl(T<N>): <sub-task> [k/total]
  │   • Returns: implementation report
  │
  ├─ IMPLEMENTATION EVALUATOR AGENT
  │   • Reads changed files + git diff
  │   • Re-runs cargo build + cargo test
  │   • Checks each sub-task goal + overall task goal
  │   • PASS → done | NEEDS_REVISION → feedback goes back to Planner
  │
  └─ [Outer loop, max 3 cycles] — on NEEDS_REVISION, full replanning with failure context
```

State is persisted to `.claude/state/tasks/<ID>.md` and `.claude/state/tasks.jsonl`.

---

## File structure

```
.claude/
├── commands/
│   ├── optimize.md          # /project:optimize — candidate-driven optimization loop
│   └── implement.md         # /project:implement — 4-agent loop for open-ended tasks
├── context/
│   ├── candidates.md        # source of truth for optimization candidates
│   └── arch.md              # architecture reference + ruled-out optimizations
├── scripts/
│   ├── status.sh            # session startup script
│   ├── parse_bench.py       # parse readings.md geyser bench JSON
│   ├── parse_race.py        # parse readings2.md Triton vs New race results
│   └── compare_bench.py     # diff two bench runs before/after
└── state/
    ├── experiments.jsonl    # append-only log of /project:optimize runs
    ├── tasks.jsonl          # append-only log of /project:implement runs
    ├── tasks/               # per-task plan files (T1.md, T2.md, ...)
    └── session.md           # ephemeral per-session notes

CLAUDE.md                    # auto-loaded context (short, always current)
HARNESS_README.md            # this file
```

---

## Candidate status reference

| Status | Meaning |
|--------|---------|
| `[OPEN]` | Not yet attempted. Eligible for selection. |
| `[TESTING]` | Currently running (set during active run). |
| `[PASS]` | Evaluator passed. Branch ready for review. |
| `[FAIL]` | Intermediate iteration failed (evaluator giving feedback). |
| `[BLOCKED]` | All 3 iterations failed. Needs human review. |
| `[RULED_OUT]` | Decided against (with reason). Never selected. |

---

## Viewing experiment history

```bash
# See all experiments
cat .claude/state/experiments.jsonl | python3 -c "
import json, sys
for line in sys.stdin:
    e = json.loads(line)
    print(f\"{e['ts'][:10]} {e['candidate_id']:4} {e['status']:8} iter={e['iterations']} branch={e['branch']}\")
"

# Check what's left to run
grep '\[OPEN\]' .claude/context/candidates.md
```

---

## Adding a new optimization candidate

Add a new entry to `.claude/context/candidates.md` in the appropriate Tier section:

```markdown
### C<N>: <title> [OPEN]
tier: <1|2|3>
experiment_ref:

**Bottleneck:** <what is slow and why, with specific file:line references>

**Fix:** <what to change and how, concisely>

**Key files:**
- `path/to/file.rs` — <what to change>

**Expected win:** <reasoning for ~100ms win at 500+ subs>

**Constraints:** <what not to change, safety invariants to preserve>
```

The harness generates the executor and evaluator instructions dynamically from this text.
The quality of your candidate description directly determines the quality of the implementation.

---

## Latency metrics

The `latency-metrics` feature flag (branch `agentic_optimization`, module `src/latency.rs`) measures
end-to-end dispatch latency in the `geyser_dispatch` spin-loop: from when a message is dequeued off
the geyser inbox to when the `CommitmentLevel::Processed` batch is handed to the broadcast channel.

### Enable at build time

```bash
# Build with latency metrics enabled
cargo build -p yellowstone-grpc-geyser --features latency-metrics

# Or in your plugin config, add the feature to the build command that produces the .so
```

When the flag is **absent** (the default), the module is not compiled in — zero overhead.

### What is measured

| Metric name | Type | Unit | Description |
|-------------|------|------|-------------|
| `geyser_dispatch_latency_us` | Histogram | microseconds | Time from `try_recv()` Ok → `broadcast_tx.send(Processed)`. Captures the oldest message in each batch. |

Buckets: 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000 µs.

### Expose the Prometheus endpoint

Set `prometheus.address` in your plugin config (e.g. `config.toml`):

```toml
[prometheus]
address = "0.0.0.0:9090"
```

The `/metrics` endpoint will include `geyser_dispatch_latency_us_bucket`, `_count`, and `_sum`.

### Read the results

```bash
# Raw prometheus scrape
curl -s http://localhost:9090/metrics | grep geyser_dispatch_latency

# Quick summary — p50 / p99 from the histogram buckets
curl -s http://localhost:9090/metrics | python3 - <<'EOF'
import sys, re

buckets, count, total = [], 0, 0.0
for line in sys.stdin:
    m = re.match(r'geyser_dispatch_latency_us_bucket\{le="([^"]+)"\} (\d+)', line)
    if m:
        le = float(m.group(1)) if m.group(1) != '+Inf' else float('inf')
        buckets.append((le, int(m.group(2))))
    m2 = re.match(r'geyser_dispatch_latency_us_count (\d+)', line)
    if m2: count = int(m2.group(1))
    m3 = re.match(r'geyser_dispatch_latency_us_sum ([0-9.]+)', line)
    if m3: total = float(m3.group(1))

if count:
    mean = total / count
    print(f"count={count}  mean={mean:.1f}µs")
    for pct, label in [(0.50, "p50"), (0.95, "p95"), (0.99, "p99")]:
        target = pct * count
        for le, cum in buckets:
            if cum >= target:
                print(f"  {label} ≤ {le:.0f}µs")
                break
EOF
```

### Grafana / Prometheus scrape config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: yellowstone_geyser
    static_configs:
      - targets: ['<your-node>:9090']
```

Useful PromQL queries:

```promql
# p99 dispatch latency over a 1-minute window
histogram_quantile(0.99, rate(geyser_dispatch_latency_us_bucket[1m]))

# Mean dispatch latency
rate(geyser_dispatch_latency_us_sum[1m]) / rate(geyser_dispatch_latency_us_count[1m])

# Request rate (batches/sec)
rate(geyser_dispatch_latency_us_count[1m])
```

### Plugging into optimization branches

The module is designed to be merged into any `opt/` branch without conflict — it only adds
cfg-gated statements around the existing broadcast sends. To activate it on any branch:

1. Rebase or cherry-pick commit `impl(T1): Wire latency call sites [3/3]` from branch
   `impl/T1-first-create-a-latency-measurement-mecha` onto your target branch.
2. Build with `--features latency-metrics`.
3. Compare p99 before/after your optimization using the PromQL queries above.

---

## Bench validation (after branch review)

Once you manually review a PASS branch and merge it:

```bash
# Run geyser bench before/after and save outputs
# Then compare:
python3 .claude/scripts/compare_bench.py readings_before.md readings_after.md --subs 700
python3 .claude/scripts/parse_race.py readings2.md
```

Update the experiment entry in `experiments.jsonl` with bench results (manual step).

---

## Current optimization roadmap

| Tier | ID | Title | Status |
|------|----|-------|--------|
| 1 | C2 | filter.get_updates() message-type fast-path | PASS |
| 1 | C3 | Lock-free per-subscriber queue | OPEN |
| 1 | C4 | Inline encode + direct write for fast subscribers | OPEN |
| 2 | C5 | Remove encoded_len() from hot path | OPEN |
| 2 | C6 | SmallVec for FilteredUpdates | OPEN |
| 2 | C7 | Skip broadcast for zero-subscriber message types | OPEN |
| 2 | C8 | Tokio worker thread tuning | OPEN |
| — | C1 | Shard broadcast by commitment level | RULED_OUT |
