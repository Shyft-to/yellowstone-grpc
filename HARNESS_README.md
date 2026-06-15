# yellowstone-grpc Optimization Harness

AI-driven optimization harness for reducing geyser fan-out latency. Each optimization runs in
an Executor → Evaluator agent loop on an isolated git branch, with results persisted to a state log.

---

## Quick start

```bash
# See current state before starting
bash .claude/scripts/status.sh

# Run the next 1 best optimization
/project:optimize

# Run the next 3 best optimizations sequentially
/project:optimize 3
```

That's it. The harness picks the next OPEN candidates by tier, implements them, evaluates
qualitatively, retries on failure (up to 3 iterations), and logs everything.

---

## How it works

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

## File structure

```
.claude/
├── commands/
│   └── optimize.md          # /project:optimize command (the harness)
├── context/
│   ├── candidates.md        # source of truth for optimization candidates
│   └── arch.md              # architecture reference + ruled-out optimizations
├── scripts/
│   ├── status.sh            # session startup script
│   ├── parse_bench.py       # parse readings.md geyser bench JSON
│   ├── parse_race.py        # parse readings2.md Triton vs New race results
│   └── compare_bench.py     # diff two bench runs before/after
└── state/
    ├── experiments.jsonl    # append-only experiment log (one JSON per line)
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
