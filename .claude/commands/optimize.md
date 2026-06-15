# /project:optimize — Optimization Harness Orchestrator

**Usage:** `/project:optimize [N]`  
Runs the next N best OPEN optimization candidates from `.claude/context/candidates.md`.  
Defaults to N=1 if not specified.

---

## STEP 0 — Parse N

Read `$ARGUMENTS`. If it is a positive integer, set N to that value. Otherwise N = 1.

---

## STEP 1 — Load state and pick candidates

1. Read `.claude/context/candidates.md` in full.
2. Read `.claude/state/experiments.jsonl` in full (may be empty).
   Parse each line as JSON: `{ "candidate_id": ..., "status": ..., ... }`.
   Build a set of candidate IDs that already have a `"status": "PASS"` or `"status": "BLOCKED"` entry.
3. From candidates.md, extract all candidates where the status in the header is `[OPEN]`.
   **Skip** any candidate whose ID appears in the already-completed set from experiments.jsonl.
4. Sort the remaining OPEN candidates by tier (ascending: 1 first, then 2, then 3).
   Within the same tier, preserve document order.
5. Take the first N candidates from this sorted list.
6. Print to the user:
   ```
   Running N optimization(s):
     1. <ID>: <title> (Tier <tier>)
     2. <ID>: <title> (Tier <tier>)
     ...
   Base branch: <current git branch>
   ```

---

## STEP 2 — For each selected candidate, run the executor/evaluator loop

Process candidates **sequentially** (one at a time). For each candidate:

### 2a. Record the base branch
Run `git rev-parse --abbrev-ref HEAD` to get the base branch name. Store it.
Verify the working tree is clean (`git status`). If there are uncommitted changes, STOP and tell
the user — do not proceed while there are uncommitted changes on the base branch.

### 2b. Create the feature branch  ← BRANCHING STRATEGY (mandatory)
Every optimization MUST land on its own `opt/<ID>-<slug>` branch. Never commit directly to the
base branch. This is non-negotiable — it keeps each change reviewable and revertable in isolation.

Derive a branch slug from the candidate title: lowercase, replace spaces and special chars with `-`,
keep alphanumeric and `-` only. Max 40 chars.
Create the branch: `git checkout -b opt/<ID>-<slug>`
After this command you are on the feature branch. All subsequent commits go here.

### 2c. Run EXECUTOR AGENT (iteration 1)

Spawn an Agent with the following prompt (fill in `<...>` placeholders from candidates.md):

```
SYSTEM ROLE: You are the Executor Agent for the yellowstone-grpc-geyser latency optimization project.
Your sole job is to implement ONE specific code change, commit it, and return a detailed implementation report.
You are working on branch: opt/<ID>-<slug>

═══════════════════════════════════════════════════════════
CANDIDATE TO IMPLEMENT
═══════════════════════════════════════════════════════════
<paste the full candidate section from candidates.md verbatim, including Bottleneck, Fix, Key files, Expected win, Constraints>

═══════════════════════════════════════════════════════════
ARCHITECTURE CONTEXT (read before writing any code)
═══════════════════════════════════════════════════════════
<paste .claude/context/arch.md verbatim>

═══════════════════════════════════════════════════════════
YOUR TASK — follow these steps IN ORDER
═══════════════════════════════════════════════════════════

STEP E1 — READ all files listed in "Key files" above, plus any files they import that are directly relevant to your change. Do not skip this step.

STEP E2 — PLAN. Before touching any file, write a numbered plan:
  - List every file you will change
  - For each file: list every function/line you will modify and exactly what the change is
  - State the precise mechanism by which this achieves the ~100ms latency win
  - List what you will NOT change (explicit scope boundary)
  This plan is your contract. The evaluator will check your implementation against it.

STEP E3 — IMPLEMENT. Make the code changes using the Edit tool.
  Rules:
  - Change ONLY the files listed in "Key files" and any transitive imports that MUST change
  - Do not refactor, rename, or reformat anything not required for the optimization
  - Do not add comments explaining what the code does — only add comments for non-obvious WHY
  - If you discover the candidate description has an error (wrong line number, wrong function name), note the correction and implement the corrected version

STEP E4 — BUILD AND TEST. Before committing, verify the code compiles and tests pass:
  Run: cargo build -p yellowstone-grpc-geyser 2>&1
  Run: cargo test -p yellowstone-grpc-geyser 2>&1
  If build fails: fix the errors and retry — do NOT commit broken code.
  If tests fail: fix the failures and retry — do NOT commit failing tests.
  Record the build/test result in your report.

STEP E5 — COMMIT. Run:
  git add <list each changed file explicitly — do NOT use git add .>
  git commit -m "opt(<ID>): <title>

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"

STEP E6 — REPORT. Write a structured report with these exact sections:

## IMPLEMENTATION REPORT

### Plan executed
<numbered list of what you changed, one item per function/file>

### Files changed
<file path> : <what changed in one sentence>
...

### Build and test result
Build: PASS or FAIL (paste first error line if FAIL)
Tests: PASS (N/N) or FAIL (paste failing test name)

### Latency win mechanism
<2-3 sentences: exactly what bottleneck is removed, why ~100ms reduction is expected at 500+ subs>

### Risks and tradeoffs
<any correctness risks, behavioral changes, or edge cases to watch>

### What was NOT changed
<explicit list>
```

After the Agent completes, capture its full response as `executor_report_1`.

### 2d. Run EVALUATOR AGENT (iteration 1)

Get the git diff of the changes: `git diff <base_branch>...opt/<ID>-<slug>`

Spawn an Agent with the following prompt:

```
SYSTEM ROLE: You are the Evaluator Agent for the yellowstone-grpc-geyser latency optimization project.
Your sole job is to qualitatively assess whether the Executor's implementation is correct and likely
to achieve the intended ~100ms latency win. You CANNOT compile or run the code.

═══════════════════════════════════════════════════════════
CANDIDATE BEING EVALUATED
═══════════════════════════════════════════════════════════
<paste the full candidate section from candidates.md verbatim>

═══════════════════════════════════════════════════════════
EXECUTOR'S IMPLEMENTATION REPORT
═══════════════════════════════════════════════════════════
<paste executor_report_1 verbatim>

═══════════════════════════════════════════════════════════
YOUR TASK
═══════════════════════════════════════════════════════════

STEP V1 — READ the changed files on branch `opt/<ID>-<slug>`.
  For each file listed in the executor's "Files changed" section, read it in full.
  Also read the git diff: run `git diff <base_branch>...opt/<ID>-<slug>`

STEP V2 — CHECK BUILD. Verify the executor's reported build/test result:
  - If executor reported Build: FAIL or Tests: FAIL — this is an automatic VERDICT: FAIL.
    Skip to STEP V5 with feedback: "Fix the build/test errors reported in your own report."
  - If executor reported Build: PASS and Tests: PASS — run `cargo build -p yellowstone-grpc-geyser`
    and `cargo test -p yellowstone-grpc-geyser` yourself to independently verify.
    If the build or tests fail for you, treat it as VERDICT: FAIL.

STEP V3 — GENERATE a checklist. Based on the candidate's Bottleneck, Fix, Constraints, and
Expected win, generate 6-10 specific PASS/FAIL criteria. Each criterion must be:
  - Specific enough to have a binary answer from reading the code
  - Named [B1] for build check, [E1]-[En] for correctness checks, [P1]-[Pn] for performance checks, [Q1]-[Qn] for quality

STEP V4 — EVALUATE each criterion. For each one:
  [PASS/FAIL] <ID> <name>: <one sentence of evidence from the actual code>

STEP V5 — VERDICT. Write one of:
  VERDICT: PASS   (if ALL criteria are PASS)
  VERDICT: FAIL   (if ANY criterion is FAIL)

STEP V6 — If VERDICT is FAIL, write:
  ## FEEDBACK FOR EXECUTOR (ITERATION 2)
  <Numbered list of specific, actionable fixes. For each FAIL criterion: what exactly needs to
  change, which file and function, and why the current implementation doesn't meet it.
  Be precise — the next Executor spawned will receive ONLY this feedback, not your full checklist.>

Output format:
## EVALUATION REPORT — <ID> Iteration 1

### Criteria
[PASS/FAIL] B1 build: ...
[PASS/FAIL] E1 ...: ...
[PASS/FAIL] P1 ...: ...
...

### Verdict
VERDICT: PASS or FAIL

### Feedback for Executor (if FAIL)
...
```

Capture the evaluator's full response as `evaluator_report_1`. Extract `verdict_1` (PASS or FAIL).

### 2e. Iteration loop (up to 2 more iterations if needed)

If `verdict_1 == PASS`, skip to step 2f.

If `verdict_1 == FAIL` and we've done fewer than 3 total evaluations:

Spawn a **fresh Executor Agent** (new agent, no prior context) with this prompt:

```
SYSTEM ROLE: You are the Executor Agent for the yellowstone-grpc-geyser latency optimization project.
You are fixing a PREVIOUS FAILED IMPLEMENTATION. You are working on branch: opt/<ID>-<slug>

═══════════════════════════════════════════════════════════
CANDIDATE TO IMPLEMENT
═══════════════════════════════════════════════════════════
<paste full candidate section from candidates.md>

═══════════════════════════════════════════════════════════
ARCHITECTURE CONTEXT
═══════════════════════════════════════════════════════════
<paste .claude/context/arch.md>

═══════════════════════════════════════════════════════════
PREVIOUS ATTEMPT FAILED — EVALUATOR FEEDBACK
═══════════════════════════════════════════════════════════
<paste evaluator's "Feedback for Executor" section verbatim>

═══════════════════════════════════════════════════════════
YOUR TASK
═══════════════════════════════════════════════════════════
The branch already has the failed implementation. Read the current state of the key files first.
Then make the targeted fixes described in the evaluator feedback above.
Follow the same STEP E1 → E5 procedure as before.
Your commit message should be:
  "opt(<ID>): <title> — revision <N>

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

Run the Evaluator again (same prompt structure, updated with new diff and new executor report).
Repeat up to iteration 3 total.

If after 3 evaluations the verdict is still FAIL, set final status = BLOCKED.

### 2f. Record result to state

After the loop, append a JSON line to `.claude/state/experiments.jsonl`:
```json
{"ts": "<ISO timestamp>", "candidate_id": "<ID>", "candidate_title": "<title>", "branch": "opt/<ID>-<slug>", "base_branch": "<base_branch>", "iterations": <1|2|3>, "status": "<PASS|BLOCKED>", "evaluator_summary": "<one sentence from evaluator verdict section>", "executor_plan": "<one sentence from executor plan>"}
```

### 2g. Update candidates.md status

In `.claude/context/candidates.md`, find the candidate's status in its header line.
Change `[OPEN]` to `[PASS]`, `[BLOCKED]`, etc.
Also set `experiment_ref:` to the ISO timestamp from the log entry.

### 2h. Switch back to base branch

`git checkout <base_branch>`

Print a summary line for this candidate:
```
✅ <ID> PASS — branch opt/<ID>-<slug> ready for review
```
or
```
❌ <ID> BLOCKED after <N> iterations — branch opt/<ID>-<slug> contains last attempt
```

---

## STEP 3 — Final summary

After all N candidates have been processed, print:

```
═══════ OPTIMIZATION RUN COMPLETE ═══════
Ran: N candidate(s)
Passed: <count>
Blocked: <count>

Results:
  ✅ <ID>: <title> → branch opt/<ID>-<slug>
  ❌ <ID>: <title> → BLOCKED (see .claude/state/experiments.jsonl)

Next OPEN candidates remaining:
  <list top 3 remaining OPEN by tier>

To continue: /project:optimize <M>
```
