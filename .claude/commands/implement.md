# /project:implement — 4-Agent Task Implementation Harness

**Usage:** `/project:implement <task description>`

Runs a full Planner → Plan Evaluator → Implementor → Implementation Evaluator loop for any
open-ended implementation task. The Planner and Plan Evaluator iterate until the plan is approved,
then the Implementor executes it. If the Implementation Evaluator finds the result short of the goal,
the Planner is re-engaged with failure context for another full cycle.

---

## STEP 0 — Parse task description

Read `$ARGUMENTS`. If empty or whitespace-only, print:
```
Usage: /project:implement <task description>
Example: /project:implement decouple geyser_dispatch from the encoder
```
and stop.

Store `task_description = $ARGUMENTS`.

---

## STEP 1 — Initialize

1. **Assign task ID:**
   - If `.claude/state/tasks.jsonl` exists, count lines (each = one prior task) → `N = line_count + 1`
   - If file does not exist, `N = 1`
   - `task_id = "T<N>"`

2. **Record base branch:**
   `base_branch = git rev-parse --abbrev-ref HEAD`

3. **Verify working tree is clean:**
   Run `git status`. If there are uncommitted changes, print:
   ```
   ⚠ Working tree is not clean. Commit or stash changes before running /project:implement.
   ```
   and stop.

4. **Derive branch slug:**
   Lowercase `task_description`. Replace spaces and non-alphanumeric chars with `-`.
   Trim leading/trailing `-`. Truncate to 40 chars.
   `branch_name = "impl/<task_id>-<slug>"`

5. **Print header:**
   ```
   ════════════════════════════════════════
   Task <task_id>: <task_description>
   Base branch:   <base_branch>
   Feature branch: <branch_name>
   ════════════════════════════════════════
   ```

6. **Initialize state variables:**
   - `outer_cycle = 0`
   - `impl_feedback = ""` (empty; filled if implementation fails)
   - `final_status = ""`
   - `planning_rounds_per_cycle = []`

---

## STEP 2 — Outer loop (max 3 full cycles)

Repeat up to 3 times. Each cycle = inner planning loop + one implementation attempt.

Read `.claude/context/arch.md` in full. Store as `arch_context`.

### STEP 2a — Inner planning loop (max 3 rounds per cycle)

`outer_cycle += 1`
`inner_round = 0`
`plan_approved = false`

Repeat up to 3 times (or until approved):

`inner_round += 1`

#### Spawn PLANNER AGENT

```
SYSTEM ROLE: You are the Planner Agent for the yellowstone-grpc-geyser latency optimization project.
Your job is to read the codebase and decompose the given task into concrete, ordered sub-tasks
that an Implementor agent can execute one-by-one without ambiguity.

══════════════════════════════════════════════════════════
TASK
══════════════════════════════════════════════════════════
<task_description>
Task ID: <task_id> | Planning cycle <outer_cycle>, Round <inner_round>

══════════════════════════════════════════════════════════
ARCHITECTURE CONTEXT
══════════════════════════════════════════════════════════
<arch_context verbatim>

══════════════════════════════════════════════════════════
IMPLEMENTATION FEEDBACK FROM PREVIOUS CYCLE
══════════════════════════════════════════════════════════
<impl_feedback if non-empty, otherwise "None — this is the first planning cycle.">

══════════════════════════════════════════════════════════
YOUR STEPS — follow IN ORDER
══════════════════════════════════════════════════════════

STEP P1 — READ. Identify which files are relevant to this task using the architecture context above.
  Read at least 3–5 source files before writing anything. Do not skip this step.
  Reading first prevents wrong file paths, wrong function names, and broken plans.

STEP P2 — PLAN. Write a plan with this exact structure:

## TASK PLAN — <task_id> Cycle <outer_cycle> Round <inner_round>

### Sub-task 1: <title>
**Goal:** <one sentence — what this sub-task achieves>
**Files:**
- `<relative/path/to/file.rs>` — <what specifically changes here>
**Steps:**
1. <concrete step>
2. <concrete step>
...
**Verification:** <exactly what to run or check to confirm this sub-task is complete>
**Scope boundary:** <what is explicitly NOT being done in this sub-task>

### Sub-task 2: <title>
[same structure]

[2–6 sub-tasks total; more is rarely better than fewer, clearer ones]

### Integration check
<how the sub-tasks compose — name ordering dependencies between sub-tasks>

### What will NOT be changed (overall)
<explicit list of things the implementation will leave untouched>

### Risks
<known uncertainties, edge cases, or things that might need re-evaluation during implementation>

Rules:
- Each sub-task must be independently verifiable (build or test passes after it alone)
- File paths must be relative to the repo root and must actually exist
- Steps must be concrete enough that someone unfamiliar with the codebase can execute them
- Do NOT include implementation code in the plan — describe the change, not the code
```

Capture planner's full response as `planner_output`.

#### Spawn PLAN EVALUATOR AGENT

```
SYSTEM ROLE: You are the Plan Evaluator for the yellowstone-grpc-geyser latency optimization project.
Assess whether the Planner's plan is complete, correct, and safe to hand to an Implementor.
You cannot write code — only evaluate the plan.

══════════════════════════════════════════════════════════
TASK
══════════════════════════════════════════════════════════
<task_description>
Task ID: <task_id>

══════════════════════════════════════════════════════════
PLANNER'S PLAN (Cycle <outer_cycle>, Round <inner_round>)
══════════════════════════════════════════════════════════
<planner_output verbatim>

══════════════════════════════════════════════════════════
YOUR STEPS — follow IN ORDER
══════════════════════════════════════════════════════════

STEP V1 — READ. For each file listed in the plan's "Files" sections:
  - Verify the file exists
  - Read enough of it (50–100 lines around the relevant area) to confirm the plan is targeting
    the right location and the described change makes sense there
  Do not skip this step — a plan pointing to wrong files or non-existent functions must be REJECTED.

STEP V2 — GENERATE 6–8 binary PASS/FAIL criteria derived from the task and plan:

  [C1] Completeness: the sub-tasks together fully achieve the task goal (nothing major omitted)
  [C2] File accuracy: every listed file exists; targeted functions/lines are in the right place
  [C3] Order: sub-tasks have no unresolved dependency ordering issues
  [C4] Architecture alignment: approach is consistent with patterns in arch.md (no ruled-out approaches)
  [C5] Scope discipline: no unnecessary changes to unrelated code; focused on the task
  [C6] Verifiability: each sub-task's Verification step actually tests whether the goal was achieved
  [C7] Safety: each sub-task leaves the build passing; no step produces a temporarily broken state
  [C8+] Add 1–2 task-specific criteria for anything unique to this particular task

STEP V3 — EVALUATE each criterion:
  [PASS/FAIL] <ID> <name>: <one sentence of evidence from the actual plan + files you read>

STEP V4 — VERDICT:
  VERDICT: APPROVED   (if ALL criteria are PASS)
  VERDICT: NEEDS_REVISION   (if ANY criterion is FAIL)

STEP V5 — If VERDICT is NEEDS_REVISION, write:
## FEEDBACK FOR PLANNER
<Numbered list. For each FAIL criterion: what exactly is wrong, what needs to change, and why.
Be specific — the Planner will receive ONLY this feedback in the next round.>

Output format:
## PLAN EVALUATION REPORT — <task_id> Cycle <outer_cycle> Round <inner_round>

### Criteria
[PASS/FAIL] C1 completeness: ...
[PASS/FAIL] C2 file-accuracy: ...
...

### Verdict
VERDICT: APPROVED or NEEDS_REVISION

### Feedback for Planner (if NEEDS_REVISION)
1. ...
2. ...
```

Capture evaluator's full response. Extract `plan_verdict` (APPROVED or NEEDS_REVISION).

**Decision:**
- If `plan_verdict == APPROVED`:
  - Set `finalized_plan = planner_output`
  - Set `plan_approved = true`
  - Record `planning_rounds_per_cycle[outer_cycle] = inner_round`
  - Break inner loop

- If `plan_verdict == NEEDS_REVISION` and `inner_round < 3`:
  - Extract "Feedback for Planner" section from evaluator response
  - Print: `  ↺ Plan round <inner_round> rejected — respawning Planner with feedback`
  - Next Planner invocation includes the feedback in its prompt (append to the prompt above:)
    ```
    PLAN EVALUATOR FEEDBACK FROM ROUND <inner_round>:
    <feedback verbatim>
    ```
  - Continue inner loop

- If `plan_verdict == NEEDS_REVISION` after 3 inner rounds:
  - Print: `❌ <task_id> PLANNING_BLOCKED — plan not approved after 3 rounds`
  - Set `final_status = "PLANNING_BLOCKED"`
  - Skip to STEP 3

---

### STEP 2b — Create feature branch

```
git checkout -b <branch_name>
```

Print: `  Branch <branch_name> created.`

---

### STEP 2c — Spawn IMPLEMENTOR AGENT

```
SYSTEM ROLE: You are the Implementor Agent for the yellowstone-grpc-geyser latency optimization project.
Execute each sub-task in order, verify it, commit it, and move to the next.
You are working on branch: <branch_name>

══════════════════════════════════════════════════════════
TASK
══════════════════════════════════════════════════════════
<task_description>
Task ID: <task_id>

══════════════════════════════════════════════════════════
ARCHITECTURE CONTEXT
══════════════════════════════════════════════════════════
<arch_context verbatim>

══════════════════════════════════════════════════════════
FINALIZED PLAN
══════════════════════════════════════════════════════════
<finalized_plan verbatim>

══════════════════════════════════════════════════════════
YOUR STEPS — follow IN ORDER
══════════════════════════════════════════════════════════

For EACH sub-task listed in the plan, in order, execute STEP E1 → E4 before moving on:

STEP E1 — READ. Read every file listed in this sub-task's "Files" section.
  Understand the current state before making any change.

STEP E2 — IMPLEMENT. Make exactly the changes described in the sub-task's "Steps".
  Rules:
  - Only change the files listed in this sub-task
  - Do not reformat, rename, or clean up unrelated code
  - Do not add comments explaining what the code does — only "why" comments where non-obvious
  - If you discover the plan has an error (wrong line number, wrong function name), note the
    correction clearly and implement the corrected version

STEP E3 — VERIFY. Run the sub-task's stated Verification step.
  If it fails: fix the issue and retry. Do NOT proceed to the next sub-task with a failing build.
  Record the result.

STEP E4 — COMMIT.
  git add <list each changed file explicitly — do NOT use git add .>
  git commit -m "impl(<task_id>): <sub-task title> [<k>/<total_sub_tasks>]

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"

After ALL sub-tasks are committed:

STEP E5 — FULL VERIFY. Run:
  cargo build -p yellowstone-grpc-geyser 2>&1
  cargo test -p yellowstone-grpc-geyser 2>&1
  Record results.

STEP E6 — REPORT. Write this exact structure:

## IMPLEMENTATION REPORT — <task_id>

### Sub-task results
**Sub-task 1: <title>**
Files changed: <list>
Build after this sub-task: PASS or FAIL (paste first error line if FAIL)
Tests after this sub-task: PASS (N/N) or FAIL (paste failing test name)
Notes: <anything unexpected or corrected from plan>

[repeat for each sub-task]

### Overall build/test
Build: PASS or FAIL
Tests: PASS (N/N) or FAIL

### Task goal achievement
<2–3 sentences: does the implementation achieve the stated task goal? Be honest about partial success.>

### What was NOT changed
<explicit list>
```

Capture implementor's full response as `implementor_report`.

---

### STEP 2d — Spawn IMPLEMENTATION EVALUATOR AGENT

Get full git diff:
```
git diff <base_branch>...<branch_name>
```
Store as `git_diff`.

```
SYSTEM ROLE: You are the Implementation Evaluator for the yellowstone-grpc-geyser latency
optimization project. Assess whether the implementation achieves the original task goal.
You have full code and build access.

══════════════════════════════════════════════════════════
TASK
══════════════════════════════════════════════════════════
<task_description>
Task ID: <task_id>

══════════════════════════════════════════════════════════
FINALIZED PLAN
══════════════════════════════════════════════════════════
<finalized_plan verbatim>

══════════════════════════════════════════════════════════
IMPLEMENTOR'S REPORT
══════════════════════════════════════════════════════════
<implementor_report verbatim>

══════════════════════════════════════════════════════════
GIT DIFF (base..<branch_name>)
══════════════════════════════════════════════════════════
<git_diff>

══════════════════════════════════════════════════════════
YOUR STEPS — follow IN ORDER
══════════════════════════════════════════════════════════

STEP V1 — READ AND BUILD.
  Read each file listed in the implementor's "Files changed" sections.
  Run independently:
    cargo build -p yellowstone-grpc-geyser 2>&1
    cargo test -p yellowstone-grpc-geyser 2>&1
  If either fails: VERDICT is NEEDS_REVISION regardless of other criteria.

STEP V2 — GENERATE 6–10 binary PASS/FAIL criteria:
  [B1] build: cargo build passes independently
  [T1] tests: all tests pass independently
  For each sub-task k:
    [E<k>] sub-task-<k>-<title>: the sub-task's stated goal is achieved in the code
  [I1] integration: sub-tasks compose correctly end-to-end (no interface mismatches)
  [G1] task-goal: the overall task description is satisfied by the implementation
  [Q1] scope: no unintended side effects, no unrelated changes included

STEP V3 — EVALUATE each criterion:
  [PASS/FAIL] <ID> <name>: <one sentence of evidence from the actual code or diff>

STEP V4 — VERDICT:
  VERDICT: PASS   (if ALL criteria are PASS)
  VERDICT: NEEDS_REVISION   (if ANY criterion is FAIL)

STEP V5 — If VERDICT is NEEDS_REVISION, write:
## FEEDBACK FOR NEXT PLANNING CYCLE
<Numbered list for the Planner.
For each FAIL criterion: what was attempted, what is wrong, what needs to change.
Include specific file:line references from the diff where relevant.
Be precise — this feedback drives the ENTIRE next planning cycle.>

Output format:
## IMPLEMENTATION EVALUATION REPORT — <task_id> Cycle <outer_cycle>

### Criteria
[PASS/FAIL] B1 build: ...
[PASS/FAIL] T1 tests: ...
[PASS/FAIL] E1 ...: ...
...

### Verdict
VERDICT: PASS or NEEDS_REVISION

### Feedback for Next Planning Cycle (if NEEDS_REVISION)
1. ...
2. ...
```

Capture evaluator's full response. Extract `impl_verdict` (PASS or NEEDS_REVISION).

---

### STEP 2e — Outer loop decision

- If `impl_verdict == PASS`:
  - Set `final_status = "PASS"`
  - Print: `  ✅ Implementation verified — <task_id> PASS`
  - Break outer loop

- If `impl_verdict == NEEDS_REVISION` and `outer_cycle < 3`:
  - Extract "Feedback for Next Planning Cycle" from evaluator response
  - Set `impl_feedback = <that feedback>`
  - Print: `  ↺ Implementation cycle <outer_cycle> failed — replanning with feedback`
  - `git checkout <base_branch>` (leave branch as-is for inspection)
  - Continue outer loop (next cycle's Planner will receive `impl_feedback`)

- If `impl_verdict == NEEDS_REVISION` after 3 outer cycles:
  - Set `final_status = "BLOCKED"`
  - Print: `❌ <task_id> BLOCKED after 3 full cycles`

---

## STEP 3 — Record state

### 3a. Write per-task plan file

Create directory path `.claude/state/tasks/` if it doesn't exist.
Write `.claude/state/tasks/<task_id>.md`:

```markdown
# Task <task_id>: <task_description>
Status: <final_status>
Timestamp: <ISO8601 timestamp>
Branch: <branch_name>
Base branch: <base_branch>
Planning cycles completed: <outer_cycle>

## Original task
<task_description>

## Finalized plan
<finalized_plan verbatim, or "Plan never approved (PLANNING_BLOCKED)" if applicable>

## Outcome
<one-sentence summary from the last evaluator verdict section>
```

### 3b. Append to tasks.jsonl

Append one JSON line to `.claude/state/tasks.jsonl`:
```json
{"ts": "<ISO8601>", "task_id": "<task_id>", "task": "<task_description>", "branch": "<branch_name>", "base_branch": "<base_branch>", "planning_cycles": <outer_cycle>, "planning_rounds_per_cycle": <planning_rounds_per_cycle array>, "status": "<final_status>", "evaluator_summary": "<one sentence from last evaluator verdict>", "planner_summary": "<one sentence from finalized plan's Integration check>"}
```

### 3c. Return to base branch

```
git checkout <base_branch>
```

---

## STEP 4 — Print summary

For PASS:
```
✅ <task_id> PASS — branch <branch_name> ready for review
   Planning cycles: <outer_cycle>  |  Planning rounds: <planning_rounds_per_cycle>
   Sub-tasks implemented: <count>  |  Commits: <count>
```

For BLOCKED:
```
❌ <task_id> BLOCKED after <outer_cycle> cycle(s)
   Last attempt: branch <branch_name>
   See .claude/state/tasks/<task_id>.md for full planning and evaluation history
```

For PLANNING_BLOCKED:
```
❌ <task_id> PLANNING_BLOCKED — plan not approved after 3 rounds in cycle <outer_cycle>
   No implementation was attempted. Check the Plan Evaluator's final feedback above.
```

---

## Agent summary reference

| Agent | Receives | Outputs | Max spawns |
|-------|----------|---------|------------|
| Planner | task + arch.md + impl_feedback | Structured sub-task plan | 3 per cycle × 3 cycles = 9 |
| Plan Evaluator | task + planner output | APPROVED / NEEDS_REVISION + feedback | Same as Planner |
| Implementor | task + arch.md + finalized plan | Implementation report + commits | 1 per cycle = 3 |
| Impl Evaluator | task + plan + report + git diff | PASS / NEEDS_REVISION + feedback | 1 per cycle = 3 |

Max agent spawns: 9 + 9 + 3 + 3 = 24 across all cycles.
Typical case (1 planning round, 1 cycle): 4 agents total.
