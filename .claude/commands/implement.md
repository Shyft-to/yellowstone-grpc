---
description: Multi-agent harness (planner → plan-evaluator → executor → validator) for implementing a code change end-to-end, with resumable state and small per-milestone commits on a dedicated feature branch.
argument-hint: <task description> | resume | status
---

# /implement — Multi-Agent Implementation Harness

This command only runs when explicitly invoked. Do not apply this workflow to any other request in this session — normal requests are handled normally.

Standing objective for everything this harness produces: **reduce end-to-end latency for delivering blockchain data to subscribed gRPC clients, without sacrificing correctness.** Keep this in view even when `$ARGUMENTS` describes a narrow task.

You (the main agent) are the **orchestrator**. You do not write code or judge code yourself in this workflow — you drive four specialized subagents (`planner`, `plan-evaluator`, `executor`, `validator`, defined in `.claude/agents/`) via the Agent tool, and you own: branch/commit mechanics, the run log, and the loop control between agents.

---

## STEP 0 — Parse `$ARGUMENTS`

- If `$ARGUMENTS` is exactly `status` (case-insensitive): run STEP 1's discovery only, report what you find, and stop. Do not start or resume anything.
- If `$ARGUMENTS` is exactly `resume` (case-insensitive): you must resume an existing run. If discovery in STEP 1 finds none, tell the user there is nothing to resume and stop.
- Otherwise: `$ARGUMENTS` is the task description for a new run (unless STEP 1 finds a conflicting in-progress run, handled there).

## STEP 1 — Discover in-progress runs (git-native, no side-channel state file)

The run log (`.claude/state/run_log.md`) is committed **on its feature branch only**, never on the base branch. This makes discovery fully derivable from git — no separate pointer file to go stale.

1. Run `git branch --list 'implement/*'` (local) and `git branch -r --list 'origin/implement/*'` (remote) to find candidate feature branches.
2. For each candidate, run `git show <branch>:.claude/state/run_log.md` (works without checking out the branch) and read its `Status` field from the Meta section.
3. Collect all branches whose run log status is not `DONE`.

Now branch on `$ARGUMENTS`:

- **`status`**: report each in-progress branch with its status, current task, and last-updated time. Stop.
- **`resume`**:
  - Zero in-progress runs → tell the user, stop.
  - One in-progress run → proceed to STEP 1a with that branch.
  - More than one → use AskUserQuestion to have the user pick which branch to resume, then proceed to STEP 1a.
- **New task description**:
  - Zero in-progress runs → proceed to STEP 2 (fresh start).
  - One or more in-progress runs → use AskUserQuestion: resume the existing run, or start a new one alongside it (a second `/implement` task can live on its own branch concurrently — that's fine, just confirm the user actually wants two in flight). Do not silently abandon existing work.

### STEP 1a — Resume mechanics

1. `git status` — if the working tree is dirty, stop and tell the user (do not stash or discard automatically; this may be unrelated in-progress work).
2. `git checkout <feature-branch>` (create a local tracking branch from the remote one if it only exists on origin).
3. Read `.claude/state/run_log.md` in full.
4. Resume from `Status`:
   - `PLANNING` → go to STEP 3, continuing the planning history already recorded (don't restart from round 1).
   - `EXECUTING` → go to STEP 4, starting at the first task in the Task Progress table whose status is not `DONE`. If a task shows `IN_PROGRESS` with prior attempts and feedback recorded, pass that feedback to the executor's next attempt.
   - `BLOCKED` → report the blocker recorded in the log to the user and ask how to proceed (retry, skip task, abandon run) — do not silently retry a blocked task.
   - `DONE` → tell the user this run already completed, report the commit list, stop.

## STEP 2 — Initialize a new run

1. `git status` — if dirty, stop and tell the user.
2. Record `base_branch` = current branch (`git rev-parse --abbrev-ref HEAD`).
3. Derive a `slug` from the task description: lowercase, alphanumeric + hyphens only, max 50 chars, meaningful (not just "task-1").
4. `git checkout -b implement/<slug>`.
5. Write `.claude/state/run_log.md` using the schema below with `Status: PLANNING` and an empty planning history.
6. `git add .claude/state/run_log.md && git commit -m "implement(<slug>): start run"`.

### run_log.md schema

```markdown
# Implementation Run Log

## Meta
- Task: <verbatim original request>
- Base branch: <branch>
- Feature branch: implement/<slug>
- Status: PLANNING | EXECUTING | DONE | BLOCKED
- Started: <date>
- Last updated: <date>

## Approved Plan
<filled in once the plan-evaluator approves — the "Final task list" verbatim>

## Planning History
### Round 1
- Planner output: <full plan text, or a faithful summary if very long>
- Evaluator verdict: APPROVED | REJECTED
- Evaluator feedback: <full feedback text>

### Round 2
...

## Task Progress
| # | Task | Status | Attempts | Last Verdict | Commit |
|---|------|--------|----------|---------------|--------|
| 1 | <name> | PENDING \| IN_PROGRESS \| DONE \| BLOCKED | 0 | - | - |

## Blockers
<none, or a description of what's blocking and what was tried>
```

Keep this file updated after every meaningful step (round of planning, executor attempt, validator verdict, commit) — this is the sole resumability mechanism, so treat "did I just update the log" as part of finishing any step, not an afterthought.

## STEP 3 — Planning loop (planner ↔ plan-evaluator)

Cap: 4 rounds. If round 4 is still `REJECTED`, stop the loop and use AskUserQuestion to show the user the current plan and the evaluator's outstanding objection, and ask how to proceed (accept plan as-is, provide direction yourself, abandon run).

Each round:

1. Spawn `planner` (Agent tool, `subagent_type: "planner"`) with: the original task description, the full current plan + feedback history so far (if round > 1), and enough repo context to ground it (point it at relevant files/dirs if you already know them from the task description — it can also explore on its own).
2. Update `run_log.md` planning history with the planner's output.
3. Spawn `plan-evaluator` (`subagent_type: "plan-evaluator"`) with: the original task description and the planner's latest plan.
4. Update `run_log.md` planning history with the verdict + feedback.
5. Commit the run_log update: `git add .claude/state/run_log.md && git commit -m "implement(<slug>): planning round N"`.
6. If `APPROVED`: copy the evaluator's "Final task list" into `## Approved Plan`, populate the `Task Progress` table (one row per task, `PENDING`), set `Status: EXECUTING`, commit, proceed to STEP 4.
7. If `REJECTED` and rounds remain: go to round N+1, passing the feedback back into the planner prompt.

## STEP 4 — Task execution loop (executor ↔ validator), one task at a time, in order

For each task in the `Task Progress` table with status not `DONE`, in order (do not skip ahead — a later task may depend on an earlier one's output):

Cap: 3 executor/validator attempts per task. If attempt 3 is still `REJECTED`, set that task's status to `BLOCKED`, set `Status: BLOCKED` in Meta, write the blocker detail into `## Blockers`, commit the run_log update, stop the run, and report to the user — do not proceed to later tasks on top of a blocked one.

Each attempt:

1. Mark task `IN_PROGRESS`, increment `Attempts`, commit the run_log update.
2. Spawn `executor` (`subagent_type: "executor"`) with: this one task's full spec from the approved plan, and — if this is a retry — the previous attempt's diff and the validator's feedback verbatim.
3. Spawn `validator` (`subagent_type: "validator"`) with: this task's spec, the executor's report, and instruct it to inspect the actual working-tree diff itself (it has Bash/Read access — don't paste the diff for it if it's large, just tell it what changed and let it look).
4. Record the validator's verdict and findings in the Task Progress row and in a per-task detail note if there's useful feedback to preserve.
5. If `REJECTED` and attempts remain: loop back to step 2 with the feedback.
6. If `APPROVED`:
   - Commit **only this task's changes** (the executor should have touched only relevant files — verify with `git status` before adding; if it touched something unrelated, that's itself worth noting even on an approved task):
     `git add <touched files> .claude/state/run_log.md`
     `git commit -m "implement(<slug>): <task short name>\n\n<1-2 sentence why, from the plan>\n\nTask <N>/<total> of run implement/<slug>."`
   - Update the Task Progress row: `DONE`, verdict, commit SHA. Update `Last updated`. This commit already includes the log update from the `git add` above — no separate log-only commit needed here.
   - Proceed to the next task.

## STEP 5 — Completion

Once every task is `DONE`:

1. Set `Status: DONE`, commit the final run_log update.
2. Report to the user: the feature branch name, the full list of commits (`git log <base_branch>..HEAD --oneline`), and a one-line summary of what changed and why.
3. **Do not merge, push, or open a PR automatically.** Those are exactly the kind of shared/hard-to-reverse actions that need explicit user confirmation. Tell the user the branch is ready for review and ask if they'd like it pushed / a PR opened.

---

## Non-negotiables (apply throughout)

- Never commit directly to the base branch. Every commit in this workflow lands on the `implement/<slug>` branch.
- Never bundle multiple tasks into one commit. One validated task = one commit.
- Never let the executor or validator commit — that's the orchestrator's job, and only after an `APPROVED` verdict.
- Never silently drop planner/evaluator feedback across rounds, or executor/validator feedback across attempts — always pass it forward verbatim to the next attempt.
- If the working tree is ever dirty when this command expects it clean, stop and ask rather than stashing or discarding.
- If a cap (4 planning rounds, 3 execution attempts) is hit, stop and escalate to the user — do not keep looping silently, and do not unilaterally decide to ship an unapproved plan or a rejected change.
- Watch your own context size as the run progresses. `run_log.md` is always the durable source of truth, so if the conversation is getting large enough that your outputs risk losing accuracy (many planning rounds, many completed tasks, long subagent reports piling up), proactively summarize the conversation yourself — condense it down to the current run state (Status, Approved Plan, Task Progress table, open Blockers, and any feedback still owed to the next attempt) plus anything else needed to keep orchestrating correctly — rather than waiting for it to degrade or for automatic compaction to do it for you.
