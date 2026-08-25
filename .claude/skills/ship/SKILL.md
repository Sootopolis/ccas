---
name: ship
description: Ship the current branch through the full release flow — commit, push, open a PR, merge when CI is green, sync worktrees, watch main CI, and update issues. Use when the user says "ship it", "ship this", "ship the branch", or asks to take the current branch through PR → merge → sync.
---

# Ship the current branch

Take the work on the **current branch** through this repo's full release flow. Branch-agnostic: ships whatever branch is checked out — it must NOT be the base branch. The whole sequence is authorized in one go only when the user explicitly asks to ship (i.e. asks for the downstream steps too); otherwise stop after the step they named.

## 0. Preflight (gather, don't assume)
- Current branch: `git rev-parse --abbrev-ref HEAD`.
- Base branch: `gh repo view --json defaultBranchRef -q .defaultBranchRef.name` (here: `main`).
- If current branch == base → STOP. Nothing to ship from base; tell the user to branch first.
- If current branch == the local-only parking branch (`wip`) → **STOP.** Step 2's `git push -u` would republish it and
  reintroduce the force-push cycle that unpublishing it removed. The work needs a throwaway branch first — carry it
  across with `git switch -c <feature>` (which brings uncommitted changes with it), and if commits were already made
  on `wip`, put `wip` back afterwards with `git reset --hard origin/<base>`. Say this; do not do it unasked, since it
  moves the user's work.
- `git status --porcelain` — uncommitted changes are part of this ship (step 1). If clean AND `git rev-list --left-right --count origin/<base>...HEAD` shows 0 ahead → STOP (nothing to ship).
- `git worktree list --porcelain` — note which worktree holds the base branch (fast-forwarded in step 6) and whether any worktree sits **parked** on a local branch (`wip`) that is *not* the branch being shipped — that parked branch is re-based onto the merge in step 7. NEVER hardcode worktree paths.
- `git rev-parse --abbrev-ref <branch>@{upstream}` — whether the shipped branch is published. A branch with no upstream has never left the machine, which changes disposal (step 7).

## 1. Commit (only if there are changes)
- Conventional Commits. Subject ≤ ~70 chars; body explains the *why* per change, not the *what*.
- End every commit message with the `Co-authored-by:` trailer from the global CLAUDE instructions, matching the casing already in `git log` (this repo uses `Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>`). Add a session trailer ONLY if one appears in recent commits — do not invent one.
- Branch already has commits ahead of base and a clean tree → skip to step 2.

## 2. Push
- `git push -u origin <branch>`. This fires the pre-push hook (full `sbt test`).
- **Possible infra flake — verify, do not assume.** `Failed to get driver instance for ...ccas_test` / `No suitable driver`
  can come from pgjdbc's JVM-global `DriverManager` registration rather than from the code: the driver self-registers on
  first class load, and if a later run gets a fresh test classloader, `DriverManager.isDriverAllowed` resolves
  `org.postgresql.Driver` to a different `Class` object and rejects it. It reaches `PostgresClient`'s `setJdbcUrl` path,
  not the `dataSource.*` one.
- Treat that as a hypothesis, never as a licence to skip the gate. **Reproduce it before acting**: re-run the suite in a
  *fresh* sbt server (`sbt --client shutdown`, then `sbt -batch test`). If it passes cold, it was environmental; if it
  fails cold, it is real and you stop. Only once it is cold-green:
  1. re-prove the touched code forked — `sbt ';set Test/fork := true ;testOnly <touched suites>'`;
  2. then `git push --no-verify` and let CI (which always runs cold) be the gate, saying plainly in the PR that the hook
     was bypassed and why.
- Any REAL test failure → stop and fix. Never `--no-verify` past a real failure.
- Status note: as of 2026-08-25 this had not been reproduced on sbt 1.13.0 — the hook ran clean repeatedly, and four
  consecutive `testFull` runs in one warm sbt 2.0.7 server were green. If you hit it, capture the evidence rather than
  citing this line.

## 3. Open the PR
- `gh pr create --base <base> --head <branch>` with:
  - title mirroring the main commit subject;
  - body: **What / Fixes / Testing / linked issues**.
- End the PR body with the required footer (`🤖 Generated with Claude Code` + session URL).

## 3b. Permission preflight (before anything irreversible)
Steps 7-9 use commands that may be gated (classifier or allowlist). A denial there strands the flow *after* the merge,
which is the one step that cannot be undone. So check first, while everything is still reversible:
- Confirm `gh pr`, `gh run` and `gh issue` are usable — a cheap `gh run list --branch <base> --limit 1` proves it.
- Post the step-9 issue backrefs NOW (comment only; leave closing until after the merge, since the SHA isn't known yet).
- If any of those is denied: say so and ask whether to merge anyway. Merging with the follow-up steps known-blocked is a
  choice the user should make deliberately, not discover afterwards.

## 4. Watch PR CI
- `gh pr checks <n> --watch --interval 20`.
- Green → step 5. Failure → `gh run view <id> --log-failed`, report the real failure, stop.

## 5. Merge when green
- Match the repo's merge convention. **This repo squash-merges** (PR number shows up in `main` commit subjects, e.g. `… (#94)`): `gh pr merge <n> --squash`.
- Do NOT pass `--delete-branch`; branch disposal is step 7 and depends on the branch's role.

## 6. Sync the base worktree
- `git fetch origin --prune`.
- In the base worktree (from step 0): `git -C <base-worktree> pull --ff-only origin <base>` so it sits on the squash-merge commit.

## 7. Dispose of the shipped branch
**The working model is throwaway branches.** One branch per PR, cut from base, deleted after merge. `wip` is a
*local-only parking branch* for the worktree — it is never pushed and has no upstream (`origin` holds `main` alone).
That is deliberate: a published branch would have to be force-pushed to recycle, because squash-merge gives `main` a
new SHA and the branch then diverges. Unpublished, recycling is a local reset nobody has to approve.

- **The shipped branch** — delete it: `git push origin --delete <branch>` (only if it was published), then prune the
  local branch. Never `--delete-branch` on the merge; disposal is decided here.
- **Before deleting anything published, prove it holds nothing unique.** `git rev-list` "ahead" counts are misleading
  under squash-merge: the branch commit and the squash commit have the same content and different SHAs, so a merged
  branch still reads as ahead. Use `git cherry <base> <branch>` — `-` means the patch is already in base (safe), `+`
  means genuinely absent (STOP and ASK). Cross-check with `gh pr list --state all --head <branch>`.
- **Ambiguous, or the user calls a branch long-lived?** ASK before deleting. Deletion is irreversible from here.

### Re-base the parked branch (`wip`)
The worktree parked on `wip` (from step 0) is now behind the merge. Because `wip` is local-only this is one command,
no push and no force:
- **Pure-lag only.** `git rev-list --left-right --count origin/<base>...wip` must show **0 ahead**. Then, with a clean
  tree: `git reset --hard origin/<base>`.
- **Has commits ahead?** STOP and ASK. That is unshipped work and `reset --hard` destroys it.
- **If `wip` has somehow acquired an upstream again**, do not force-push it back into line — say so and ask. Getting it
  republished is the regression; a force-push would just entrench it.

## 8. Watch main CI
- `gh run list --branch <base> --limit 1` → `gh run watch <id> --interval 20 --exit-status` on the merge commit's run. Report green/red.

## 9. Update issues
Backref comments should already be posted (step 3b); this step adds the merge SHA and closes what the PR resolved.
- Issues the PR *resolves*: comment with the PR link + merge SHA, then close.
- Issues *deferred* or `pending-decision`: comment a backref (PR + SHA), keep open. Never auto-close a `pending-decision` item.
- If no issue is resolved by the PR, say so explicitly rather than silently skipping the step.

## Output
Finish with a compact status table: commit SHA, PR #, PR CI, merge SHA, base-worktree sync, branch disposal, main CI, issues touched.
Mark any step that was refused or skipped as such — never leave a blocked step looking complete, and list the exact
commands needed to finish it.
