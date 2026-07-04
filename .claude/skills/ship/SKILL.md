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
- `git status --porcelain` — uncommitted changes are part of this ship (step 1). If clean AND `git rev-list --left-right --count origin/<base>...HEAD` shows 0 ahead → STOP (nothing to ship).
- `git worktree list --porcelain` — note which worktree holds the base branch (fast-forwarded in step 6) and whether any worktree sits **parked** on a persistent branch (e.g. `wip`) that is *not* the branch being shipped — that parked branch is recycled in step 7. NEVER hardcode worktree paths.

## 1. Commit (only if there are changes)
- Conventional Commits. Subject ≤ ~70 chars; body explains the *why* per change, not the *what*.
- End every commit message with the required trailers (`Co-Authored-By:` + `Claude-Session:`) from the global/CLAUDE instructions.
- Branch already has commits ahead of base and a clean tree → skip to step 2.

## 2. Push
- `git push -u origin <branch>`. This fires the pre-push hook (full `sbt test`).
- **Known infra flake:** in a churned sbt JVM the pgjdbc driver can deregister → `Failed to get driver instance for ...ccas_test` / `No suitable driver`. That is NOT a code failure (see memory `project_sbt_test_driver_deregistration_flake`). If the hook fails ONLY on that:
  1. prove the code with a forked run: `sbt ';set Test/fork := true ;testOnly <touched suites>'` (forking re-registers the driver);
  2. then `git push --no-verify` and let CI (runs cold) be the gate.
- Any REAL test failure → stop and fix. Never `--no-verify` past a real failure.

## 3. Open the PR
- `gh pr create --base <base> --head <branch>` with:
  - title mirroring the main commit subject;
  - body: **What / Fixes / Testing / linked issues**.
- End the PR body with the required footer (`🤖 Generated with Claude Code` + session URL).

## 4. Watch PR CI
- `gh pr checks <n> --watch --interval 20`.
- Green → step 5. Failure → `gh run view <id> --log-failed`, report the real failure, stop.

## 5. Merge when green
- Match the repo's merge convention. **This repo squash-merges** (PR number shows up in `main` commit subjects, e.g. `… (#94)`): `gh pr merge <n> --squash`.
- Do NOT pass `--delete-branch`; branch disposal is step 7 and depends on the branch's role.

## 6. Sync the base worktree
- `git fetch origin --prune`.
- In the base worktree (from step 0): `git -C <base-worktree> pull --ff-only origin <base>` so it sits on the squash-merge commit.

## 7. Dispose of the shipped branch (the only role-dependent step)
Squash-merge gives `main` a new SHA, so the feature branch now diverges. Decide by role:
- **Persistent / rolling branch** — one the user reuses across features (default: `wip`; treat any branch the user keeps coming back to this way). Recycle it onto merged base so it's a clean start:
  - working tree must be clean: `git reset --hard origin/<base>` then `git push --no-verify --force-with-lease origin <branch>` (`--no-verify`: the tree is byte-identical to the just-merged, CI-green base, so the pre-push `sbt test` is pure waste).
  - Run the reset and the push as **separate Bash invocations**, never chained with `&&` — the settings allowlist uses exact-match rules that don't match compound commands.
- **Throwaway feature branch** — one PR, one purpose: delete it: `git push origin --delete <branch>`, and prune any local branch/worktree.
- **Ambiguous?** ASK recycle-vs-delete before doing either. Default `wip` → recycle.
- (If the user later runs several rolling branches, lift the single `wip` default into a small "persistent branches" list — until then, default + ask is enough.)

### Also recycle the parked persistent branch
When the shipped branch was a **throwaway cut from base** while a worktree sat parked on a persistent branch (`wip`, from step 0), that parked branch is now behind the merge even though it was never shipped. Bring it level with base too — guarded:
- **Pure-lag only.** `git rev-list --left-right --count origin/<base>...<parked>` must show **0 ahead**. If 0 ahead → clean tree, then `git reset --hard origin/<base>` + `git push --no-verify --force-with-lease origin <parked>`. `--no-verify`: the tree is byte-identical to an already-green base, so the pre-push `sbt test` is pure waste here.
- **Has commits ahead?** STOP and ASK. Those are unshipped work and `reset --hard` destroys them — never auto-recycle a parked branch that is ahead of base.

## 8. Watch main CI
- `gh run list --branch <base> --limit 1` → `gh run watch <id> --interval 20 --exit-status` on the merge commit's run. Report green/red.

## 9. Update issues
- Issues the PR *resolves*: comment with the PR link + merge SHA, then close.
- Issues *deferred* or `pending-decision`: comment a backref (PR + SHA), keep open. Never auto-close a `pending-decision` item.

## Output
Finish with a compact status table: commit SHA, PR #, PR CI, merge SHA, base-worktree sync, branch disposal, main CI, issues touched.
