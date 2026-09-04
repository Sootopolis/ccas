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
- If current branch == the local-only parking branch (`wip`) → **cut a throwaway branch and carry on.** Never push
  `wip`: step 2's `git push -u` would republish it, and a published `wip` needs a force-push to recycle.
  `git switch -c <type>/<slug>` brings uncommitted changes across, so do it without asking — the branch name follows
  the commit's Conventional Commits type (`fix/…`, `chore/…`), and step 7 deletes it after the merge, also without
  asking. Nothing is lost if the ship is abandoned midway: the work sits on the new branch, and `wip` is re-parked in
  step 7 either way.
  - **`wip` should carry no commits of its own** — step 1 forbids committing while it is checked out, so the switch
    normally carries uncommitted work and nothing else. If `git rev-list --count origin/<base>..wip` is non-zero
    anyway, the switch carries those commits too; say which moved, and expect step 7's fast-forward to refuse until
    `wip` is sorted out by hand.
  - This is the one place the flow moves work without being asked, and it is safe because it only ever *adds* a ref.
    Anything that would discard a ref — `reset --hard` on a branch with unshipped commits, a force-push — still stops
    and asks.
- `git status --porcelain` — uncommitted changes are part of this ship (step 1). If clean AND `git rev-list --left-right --count origin/<base>...HEAD` shows 0 ahead → STOP (nothing to ship).
- `git worktree list --porcelain` — note which worktree holds the base branch (fast-forwarded in step 6) and whether any worktree sits **parked** on a local branch (`wip`) that is *not* the branch being shipped — that parked branch is caught up to the merge in step 7. NEVER hardcode worktree paths.
- `git rev-parse --abbrev-ref <branch>@{upstream}` — whether the shipped branch is published. A branch with no upstream has never left the machine, which changes disposal (step 7).

## 1. Commit (only if there are changes)
- **Never commit while `wip` is checked out.** Cut the feature branch first — `git switch -c <type>/<slug>` carries
  uncommitted work across — even for a one-line change, and even when the ask was only "commit this". Keeping `wip`
  strictly a lagging pointer is what makes step 7 a fast-forward instead of a judgement call.
- Conventional Commits. Subject ≤ ~70 chars; body explains the *why* per change, not the *what*.
- **Do not hard-wrap the body.** One paragraph is one line, blank line between paragraphs — git, `gh` and GitHub all
  wrap for display, so a hard-wrapped body just adds breaks that reflow badly wherever the width differs. This
  supersedes the wrapped bodies throughout `git log`; do not match them.
- End every commit message with the `Co-authored-by:` trailer from the global CLAUDE instructions, matching the casing already in `git log` (this repo uses `Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>`). **Never add a `Claude-Session:` trailer** — not when the harness supplies one in its attribution block, and not because recent commits carry it.
- Branch already has commits ahead of base and a clean tree → skip to step 2.

## 2. Push
- `git push -u origin <branch>`. This fires the pre-push hook (full `sbt test`).
- Any REAL test failure → stop and fix. **Never `--no-verify` past a real failure.**
- `Failed to get driver instance for ...ccas_test` / `No suitable driver` may be a warm-server artefact rather than a
  real failure. Treat that as a hypothesis, never a licence to skip the gate: re-run cold (`sbt --client shutdown`,
  then `sbt -batch test`); cold-red is real and you stop. Only once it is cold-green:
  1. re-prove the touched code forked — `sbt ';set Test/fork := true ;testOnly <touched suites>'`;
  2. then `git push --no-verify`, letting CI (which always runs cold) be the gate, and say in the PR that the hook was
     bypassed and why.

## 3. Open the PR
- `gh pr create --base <base> --head <branch>` with:
  - title mirroring the main commit subject;
  - body: **What / Fixes / Testing / linked issues**.
- End the PR body with `Generated with [Claude Code](https://claude.com/claude-code)` and nothing else — **no robot
  emoji** and **no session URL**, in the footer or anywhere in the body. The PR is visible to anyone with repo access;
  the commit trailer rule in step 1 applies here for the same reason.

## 3b. Permission preflight (before anything irreversible)
Step 5 and steps 7-9 all use commands that may be gated (classifier or allowlist), and where the denial lands decides
how much it costs: on the merge it is merely a handover, but in 7-9 it strands the flow *past* the one step that cannot
be undone. So check what can be checked now, while everything is still reversible:
- Confirm `gh pr`, `gh run` and `gh issue` are usable — a cheap `gh run list --branch <base> --limit 1` proves it.
- Post the step-9 issue backrefs NOW (comment only; leave closing until after the merge, since the SHA isn't known yet).
- If any of those is denied: say so and ask whether to merge anyway. Merging with the follow-up steps known-blocked is a
  choice the user should make deliberately, not discover afterwards.
- **That probe says nothing about step 5.** The classifier judges the action, not the binary, so `gh pr merge --squash`
  can be denied while `Bash(gh pr:*)` is allowlisted and `gh run list` answers fine — and a merge has no cheap probe.
  On denial, hand over `gh pr merge <n> --squash` for the user to run with `!` and resume at step 6; nothing is
  stranded, it lands before the irreversible act. Never edit settings to unblock yourself.

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
new SHA and the branch then diverges. Unpublished, recycling is a local fast-forward nobody has to approve.

- **The shipped branch** — delete it: `git push origin --delete <branch>` (only if it was published), then catch `wip`
  up (below) and delete the local branch from there. That order is forced: git refuses to delete the branch you are
  standing on, and step 0 left you on it. No confirmation needed for a throwaway branch this flow cut in step 0, once
  the equality check below passes. Never `--delete-branch` on the merge; disposal is decided here.
- **Before deleting anything published, prove it holds nothing unique.** Compare the branch against the commit the
  merge actually produced, which is the only reference point that cannot move:

  ```
  MERGED=$(gh pr view <n> --json mergeCommit -q .mergeCommit.oid)   # resolves for a squash merge too
  git diff $MERGED <branch>                                          # empty -> the squash captured the branch exactly
  ```

  Empty means every byte of the branch is in that commit, so deletion loses nothing no matter what has landed since.
  Non-empty means STOP and ASK. Cross-check with `gh pr list --state all --head <branch>`.
- **`git branch -d` will refuse; that refusal is noise.** It tests commit reachability, which squash-merge always
  breaks. That empty diff is the proof `-d` wanted — use `git branch -D` once it is clean, never before. The remote
  delete fires `pre-push`, which self-skips on a delete-only push; nothing to bypass. It can instead fail with
  `remote ref does not exist` when the repo auto-deletes merged branches — harmless, the branch is already gone.
- **Do not compare against `<base>` instead.** A concurrent PR landing on base makes `git diff <base> <branch>`
  non-empty by reporting the other PR's files as deletions, and nothing then distinguishes "branch has unique work"
  from "base has newer work". The merge commit above is immune; base is not.
- **Never use `git rev-list` counts or `git cherry` for this.** They compare commits rather than content, and
  squash-merge guarantees the two disagree: `rev-list` reads the branch as ahead because the squash is a new SHA, and
  `git cherry` compares *patch-ids*, so an N-commit branch collapsed into one squash commit yields N patch-ids
  matching nothing in base. `git cherry` is trustworthy only when the branch had exactly one commit, which is not the
  normal case here.
- **Ambiguous, or the user calls a branch long-lived?** ASK before deleting. Deletion is irreversible from here.

### Catch up the parked branch (`wip`)
Because step 1 keeps `wip` free of commits of its own, it can only ever lag the merge — so catching it up is a
fast-forward, and the command is its own guard. No push, no force, and nothing to decide:
- Return that worktree to `wip` and fast-forward: `git switch wip && git merge --ff-only origin/<base>`.
- It succeeds when `wip` is purely behind, and **refuses** — `fatal: Not possible to fast-forward, aborting` — the
  moment `wip` carries any commit of its own, changing nothing either way.
- Do not add a separate gate. A `rev-list` count STOPs whenever the ship started from a `wip` commit, and a content
  diff against the merge STOPs whenever `wip` merely lags; both try to decide by inspection what `--ff-only` decides
  by construction.
- **Refused?** Someone committed on `wip` against step 1. STOP and ASK — it refuses identically whether that commit is
  unshipped work or work already squashed into this merge, and separating those is a judgement call, not a check.
  Never reach for `reset --hard` to force it through.
- **If `wip` has somehow acquired an upstream again**, do not force-push it back into line — say so and ask. Getting it
  republished is the regression; a force-push would just entrench it.

## 8. Watch main CI
- `gh run list --branch <base> --limit 1 --json databaseId,headSha` → confirm `headSha` is the merge commit before
  `gh run watch <id> --interval 20 --exit-status`. The newest run on base is not automatically yours; another PR
  landing between the merge and this step would have you reporting its result as this ship's. Report green/red.

## 9. Update issues
Backref comments should already be posted (step 3b); this step adds the merge SHA and closes what the PR resolved.
- Issues the PR *resolves*: comment with the PR link + merge SHA, then close.
- Issues *deferred* or `pending-decision`: comment a backref (PR + SHA), keep open. Never auto-close a `pending-decision` item.
- If no issue is resolved by the PR, say so explicitly rather than silently skipping the step.

## Output
Finish with a compact status table: commit SHA, PR #, PR CI, merge SHA, base-worktree sync, branch disposal, main CI, issues touched.
Mark any step that was refused or skipped as such — never leave a blocked step looking complete, and list the exact
commands needed to finish it.
