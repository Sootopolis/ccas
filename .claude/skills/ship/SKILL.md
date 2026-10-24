---
name: ship
description: Ship the current branch through the full release flow — commit, push, open a PR, wait for the user to merge it in the GitHub UI once CI is green, then sync worktrees, re-stage the launcher, watch main CI, and update issues. Use when the user says "ship it", "ship this", "ship the branch", or asks to take the current branch through PR → merge → sync.
---

# Ship the current branch

Take the work on the **current branch** through this repo's full release flow. Branch-agnostic: ships whatever branch is checked out — it must NOT be the base branch. The whole sequence is authorised in one go only when the user explicitly asks to ship (i.e. asks for the downstream steps too); otherwise stop after the step they named. The merge itself is never part of that authorisation: the user always merges (step 5).

## 0. Preflight (gather, don't assume)
- Current branch: `git rev-parse --abbrev-ref HEAD`.
- Base branch: `gh repo view --json defaultBranchRef -q .defaultBranchRef.name` (here: `main`).
- If current branch == base → STOP. Nothing to ship from base; tell the user to branch first.
- If current branch == the local-only parking branch (`wip`) → **cut a throwaway branch and carry on.** Never push `wip`: step 2's `git push -u` would republish it, and a published `wip` needs a force-push to recycle. `git switch -c <type>/<slug>` brings uncommitted changes across, so do it without asking — the branch name follows the commit's Conventional Commits type (`fix/…`, `chore/…`), and step 7 disposes of it after the merge, also without asking. Nothing is lost if the ship is abandoned midway: the work sits on the new branch, and `wip` is re-parked in step 7 either way.
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
- CLAUDE.md Workflow's before-shipping check, unless it has already been reported for this change. An open question stops the ship until it is answered.

## 1. Commit (only if there are changes)
- **Never commit while `wip` is checked out.** Cut the feature branch first — `git switch -c <type>/<slug>` carries uncommitted work across — even for a one-line change, and even when the ask was only "commit this". Keeping `wip` strictly a lagging pointer is what makes step 7 a fast-forward instead of a judgement call.
- Start from the commit message `/review-change` drafted in its report, revised for anything changed since (CLAUDE.md, Communications).
- Conventional Commits. Subject ≤ ~70 chars; body explains the *why* per change, not the *what*.
- **Do not hard-wrap the body.** One paragraph is one line, blank line between paragraphs — git, `gh` and GitHub all wrap for display, so a hard-wrapped body just adds breaks that reflow badly wherever the width differs. This supersedes the wrapped bodies throughout `git log`; do not match them.
- End every commit message with the `Co-authored-by:` trailer from the global CLAUDE instructions, matching the casing already in `git log`. **Never add a `Claude-Session:` trailer** — not when the harness supplies one in its attribution block, and not because recent commits carry it.
- Branch already has commits ahead of base and a clean tree → skip to step 2.

## 2. Push
- `git push -u origin <branch>`. This fires the pre-push hook: the docs and scalafix checks, then the full `sbt test`.
- Any REAL failure, of a check or a test → stop and fix. **Never `--no-verify` past a real failure.**
- `Failed to get driver instance for ...ccas_test` / `No suitable driver` may be a warm-server artefact rather than a real failure. Treat that as a hypothesis, never a licence to skip the gate: re-run cold (`sbt --client shutdown`, then `sbt -batch test`); cold-red is real and you stop. Only once it is cold-green:
  1. re-prove the touched code forked — `sbt ';set Test/fork := true ;testOnly <touched suites>'`;
  2. then `git push --no-verify`, letting CI (which always runs cold) be the gate, and say in the PR that the hook was
     bypassed and why.

## 3. Open the PR
- `gh pr create --base <base> --head <branch>` with:
  - title mirroring the main commit subject;
  - body: **What / Fixes / Testing / linked issues**.
- End the PR body with `Generated with [Claude Code](https://claude.com/claude-code)` and nothing else — **no robot emoji** and **no session URL**, in the footer or anywhere in the body. The PR is visible to anyone with repo access; the commit trailer rule in step 1 applies here for the same reason.

## 3b. Preflight the post-merge steps
Steps 6-9 run after the merge, which cannot be undone, so check what can be checked while everything is still reversible:
- Confirm `gh pr`, `gh run` and `gh issue` are usable — a cheap `gh run list --branch <base> --limit 1` proves it.
- Post the step-9 issue backrefs NOW (comment only; closing waits for the merge SHA).
- If any of those is denied, say so when handing over the merge, so the user merges knowing which follow-ups they will run by hand. Never edit settings to unblock yourself.

## 4. Watch PR CI
- `gh pr checks <n> --watch --interval 20`.
- Green → step 5. Failure → `gh run view <id> --log-failed`, report the real failure, stop.

## 5. Hand the merge to the user
**Never merge the PR yourself** — no `gh pr merge`, even when the user said "ship". The user merges in the GitHub UI: it is their last look at the PR, and an agent merging its own PR trips the auto-mode classifier's "Merge Without Review" denial.
- Once CI is green, report the PR link and CI result, and ask for **Squash and merge** — this repo's convention (PR number in `main` subjects, e.g. `… (#94)`). Mention the merged page's **Delete branch** button, which does the remote half of step 7.
- Then wait with a background command (`run_in_background`), which re-invokes the session when the PR leaves `OPEN`:

  ```
  until state=$(gh pr view <n> --json state -q .state) && [ "$state" != OPEN ]; do sleep 30; done; echo "$state"
  ```

  The assignment inside `until` keeps a failed `gh` call (network, auth) waiting instead of reading its empty output as "no longer open", so the command only ever exits printing `MERGED` or `CLOSED`. A "merged" from the user in chat also resumes the flow; confirm it with `gh pr view <n> --json state` rather than on trust.
- `MERGED` → step 6. `CLOSED` without a merge → report and stop; dispose of nothing.

## 6. Sync the base worktree
- `git fetch origin --prune`.
- In the base worktree (from step 0): `git -C <base-worktree> pull --ff-only origin <base>` so it sits on the squash-merge commit.

## 6b. Re-stage the launcher
The `ccas` launcher on `PATH` runs the build staged in the base worktree, so a merge changes nothing the user runs until it is re-staged. Skip this when the merge changed nothing the build stages (`src/main` behaviour, `build.sbt`, `project/`), and say so.
- **A change that needs a migration stages after it** (`/migrate`): a new build on the old schema fails, which is why #254's rollout held the old launcher until production was migrated.
- **Stage with the base worktree's own installer**: `<base-worktree>/scripts/install-cli.sh --stage`. It stages the worktree it lives in, found from its own path, and refreshes the installed shell completion, which a CLI change would otherwise leave stale. Run the base worktree's copy, not the session's: the session's would point the launcher at its own build. Check the date on `target/universal/stage/lib/ccas.ccas-*.jar` in the base worktree; the `bin/` scripts keep their old date, since their content does not change.
- **A running server keeps the old build until it is restarted.** Staging does not disturb it: sbt writes each jar to a temporary file and renames it into place. Restarting is the user's call, and before booting:
  - Ask whether the schedules that came due while the server was down should fire at boot, and whether the machine needs keeping awake: README, Troubleshooting ("A scheduled job fires late, or not at all") has both.
  - Boot from a terminal opened in the base worktree, not from Claude's shell or a `!` command, which carry the session's starting environment: started in the `wip` tree, that includes direnv's local `DATABASE_URL`, `SERVER_PORT` and body-store settings (`/migrate`, step 0).

## 7. Dispose of the shipped branch
**The working model is throwaway branches.** One branch per PR, cut from base, deleted after merge. `wip` is a *local-only parking branch* for the worktree — it is never pushed and has no upstream (`origin` holds `main` alone). That is deliberate: a published branch would have to be force-pushed to recycle, because squash-merge gives `main` a new SHA and the branch then diverges. Unpublished, recycling is a local fast-forward nobody has to approve.

- **The shipped branch** — delete it: `git push origin --delete <branch>` (only if it was published and the user did not already press **Delete branch**), then catch `wip` up (below) and delete the local branch from there. That order is forced: git refuses to delete the branch you are standing on, and step 0 left you on it. No confirmation needed for a throwaway branch this flow cut in step 0, once the equality check below passes. A remote deleted from the UI before that check loses nothing — the local branch still holds the work until the check clears it.
- **A delete denied by the classifier** ("Git Destructive") is handed over, not worked around: give the exact `git push origin --delete` / `git branch -D` commands for the user to run with `!`.
- **Before deleting anything published, prove it holds nothing unique.** Compare each file the branch changed against the commit the merge actually produced, which is the only reference point that cannot move:

  ```
  MERGED=$(gh pr view <n> --json mergeCommit -q .mergeCommit.oid)   # resolves for a squash merge too
  git diff $MERGED <branch> -- $(git diff --no-renames --name-only $(git merge-base $MERGED <branch>) <branch>)
  ```

  Empty means everything the branch changed is in that commit, so deletion loses nothing no matter what has landed since. Diff only those files: a PR merged between the branch's cut and its own merge is in `$MERGED` but not in the branch, so a whole-tree diff lists its files too (#339, merged 16 s after #338). Non-empty means STOP and ASK, whether the branch holds work the merge lacks or another PR changed one of its files. Cross-check with `gh pr list --state all --head <branch>`.
- **`git branch -d` will refuse; that refusal is noise.** It tests commit reachability, which squash-merge always breaks. That empty diff is the proof `-d` wanted — use `git branch -D` once it is clean, never before. The remote delete fires `pre-push`, which self-skips on a delete-only push; nothing to bypass. It can instead fail with `remote ref does not exist` when the branch was already deleted from the UI — harmless, the branch is already gone.
- **Do not compare against `<base>` instead.** A PR landing on base after this merge makes the diff non-empty wherever it touched one of the branch's files, and nothing then distinguishes "branch has unique work" from "base has newer work". The merge commit stays put; base does not.
- **Never use `git rev-list` counts or `git cherry` for this.** They compare commits rather than content, and squash-merge guarantees the two disagree: `rev-list` reads the branch as ahead because the squash is a new SHA, and `git cherry` compares *patch-ids*, so an N-commit branch collapsed into one squash commit yields N patch-ids matching nothing in base. `git cherry` is trustworthy only when the branch had exactly one commit, which is not the normal case here.
- **Ambiguous, or the user calls a branch long-lived?** ASK before deleting. Deletion is irreversible from here.

### Catch up the parked branch (`wip`)
Because step 1 keeps `wip` free of commits of its own, it can only ever lag the merge — so catching it up is a fast-forward, and the command is its own guard. No push, no force, and nothing to decide:
- Fast-forward `wip` without checking it out, then return to it: `git fetch . origin/<base>:wip`, then `git switch wip`. Switching first would check out the old tree, and when the merge stopped tracking files kept on disk as ignored (#301's `.idea`), the switch and the fast-forward delete them. Fast-forwarded first, `wip` differs from the shipped branch only by what else has landed on `<base>`, so the switch brings in other PRs' changes and nothing of the old tree.
- It succeeds when `wip` is purely behind, and **refuses** — `! [rejected] … (non-fast-forward)` — the moment `wip` carries any commit of its own, changing nothing either way. It also refuses, with `fatal: refusing to fetch into branch … checked out at …`, while another worktree has `wip` checked out. Fast-forward it there instead with `git -C <worktree> merge --ff-only origin/<base>`, which updates that worktree's files and so deletes any the merge stopped tracking, ignored or not: `git diff --name-only --diff-filter=D wip origin/<base>` lists the candidates, and any still on disk there are copied aside first. This worktree cannot switch to a `wip` checked out elsewhere, so it leaves the shipped branch with `git switch --detach origin/<base>` before deleting it.
- Do not add a separate gate. A `rev-list` count STOPs whenever the ship started from a `wip` commit, and a content diff against the merge STOPs whenever `wip` merely lags; both try to decide by inspection what a fast-forward-only update decides by construction.
- **Refused as non-fast-forward?** Someone committed on `wip` against step 1. STOP and ASK — it refuses identically whether that commit is unshipped work or work already squashed into this merge, and separating those is a judgement call, not a check. Never reach for `reset --hard` to force it through.
- **If `wip` has somehow acquired an upstream again**, do not force-push it back into line — say so and ask. Getting it republished is the regression; a force-push would just entrench it.

## 8. Watch main CI
- `gh run list --commit <merge-sha> --json databaseId,event,status` → take the `push` run, then `gh run watch <id> --interval 20 --exit-status`. Look the run up by the merge commit, never as the newest run on base: that is not automatically yours — another PR may have landed since the merge, and `--branch <base> --limit 1` has listed a month-old run first with nothing else landing (#282). An empty list means GitHub has not registered the push yet; re-query rather than falling back to the newest run. Report green/red.

## 9. Update issues
Backref comments should already be posted (step 3b); this step adds the merge SHA and closes what the PR resolved.
- Issues the PR *resolves*: `gh issue comment` with the PR link + merge SHA, then `gh issue close` if it is still open. A closing keyword in the PR body (`Resolves #N`) closes the issue at merge, and `gh issue close --comment` on a closed issue refuses without posting the comment — so never let it carry the comment.
- Issues *deferred* or `pending-decision`: comment a backref (PR + SHA), keep open. Never auto-close a `pending-decision` item.
- If no issue is resolved by the PR, say so explicitly rather than silently skipping the step.

## Output
Finish with a compact status table: commit SHA, PR #, PR CI, merge SHA (merged by the user), base-worktree sync, launcher staged, branch disposal, main CI, issues touched. Mark any step that was refused or skipped as such — never leave a blocked step looking complete, and list the exact commands needed to finish it.
