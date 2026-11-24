---
name: review-change
description: Review the work in progress — the changes and their blast radius, code and documentation — and fix what the review finds. Run it unprompted once a change is built (CLAUDE.md, Workflow), and whenever the user asks for a review.
---

# Review a change

The diff is where a review starts, not where it ends. The defects this review exists to catch are the change disagreeing with something it never touched: a README entry made obsolete, a CI assumption broken, a claim moved verbatim into a new document while already false.

## 1. Read the change

`printenv CLAUDE_EFFORT`, for the report. `git status`, then `git merge-base origin/main HEAD` and `git diff <that commit>`, plus the untracked files, which a diff never shows: `git ls-files --others --exclude-standard`. Diffing against `origin/main` itself would show anything merged since the branch was cut as reversed in this change. Read the issue with its comments, and the agreed plan: the change is checked against what was promised, not only against itself.

## 2. Map the blast radius

For every symbol, setting, column, JSON field, CLI option and behaviour the change touches, find what depends on it. Search; don't reason from memory.

- **Callers and readers**, tests included. When a row can take a new state, or a column a new value, that means every query filtering on those columns, which may now leave the new rows on the wrong side.
- **What the change leaves without a caller or reader**: a method, setting or column whose last use it removes is deleted with it. `-Wunused` never reports a public member, which is how `JobFollower.handleSingle` outlived its last caller by nearly four months (#159 to #392).
- **Prose describing the old behaviour**: scaladoc, `README.md`, `docs/architecture.md`, `docs/chess-com-api.md`, `CLAUDE.md`, the skills, the ADRs. An ADR the change contradicts is superseded or amended in place, as `docs/adr/README.md` says.
- **Tests whose names state the old guarantee.**
- **Build and CI**: `.github/workflows/`, `.githooks/`, `scripts/`, `build.sbt`, `project/`.
- **Schema**: an edited `createTable` needs a hand-written `ALTER` for every existing database (`/migrate`). Name each one in the report.
- **CLI tree**: `completions/ccas.bash` must be regenerated (`.claude/rules/cli.md`, Shell completions).
- **Data already stored**: say whether existing rows need correcting or will correct themselves on the next run, and why.
- **Values read differently**: for each setting, field or input whose reading the change moves, compare old with new on when it is read (once at start, or at each use) and how strictly (blank, null, absent, malformed). A refactor that claims no behaviour change fails here first: #509's typed config made a blank `DB_PORT` block a URL-configured database, and an unresolvable report directory fail the boot instead of the write.
- **Concurrency**: a server and a CLI can share a database. Check transactions and row locks, and that no Chess.com call happens inside a transaction.
- **Copies outside the diff**: generated or gitignored files that hold a stale copy.

## 3. Check

Start the bug hunt with the built-in `/code-review`, at an effort level scaled to the change. It runs in the background, so carry on meanwhile, but don't inject faults then: it reads the working tree, and would review the broken copy. Nor run the suite: it may run its own against the same `ccas_test`, and each run resets the other's schemas. Verify each finding it returns before acting on it: a wrong one is rejected in the report with its reason, and the rest are fixed in step 4 like any other. It knows nothing of this repo beyond what it reads, so then check the change and everything step 2 found for at least:

- bugs, including the edge cases no test reaches
- security, when the change alters the server's HTTP routes, the handling of a secret, or what a shell script does: run the built-in `/security-review` as well
- claims: every statement the change makes about how the code behaves, in prose or comments, checked against the code rather than memory; absolutes (every, never, only, always) first, since false claims cluster there
- inconsistencies: names, behaviour or prose that disagree with the code around them
- duplications and redundancies, in tests as much as in main code
- inefficiencies: extra queries, extra round trips to Chess.com, work repeated per row
- test coverage: every test the plan named exists, every behaviour change has a test, and each new test is shown to fail when the code it guards is broken, on the final code, after every fix: list the faults in a JSON file and run `python3 scripts/inject-faults.py <file>` (its docstring has the format). It runs each test unbroken first, applies one fault at a time with `-Werror` off and restores the file; a fault that does not compile proves nothing, so rewrite it. Where the change replaces old behaviour, one fault puts the old behaviour back: a test that only fails on some other break can still pass on the regression it is named for (#509's `fiberCap` test, whose fixture gave the old and new pool the same cap). Read each new test against its name: the assertions check what the name promises, the effect under test actually runs, and the case that must not happen is covered. Close a gap with a unit test, refactoring production code if that is what makes it reachable, never with a manual check.
- operator-facing output: report lines, logs and errors the change adds read in the operator's terms, not the schema's or the code's, and appear once per path. Where the output has snapshots under `src/test/resources/snapshot/`, read each changed file whole, not just its diff, and give any new section or line a case in its fixture. Where an interactive command's output changes, write out the whole session it produces, the CLI's own lines and the server log it streams together, and read it start to finish: #487's second criteria block passed every snapshot
- documentation and comments up to date and to `docs/documentation-standard.md`; `python3 scripts/check-docs.py` covers the mechanical half
- the conventions in `CLAUDE.md` and in each `.claude/rules/` file whose `paths` cover a changed file — read those, since a diff does not load them — and whether each method lives where a caller would look for it; `sbt lint` covers formatting and the ones scalafix enforces

Then run the affected suites, or the full suite when the change is wide.

## 4. Fix, then ask

Fix findings inside the change straight away, then review what the fixes touched: a fix has a blast radius too. When a fix for an important finding, `/code-review`'s or your own, changed production logic, run `/code-review` once more over the whole diff; fixes to nits, wording, comments or tests alone don't call for it (#517: four rated second passes found nothing important). If that pass changes production code again, say so and ask rather than going round again.

Ask before acting on anything you are unsure of, any judgement call, and anything that reaches beyond the change, such as a codebase-wide sweep, a refactor of untouched code or an audit. Those are often deferred to an issue.

## 5. Report

Open with three lines: what the operator will see differ; the state (suite green or red, findings fixed, questions open); and whether it is ready to ship once those are answered. Label every finding **blocking**, **non-blocking**, **nit** or **question**, so the report can be skimmed for what matters. Then:

- judgement calls and questions, each in plain terms with a recommendation
- the before-shipping check (CLAUDE.md, Workflow, step 4): open questions, follow-ups worth an issue, and anything the plan promised that has not landed
- the change as it now stands: what changed and where, then a table of the plan's promises (each change, test and doc it named), each with where it landed or why it departed, departures the review itself made included. #509's review missed two unlanded promises that only a second reviewer found
- what was found and fixed, one line each
- the level this review ran at; recommend changing the session's level only when more building follows in this session (CLAUDE.md, Workflow, Effort)
- for a large change, an offer of a second review by a fresh agent that sees only the diff, the issue and the plan, since this review shares the author's blind spots. Recommend it when the change reaches an area Workflow step 1 names, or concurrency; otherwise recommend against.

Under a **Reference** heading, last:

- what was run, and the result
- for each new test, the fault injected into the code it guards and the failure that followed, so the user can trust a test by its name
- for a bug fix, the change that introduced the bug and, if `/review-change` reviewed it (merged from #319 on), the `escape` label added to the issue
- the few places worth the user's own reading, as `file:line`
- a draft commit message, to `/ship`'s rules for one (step 1), so the user has read it before shipping

End with one `AskUserQuestion` call: the report's questions, and a last one for the next step, offering "Ship now" (recommended when nothing blocks), the second review where offered, and "Hold". "Ship now" is the go-ahead for `/ship` (CLAUDE.md, Git).

Leave the changes uncommitted (CLAUDE.md, Git), unless the answer is "Ship now", which starts `/ship`.
