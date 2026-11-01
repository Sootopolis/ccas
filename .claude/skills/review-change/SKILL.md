---
name: review-change
description: Review the work in progress — the changes and their blast radius, code and documentation — and fix what the review finds. Run it unprompted once a change is built (CLAUDE.md, Workflow), and whenever the user asks for a review.
---

# Review a change

The diff is where a review starts, not where it ends. The defects this review exists to catch are the change disagreeing with something it never touched: a README entry made obsolete, a CI assumption broken, a claim moved verbatim into a new document while already false.

## 1. Read the change

`git status`, then `git merge-base origin/main HEAD` and `git diff <that commit>`, plus the untracked files, which a diff never shows: `git ls-files --others --exclude-standard`. Diffing against `origin/main` itself would show anything merged since the branch was cut as reversed in this change. Read the issue and the agreed plan too: the change is checked against what was promised, not only against itself.

## 2. Map the blast radius

For every symbol, setting, column, JSON field, CLI option and behaviour the change touches, find what depends on it. Search; don't reason from memory.

- **Callers and readers**, tests included. When a row can take a new state, or a column a new value, that means every query filtering on those columns, which may now leave the new rows on the wrong side.
- **Prose describing the old behaviour**: scaladoc, `README.md`, `docs/architecture.md`, `docs/chess-com-api.md`, `CLAUDE.md`, the skills, the ADRs. An ADR the change contradicts is superseded or amended in place, as `docs/adr/README.md` says.
- **Tests whose names state the old guarantee.**
- **Build and CI**: `.github/workflows/`, `.githooks/`, `scripts/`, `build.sbt`, `project/`.
- **Schema**: an edited `createTable` needs a hand-written `ALTER` for every existing database (`/migrate`). Name each one in the report.
- **CLI tree**: `completions/ccas.bash` must be regenerated (`.claude/rules/cli.md`, Shell completions).
- **Data already stored**: say whether existing rows need correcting or will correct themselves on the next run, and why.
- **Concurrency**: a server and a CLI can share a database. Check transactions and row locks, and that no Chess.com call happens inside a transaction.
- **Copies outside the diff**: generated or gitignored files that hold a stale copy.

## 3. Check

Start the bug hunt with the built-in `/code-review`, at an effort level scaled to the change. It runs in the background, so carry on meanwhile, but don't inject faults then: it reads the working tree, and would review the broken copy. Verify each finding it returns before acting on it: a wrong one is rejected in the report with its reason, and the rest are fixed in step 4 like any other. It knows nothing of this repo beyond what it reads, so then check the change and everything step 2 found for at least:

- bugs, including the edge cases no test reaches
- security, when the change alters the server's HTTP routes, the handling of a secret, or what a shell script does: run the built-in `/security-review` as well
- claims: every statement the change makes about how the code behaves, in prose or comments, checked against the code rather than memory; absolutes (every, never, only, always) first, since false claims cluster there
- inconsistencies: names, behaviour or prose that disagree with the code around them
- duplications and redundancies, in tests as much as in main code
- inefficiencies: extra queries, extra round trips to Chess.com, work repeated per row
- test coverage: every test the plan named exists, every behaviour change has a test, and each new test is shown to fail when the code it guards is broken. Read each new test against its name: the assertions check what the name promises, the effect under test actually runs, and the case that must not happen is covered. Close a gap with a unit test, refactoring production code if that is what makes it reachable, never with a manual check.
- operator-facing output: report lines, logs and errors the change adds read in the operator's terms, not the schema's or the code's, and appear once per path
- documentation and comments up to date and to `docs/documentation-standard.md`; `python3 scripts/check-docs.py` covers the mechanical half
- the conventions in `CLAUDE.md` and in each `.claude/rules/` file whose `paths` cover a changed file — read those, since a diff does not load them — and whether each method lives where a caller would look for it; `sbt lint` covers formatting and the ones scalafix enforces

Then run the affected suites, or the full suite when the change is wide.

## 4. Fix, then ask

Fix findings inside the change straight away, then review what the fixes touched: a fix has a blast radius too. A fix that changes the design rather than patching a finding goes back through step 3, `/code-review` included.

Ask before acting on anything you are unsure of, any judgement call, and anything that reaches beyond the change, such as a codebase-wide sweep, a refactor of untouched code or an audit. Those are often deferred to an issue.

## 5. Report

Label every finding **blocking**, **non-blocking**, **nit** or **question**, so the report can be skimmed for what matters.

- what was found and fixed, one line each
- judgement calls and questions, each in plain terms with a recommendation
- what was run, and the result
- for each new test, the fault injected into the code it guards and the failure that followed, so the user can trust a test by its name
- the few places worth the user's own reading, as `file:line`
- a draft commit message, to `/ship`'s rules for one (step 1), so the user has read it before shipping
- for a large change, an offer of a second review by a fresh agent that sees only the diff, the issue and the plan, since this review shares the author's blind spots. It costs more, so the user decides each time.

Leave the changes uncommitted (CLAUDE.md, Git).
