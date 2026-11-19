# Contributing

How to set up a clone for working on the code. Installing and running CCAS is in the [README](README.md); the rules for changing it are in [`CLAUDE.md`](CLAUDE.md) and, per kind of file, [`.claude/rules/`](.claude/rules/).

## Setup

Follow the README's Getting Started first, then, once per clone:

```bash
# Enable the tracked git hooks (pre-push runs the docs check and `sbt lint`, then the full test suite)
git config core.hooksPath .githooks

# Have git blame skip the commits that only reformatted the tree
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

Neither setting is carried by `git clone`, so set both on every machine you work from — otherwise pushes skip the pre-push test run silently, and `git blame` credits every reformatted line to the reformat.

## Everyday commands

```bash
# Run tests (requires the ccas_test database from docker compose)
sbt test

# After a deliberate change to a report's wording, rewrite its snapshots from the failed run, then read their diff
sbt snapshot4sPromote

# Check formatting (scalafmt) and the coding conventions (scalafix), as CI does
sbt lint

# Apply both tools' fixes
sbt lintFix
```

## IntelliJ

Set the Scala code style's formatter to Scalafmt (Settings → Editor → Code Style → Scala), so that Reformat Code runs scalafmt with `.scalafmt.conf`. Optimize Imports is IntelliJ's own and may order imports differently; `sbt lint` catches any it leaves.

**Phantom `X is not a member of ccas` compile errors, or tests failing with `error while loading …tasty` or `relation … does not exist`.** IntelliJ's sbt shell and a terminal `sbt` share `target/`, and concurrent use corrupts the Zinc incremental-compilation state; IntelliJ's own compile server and test runs share `target/` and the `ccas_test` database too. Run one at a time; `sbt clean` recovers. A failure that repeats with IntelliJ closed is real.
