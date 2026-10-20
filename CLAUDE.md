# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

This file holds **rules and orientation** — the things you must not get wrong, and enough of a map to find the rest. What each part *is* lives in [`docs/architecture.md`](docs/architecture.md): the packages, the types and database layer, the apps, routes and jobs. *Why* it is that way lives in [`docs/adr/`](docs/adr/), indexed by area in its README. How to install, run and configure it lives in [`README.md`](README.md). See [`docs/documentation-standard.md`](docs/documentation-standard.md) for why the split exists and what belongs where.

## Project Overview

CCAS (Chess Club Admin System) pulls data from the Chess.com public API into PostgreSQL for chess club administration: membership tracking, member performance analysis, and scouting/recruiting players from other clubs. It includes a backend HTTP server with job scheduling, and a CLI.

## Build & Test Commands

An sbt project in Scala 3. Exact versions, the JDK's included, live in `project/Versions.scala`, `project/build.properties` and `.sdkmanrc`; don't mirror them here.

The JDK is pinned in `.sdkmanrc`: run `sdk env` in the repo root, or `sdk env install` on a fresh machine. If a build fails with "Unable to locate a Java Runtime", check `java -version` before anything else.

- **Compile:** `sbt compile`
- **Run all tests:** `sbt test`
- **Run a single test suite:** `sbt "testOnly ccas.api.TestApiJsonParsing"` (fully qualified object name)

Tests use ZIO Test (`ZIOSpecDefault`). SQL tests require a running PostgreSQL instance with a `ccas_test` database (see `src/test/resources/application.conf`). A tracked `pre-push` hook runs the full suite before every push — enable it per clone with `git config core.hooksPath .githooks`.

## Workflow

Every piece of work runs the same sequence. Take each step unprompted; the developer should not have to ask for any of them. These steps and the skills' checklists are a floor, not a ceiling: anything else that looks wrong is worth raising.

1. **Plan**, in plan mode, before writing code for an issue, or for any request that involves a design choice. Show it in the chat, since the developer does not always read GitHub, and in plain terms:
   - the problem as the operator meets it, and its cause
   - the change, and what else it touches
   - the tests by name: a test's name is the guarantee it documents
   - where the schema or stored data change, the way back
   - for a bug that reached stored data, which step of this workflow should have caught it, and the change to that step

   Propose the correct fix over a workaround: with one operator and two databases, migration cost is no reason to defer it. Show each design choice as a worked example with a recommendation, and check the plan's own blast radius before showing it. Wait for agreement (`ExitPlanMode`'s approval; a rejection's notes are the next round), then post the plan to the issue as the record. A plan waived at `/pick-issue` was judged from the issue alone: if the code reveals a design choice, or reaches the schema, `ccas.utils.client`, the cache tables, the CLI tree or what a Chess.com response means, show one after all.
2. **Build.**
3. **Review** with `/review-change`, which fixes what it finds and hands over a report. Then stop (see Git).
4. **Before shipping**: open questions, follow-ups worth an issue, and anything the plan promised that has not landed.
5. **Before the session ends**: stop background tasks, post the agreed issues and comments, save what a future session needs, and say whether the standards should change.

When unsure of anything — a finding, a design choice, what the developer meant — ask rather than guess.

**Say how you know.** A claim about the code, the data or an outside system rests on something read or run in this session — a `file:line`, a command's output, a test name — cited when the developer will act on it; anything unchecked says so. Memory, of the code or of an earlier session, is a lead to check, not a source.

**The standards answer to the work.** This workflow, the skills and these conventions change on evidence from the session: something the developer had to ask for is a gap; a step that keeps finding nothing, or that the developer skips or overrides, is friction; a defect that got past review is an escape. Propose the edit in that session, a cut for friction. An escape of a kind that has happened before gets a mechanical guard — a test, a check in a script, a hook — not another sentence. An established practice from elsewhere is worth proposing when the evidence here shows the gap it closes. "Standards: no change" is the usual answer. Deletion is the default, as in [`docs/documentation-standard.md`](docs/documentation-standard.md), §10.

## Conventions

Rules that aren't derivable from reading the code. Follow them; they exist because the alternative was tried.

**Git.** Never `git commit`, `git push`, or open a PR without the developer's explicit go-ahead **in that message**. Finish the work, run the suite, and leave the changes uncommitted in the working tree; then report and stop. Review happens on the working-tree diff, and committing buries the change behind commit boundaries where it is much harder to examine — a commit message also asserts what a change does while it is still under review. Announcing an intention to commit earlier in the session is not approval; neither is a background-task notification, nor approval of an earlier commit, nor agreement to a plan that contains commits. Approval is per-action: "commit this" is not "push this", and neither is "open a PR". Offer the commit rather than making it, and give a requested sweep across existing code a commit of its own. If a commit has already happened unapproved, say so and offer to `git reset --soft` back to the base branch. Never commit while `wip` is checked out either — cut a feature branch first with `git switch -c <type>/<slug>`, which carries uncommitted work across. `wip` is a local parking branch that must only ever lag `main`; a commit on it turns re-parking from a fast-forward into a judgement call about whether that commit still exists anywhere else.

**Communications.** A commit message, PR body, issue or comment is published when sent, to a public repo, without passing the developer's diff review. Before sending one: check its claims against the code, resolve its references (issues, SHAs, links), write it for a reader who was not in the session, and leave out secrets, session links and local paths. Design discussion goes in the issue it belongs to; comment on an existing issue before filing a near-duplicate (`gh issue list`). Show a new issue, or a comment that records a decision, in the chat first; `/ship`'s backrefs go straight out.

**Documentation.** Follow [`docs/documentation-standard.md`](docs/documentation-standard.md); `scripts/check-docs.py` enforces the mechanical half from `pre-push` and CI, including this file's own word budget. Length ceilings are judgement calls — waive one inline with `docs-standard: allow long-block -- <reason>` rather than contorting the code, and never reach for `--no-verify`. `--report` prints the review agenda. Nothing in that document outranks the work. One fact, one home: scaladoc for how to call a thing, `//` for why a line is strange, an ADR for why X over Y, README for how to run it, the test name for what is guaranteed. Never restate a machine-readable fact (versions, defaults, schema) in prose — point at the definition. Comment budgets: `//` ≤ 3 lines, member scaladoc ≤ 10, class scaladoc ≤ 15. Over budget means the knowledge belongs in an ADR, with a one-line pointer left in the code. Scala already says most of it — do not narrate a `sealed trait`'s variants, an `Option`, or an error channel back to the reader.

**ADRs.** Read the relevant ADR **before** changing anything in `ccas.utils.client`, the cache tables, or the CLI command tree. Each one records a trade-off that was argued and, in several cases, a fix that was tried and reverted.

**Spelling.** British spelling throughout — identifiers, comments, docs, commit messages — except where a third-party name forces American (`zio.Fiber`, not `zio.Fibre`).

**Scala style.** Braces on `match`, parens on `if`/`else`; no Scala 3 braceless (indentation-only) syntax. All imports at the top of the file, never mid-file, and import a name rather than spelling out its package at the use site — unless the short name is shadowed (`java.lang.System` under `zio.*`) or partial qualification is the idiom (`mutable.Map`). Prefer a for-comprehension over a chain of 3+ ZIO combinators, especially inside a `catchAll` body. Don't put large blocks inside `fold` / `foldZIO` / `mapBoth` bodies — extract a named method. Name arguments in any call already split one per line, a method as much as a construction — the vertical space is spent, so the names are free, and they matter most for adjacent same-typed `None` / `Some` arguments. Varargs (`assertTrue`, `List`) have no names to give. Name every argument of a call or none of them, never a mix — a half-named call reads as if the named ones were the only ones that mattered, while the reader still counts positions for the rest (Python rejects that order outright). So when a bare `true` / `false` / `None` wants a label on a one-line call, name the whole call, splitting one argument per line if it no longer fits. Suffix an `Option` whose bare name would name the value it holds — an id, a slug, an entity, a row: `clubIdOption`, never `clubId` or `clubIdOpt`, so the unwrapped binding keeps the plain name. A value whose absence is an attribute of the record (`Player.title`) or a default (`misfirePolicy`, `since`) stays bare. A renamed persisted or wire field keeps its column and JSON key with `@SqlName` / `@jsonField`. `Option.when(cond)(value)`, not `if (cond) Some(value) else None`; when the value is itself an `Option`, put the `Option.when` inside a `flatMap` over it rather than trailing `.flatten`. Discard a value mid-block with a bare statement, never `val _ =` or `expr: Unit`: the build does not enable `-Wnonunit-statement`. No default argument values unless a caller genuinely needs them: this is an application, not a library. Never embed a raw control byte (ESC and friends) in a string literal — use `\uXXXX`.

**ZIO idioms.** `ZIO.whenDiscard` when the result is `Unit`. Prefer the `when` variants over the `unless` ones and negate the predicate instead — a reader should be able to follow the condition without mentally inverting it. Prefer `ZIO.whenZIODiscard(effect)(body)` over `effect.flatMap(b => ZIO.whenDiscard(b)(body))` whenever the bound value is used only as the predicate (`.negate` covers the inverted case); keep the `flatMap` only when the body also needs the value. `layer.build *> rest` when a layer is provided purely for its side effect (silences the `ZLayer` macro warning).

**SQL.** Write methods return the number of rows affected, not `Unit`. Record names once per transaction (`Player.writeBatch` for batches; [0020](docs/adr/0020-claims-on-one-name-queue.md)). Never use `ChessComClient` inside `withTransaction`: the connection and locks would stay held across the round trip, and the client dies if you try (#300). Prefer `UNION ALL` over `UNION` where deduplication isn't needed. Magnum specifics: a custom `DbCodec` must not throw on `null`, because Magnum's `OptionCodec` reads the value *before* checking `wasNull`; and interpolating a bare enum case (`${TriggerType.Interval}`) into `sql""` emits no placeholder and produces a syntax error — widen the value to the enum type first.

**Formatting.** The repo is not scalafmt-clean. Never run `scalafmtAll` / `scalafmtSbt` — it churns ~120 files. Format only the files you touched, and even then check the diff: reformatting a file that was never formatted rewraps comments and reorders imports far beyond your change.

**Tests.** `Test/parallelExecution := false` is deliberate — several suites touch process-global state or a shared schema. Within a suite ZIO Test still runs tests in parallel, so a suite sharing mutable schema or filesystem state needs `@@ TestAspect.sequential`. Fix flakes at the root; `@@ TestAspect.flaky` is not an acceptable remedy. A test of a fiber's cleanup under interruption asserts `exit.isInterruptedOnly`: `isInterrupted` also passes when the cleanup crashed. Tests run against `ccas_test` even with `DATABASE_URL` exported — the test `application.conf` nulls `database.url` — and `FreshSchemaLayer` still refuses to reset a schema in any database not named `*_test`, in case another route reaches production.

**Output files.** Apps that write reports go through `OutputFile` (`ccas.utils`), which owns the layout and archives earlier output. Don't hand-roll paths.

**Chess.com API behaviour.** The API misleads in specific, recorded ways: fields that lie, 404s that are not permanent, closed accounts that vanish from rosters while still members. Before writing code that decides what a response means — what a status, a timestamp, an absence or a 404 says — read [`docs/chess-com-api.md`](docs/chess-com-api.md), and record anything new you observe there, dated. A reported-not-found is an answer, not a failure: the client returns `FetchResult.Missing` and the app decides what absence means ([0019](docs/adr/0019-a-reported-404-is-an-answer.md)).

**Schema changes.** Never key a table on a mutable external label: when Chess.com owns both an id and a name, the id is identity and the name is history ([0016](docs/adr/0016-identity-is-the-id-names-are-observations.md)). The `createTable` definitions in code are the schema of record — there is no migration framework. Apply a change by editing the `CREATE TABLE` and running the equivalent `ALTER` against each existing database by hand; `/migrate` has the steps. `sql/` holds dated scripts from earlier changes and is kept for history; new changes are not scripted there unless the change is intricate enough to be worth replaying. Treat that directory as an archive, not a ledger you must append to.

**Deployment.** One `CcasServer` per database is the supported model; a CLI alongside it on the same database is fine, since the CLI builds no `JobRunner`. Two or more servers against one database are unsupported until #110 and #111 are fixed (#60). The one-server premise is itself under question (#220): keep new coordination in Postgres, and flag any new dependence on a single server.

**Shell completions.** `completions/ccas.bash` is committed and asserted byte-equal to `CompletionEmitter.bash` by `TestCcasCompletion`. Regenerate after any CLI tree change with `sbt --server -batch -error 'runMain ccas.cli.Main completion bash' > completions/ccas.bash`. `--server` is load-bearing: with `SBT_NATIVE_CLIENT=true`, `-batch` alone still routes through the thin client, which writes banner lines and a raw ESC into **stdout**, corrupting the redirect.

**zio-cli argument parsing.** zio-cli swallows an option written *after* a positional as another positional value, and `Args.atMost(n)` silently *truncates* extra values rather than rejecting them. Capture every positional with `.repeat` and validate arity by hand (see `UseClub`) wherever a dropped argument would change what the command does.
