# Coding conventions are checked by scalafix local rules

**Status:** Accepted, 2026-10-24 (#333).

## Context

CLAUDE.md's Scala style was checked only by reading the diff, and reading kept missing the same rule. While #331 was being built, several multi-line lambdas went in opened with parens, which is when "braces around a lambda whose body spans lines" was written down. In #349 two multi-line ZIO bodies (`ZIO.whenDiscard(…)(…)`, `ZIO.attempt(…)`) went in the same way, past two reviews against CLAUDE.md and `/code-review`. By then the tree held 177 such blocks. A rule that escapes review three times needs a check that doesn't depend on someone noticing.

## Decision

**Scalafix, with rules local to this build.** It is the Scala Center's linter and rewriter: its rules walk the parsed tree, so they see which argument a clause holds and which bracket opens it, where a text pattern can only guess. A home-grown script would have to re-derive that from text, and would be one more tool to maintain here.

- **Rules live in the single project**, under `src/scalafix`, compiled against scalafix-core's Scala 2.13 build. Scalafix's testkit would need three more sbt projects, which is ADR 0003's trigger for revisiting sbt 2 ("a second module … appears"), so it waits for #333's second rule, when it would serve more than one. sbt-scalafix publishes for sbt 2 as well (`sbt-scalafix_sbt2_3`), so it adds nothing to that migration's cost.
- **The first rule, `MultilineBlockBraces`, is syntactic.** It flags a call's only argument in parens when it is a lambda whose body spans lines, or a multi-line body for a ZIO combinator that runs code later (`ZIO.attempt`, `ZIO.whenDiscard`, `ZIO.addFinalizer` and their kin, listed in the rule). The fix swaps the parens for braces and drops a trailing comma, which braces don't allow.
- **Shapes a swap would get wrong are reported for a hand edit instead.** An argument with braces of its own (`.map(x => {`) or a closing brace beside the `)` would end in nested braces or `} }`; a lambda parameter below the `(` would sit under the `{` with its body indented a level too deep; and inside braces a `(` that opens a line starts a new statement, so a call continued that way would split in two, which can still compile.
- **Not every by-name argument.** Detecting by-name parameters needs SemanticDB, which the compiler would then write on every build, and would also take in `ZIO.succeed`, `ZIO.log*`, `ZIO.fail`, `getOrElse` and `assertTrue`, whose arguments are values: braces around a log message or an assertion read worse.
- **Not an argument that hugs the parens**, as in `f(_.copy(` … `))` or `ZIO.attempt(require(` … `))`: the bracket closing just before `)` opens an argument list on the line of `(`. Only the inner call spans lines there, and the rewrite would leave `) }`. A block or `if` closing there is not a call, so `.map(x => {` … `})` is reported.

## Consequences

- **CI's `lint` job, the `pre-push` hook and `/review-change` run `scalafixAll --check`.** Warm, it takes about a second; the hook runs it in the same sbt session as the tests.
- **A new code-suspending ZIO combinator has to be added to the rule's list**, or a multi-line body passed to it goes unchecked.
- **The rule has no unit tests.** Its cases were checked by fault injection, and its one sweep over the tree (177 blocks in 42 files) was read in review. A rule that flags too much shows up as a CI failure on code that follows the convention; one that flags too little shows up only in review.
- **More rules follow #333's list**, each landing with the tree already clean for it.
