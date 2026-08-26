# Documentation standard

How CCAS decides *where* a piece of knowledge lives, and *how much* of it to write there.

The rules exist because the alternative was tried: comment volume in `src/` grew from 1% of lines
(March 2026) to 16% of `src/main` (August 2026) — roughly ten times faster than the code itself —
and the same facts now appear in up to three places at once, each maintained independently.

## 1. One fact, one home

Route by the question the reader is asking. Every fact has exactly one home; everywhere else links
to it.

| Reader's question | Home | Rots when |
| --- | --- | --- |
| How do I call this? | Scaladoc on the public member | the signature changes |
| Why is *this line* strange? | `//` at the line | the line changes |
| Why did we pick X over Y? | `docs/adr/NNNN-slug.md` | never — ADRs are immutable |
| How do I install / run / configure it? | `README.md` | the feature changes |
| What does this behaviour guarantee? | the test name and its assertions | the behaviour changes |
| What must a contributor or agent never do here? | `CLAUDE.md` | rarely |
| What is the exact value of a version / default / limit? | the code that defines it | — |

The framework behind the split is [Diátaxis](https://diataxis.fr/): reference (scaladoc, README's
API section), how-to (README's task sections), and explanation (ADRs) are different genres with
different lifetimes, and mixing them is what makes a document unmaintainable. Decision records use
[Nygard's lightweight ADR format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

**Never write the same fact twice.** If a rationale is worth a paragraph, it is worth an ADR — and
then the code carries a pointer, not a copy:

```scala
// Attempt budget == total budget, deliberately. See docs/adr/0009-bound-every-body-store-operation.md (#222).
```

**Only an ADR is a safe unqualified pointer target.** ADRs are immutable, so a file path is enough:
what the pointer promised is still there. Everything else moves. A pointer into `README.md` or
`docs/architecture.md` must name the section it means, so a reader who lands on a rewritten page can
tell that the target is gone instead of reading the wrong thing.

## 2. Never restate a machine-readable fact

Versions, defaults, pool sizes, schema, CLI flags and config keys are already written down in a file
the build reads. Prose copies of them drift silently and are wrong by the next bump. Point at the
definition instead.

This is not hypothetical: `README.md` and `CLAUDE.md` both currently claim "Scala 3.8.3, SBT 1.12.8"
while `project/Versions.scala` says `3.8.4` and `project/build.properties` says `1.13.0`.
`Versions.scala` already applies the rule to itself — it declines to mirror the sbt version — and the
docs simply do not.

## 3. Budgets

A comment over budget is a signal that the knowledge belongs somewhere else, not that the comment
should be squeezed.

| Form | Budget | Over budget → |
| --- | --- | --- |
| `//` inline comment | 3 lines | extract a named method, or write an ADR |
| Scaladoc on a member | 10 lines | ADR + one-line pointer |
| Scaladoc on a class / object | 15 lines | ADR, or a `docs/` explanation page |
| `CLAUDE.md` | 2,000 words total | move to `docs/`, link from `CLAUDE.md` |
| ADR | 2 pages | the decision is really several decisions |

Budgets count every line of the comment, delimiters included, so a 10-line scaladoc is `/**`, eight
lines of text and `*/`. `@param` / `@return` blocks on a config or DTO case class are reference
material and are exempt — one line per field, no prose beyond it.

`CLAUDE.md` is loaded into every agent session, so its cost is paid on every task, and long
instruction files measurably degrade adherence to the instructions already in them
([Claude Code best practices](https://code.claude.com/docs/en/best-practices)). It is currently 7,679
words. Treat it as a rules file, not an architecture manual: build commands, conventions that a
linter cannot enforce, and links.

## 4. What a comment is for

Per [Ousterhout](https://blog.pragmaticengineer.com/a-philosophy-of-software-design-review/), a
comment must add information not present in the code — precision below it, or intuition above it. A
comment at the same level of detail as the code it sits on is noise that still has to be maintained.

Write:

- non-obvious **why**: a constraint, a measured result, a bug the shape of the code prevents
- an invariant a caller must uphold, on the member that requires it
- a pointer to the ADR or issue that holds the full story

Do not write:

- **restatements** — see §5
- **worked examples** — they belong in tests, where they are executed. `ApiConcurrency.cappedFor`
  carries a six-row example table that `TestApiConcurrency` already asserts, in more cases.
- **speculation** — future plans go in an issue. `HttpClientLayer`'s HTTP/2 paragraph describes an
  upgrade that has not happened and prescribes code that may not be the right shape when it does.
- **history** — "used to", "was reverted", "see commit 6706d5ef". Git and the ADR hold this. The one
  exception is a regression test, where the rationale is the test's reason to exist: keep it to two
  lines and name the issue.
- **implementation detail in an interface comment** — a caller of `putOrSkip` needs the contract
  ("do not write a pointer row on `false`"), not the orphaned-object analysis.

## 5. Let Scala say it

Scala already encodes most of what a naive comment would say, and it encodes it in a form the
compiler keeps true. Before writing a sentence, check whether a language construct is already
carrying it:

| The code already says | So don't write |
| --- | --- |
| `sealed trait` + its cases | a prose list of the variants |
| `Option[A]` | "returns `None` when absent" |
| `URIO` / `IO[E, A]` | "cannot fail" / "fails with `E`" |
| an opaque type's `validateRaw` | the constraint, restated |
| `private[ccas]` | "internal, not for callers" |
| named arguments | a comment labelling the arguments |
| a for-comprehension | a step-by-step narration of the steps |
| `@Table` / `derives DbCodec` | the column mapping |
| an exhaustive `match` | an enumeration of the cases |
| the method name, when precise | the method name, as a sentence |

The test is subtractive: **delete the comment and ask what the reader has lost.** "Nothing, the
signature says it" means it was never carrying anything. This is Ousterhout's rule that a comment
must sit at a different level of detail than the code — and in an expressive language the code's
level is already high, so the band left for a useful comment is narrower than in Java or Go.

Two consequences worth stating separately:

- **The prose copy is the one that goes wrong.** A comment listing `Fresh`, `Revalidated`,
  `IdenticalBody` above a `sealed trait` survives a fourth variant being added; the compiler does
  not. Restating a checked fact converts it into an unchecked one.
- **Restatement scales with block length.** One-line scaladoc in this repo is mostly sound; the
  retelling appears in the 30-to-60-line headers, where a paragraph re-narrates the members
  immediately below it. Budget (§3) catches most of this on its own.

If a declaration genuinely needs no comment, that is the success case, not a gap. 46 of 182 files in
`src/main` carry no comments at all and are none the worse for it.

## 6. Issue and ADR references are the compression mechanism

`(#222)` is not decoration — it is what lets the comment stop at one line. A reference is only
allowed to replace an explanation if the target actually holds it, so write the ADR or the issue
body *before* trimming the comment that quotes it.

## 7. Where the existing `docs/` fit

Three of the four files already in `docs/` were ADRs in everything but name — Problem / Options /
Decision, dated, immutable — and now live in [`docs/adr/`](adr/) with numbers. Use them as the
template for new ones.

`docs/chess-com-client-followups.md` is the exception and deliberately stays put: it is a list of
four parked items, not a decision, and forcing it into the ADR numbering would make the sequence
mean two different things. Its real home is GitHub issues; until they are filed it is a backlog note
in `docs/`.

## 8. Enforcement

Two things here are mechanically checkable, and both belong in `.githooks/pre-push` (or an sbt task):

1. **Comment-block length** — fail on a block over 15 lines outside `docs/`. Cheap, and it prevents
   the slow return of essays.
2. **Pointer resolution** — fail on a markdown link or a cited `docs/...` path that does not resolve,
   scanning `docs/**` *and* the `docs/adr/...` paths cited from `.scala` files. §6 lets a reference
   replace an explanation only when the target holds it, and nothing else enforces that. It has
   already broken once: renaming `sbt-2-evaluation.md` into `docs/adr/` stranded the link to it in
   ADR 0002, and a throwaway script caught it rather than a gate.

Turn both on only once the existing over-budget blocks are cleared, or every push fails on debt the
committer did not create.

Everything else in this document is a review rule, not a lint rule.

**Dating a status line.** Take it from git — `git log -S<symbol> --reverse --date=short` on the
symbol the decision introduced — never from memory or from an issue number. Every date in
`docs/adr/` was wrong on first writing for exactly that reason.
