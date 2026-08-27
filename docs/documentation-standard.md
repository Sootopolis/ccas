# Documentation standard

How CCAS decides *where* a piece of knowledge lives, and *how much* of it to write there.

The rules exist because the alternative was tried: comment volume grew roughly ten times faster than
the code, and the same facts ended up in three places at once, each maintained independently. The
measurements behind that are in [Baseline](#baseline-august-2026), stated once, at the end.

## 1. One fact, one home

Route by the question the reader is asking. Every fact has exactly one home; everywhere else links
to it.

| Reader's question | Home | Rots when |
| --- | --- | --- |
| How do I call this? | Scaladoc on the public member | the signature changes |
| Why is *this line* strange? | `//` at the line | the line changes |
| Why did we pick X over Y? | `docs/adr/NNNN-slug.md` | never — but see currency, below |
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

**Only an ADR is a safe unqualified pointer target**, because only an ADR is immutable: what the
pointer promised is still there. Everything else moves, so a pointer into `README.md` or
`docs/architecture.md` must name the section it means — then a reader who lands on a rewritten page
can tell the target is gone instead of reading the wrong thing.

**Immutability is not currency, and the difference bites.** A superseded ADR still exists, still
resolves, and still reads as a decision, so a bare path into one hands the reader a wrong answer
delivered with confidence — worse than a dead link, because nothing signals the failure. Two rules
close it:

- A superseded ADR's status line must link forward to its successor, and the successor must link
  back. A reader who lands on a dead decision can then always walk to the live one.
- A citation of a superseded ADR from code or from any doc outside `docs/adr/` must say so on the
  citing line. `scripts/check-docs.py` enforces both.

## 2. Never restate a machine-readable fact

Versions, defaults, pool sizes, schema, CLI flags and config keys are already written down in a file
the build reads. Prose copies of them drift silently and are wrong by the next change. Point at the
definition instead.

Two demonstrations, both from this repo:

- `README.md` and `CLAUDE.md` both claimed a Scala and sbt version that `project/Versions.scala` and
  `project/build.properties` had already moved past. `Versions.scala` applies the rule to itself — it
  declines to mirror the sbt version — and the docs simply did not.
- This document's first draft quoted `CLAUDE.md`'s word count. The figure was **correct when
  written** and stale one commit later, when that file was cut. Sourcing was not the problem;
  copying was. That is the whole rule in one example, and it is why the count now lives in a budget
  the checker asserts rather than in a sentence.

## 3. The subtractive test

Before writing a comment, and again when reviewing one: **delete it and ask what the reader has
lost.**

"Nothing, the signature says it" means it was never carrying anything. This single test decides most
of §4 and §5, which are the two ways a comment fails it — carrying nothing the code does not already
say, or carrying so much that it stopped being a comment.

## 4. Let Scala say it

The compiler maintains the code's account of itself and never forgets to update it. Prose has no such
maintainer. So the sharpest reason not to restate a checked fact is not that it is redundant — it is
that **restating a checked fact converts it into an unchecked one.** A comment listing a `sealed
trait`'s variants survives a fourth variant being added; the compiler does not.

Before writing a sentence, check whether a language construct already carries it:

| The code already says | So don't write |
| --- | --- |
| `sealed trait` + its cases | a prose list of the variants |
| `Option[A]` | "returns `None` when absent" |
| `URIO` / `IO[E, A]` | "cannot fail" / "fails with `E`" |
| an opaque type's `validateRaw` | the constraint, restated |
| `private[ccas]` | "internal, not for callers" |
| a default argument | what the default is |
| `given` / `using` | what is supplied, and from where |
| named arguments | a comment labelling the arguments |
| a for-comprehension | a step-by-step narration of the steps |
| `@Table` / `derives DbCodec` | the column mapping |
| an exhaustive `match` | an enumeration of the cases |
| the method name, when precise | the method name, as a sentence |

This is [Ousterhout's](https://blog.pragmaticengineer.com/a-philosophy-of-software-design-review/)
rule that a comment must sit at a different level of detail than the code. The band it leaves is
narrower here than in a language whose type system encodes less: C comments sprawl not because C is
terse but because C cannot encode the constraint, so prose is the only home available. As the type
system widens, the band narrows.

Restatement also scales with block length. One-line scaladoc in this repo is mostly sound; the
retelling appears in the long headers, where a paragraph re-narrates the members immediately below
it — so the budgets in §6 catch most of it on their own.

If a declaration genuinely needs no comment, that is the success case, not a gap.

## 5. What a comment is for

A comment must add information the code lacks — precision below it, or intuition above it.

Write:

- non-obvious **why**: a constraint, a measured result, a bug the shape of the code prevents
- an invariant a caller must uphold, on the member that requires it
- a pointer to the ADR or issue that holds the full story

Do not write:

- **restatements** — see §4
- **worked examples** — they belong in tests, where they are executed and cannot drift
- **speculation** — a future plan goes in an issue, where it can be closed; a comment prescribing an
  implementation that has not happened will be wrong about the shape when it does
- **history** — "used to", "was reverted", a commit hash. Git and the ADR hold this. The one
  exception is a regression test, where the rationale is the test's reason to exist: keep it to two
  lines and name the issue
- **implementation detail in an interface comment** — a caller needs the contract it must uphold,
  not the analysis that produced it

## 6. Budgets

A comment over budget signals that the knowledge belongs somewhere else, not that the comment should
be squeezed.

| Form | Budget | Over budget → |
| --- | --- | --- |
| `//` inline comment | 3 lines | extract a named method, or write an ADR |
| Scaladoc on a member | 10 lines | ADR + one-line pointer |
| Scaladoc on a class / object | 15 lines | ADR, or a `docs/` explanation page |
| `CLAUDE.md` | 2,500 words | move to `docs/`, link from `CLAUDE.md` |
| ADR | 2 pages | the decision is really several decisions |

Budgets count every line of the comment, delimiters included, so a 10-line scaladoc is `/**`, eight
lines of text and `*/`. `@param` / `@return` lines and their continuations do not count — per-field
reference material is exempt on the **shape** of the comment, one line per field with no prose beyond
it, never on what its author calls the class. A category like "DTO" is not marked in Scala and
nothing adjudicates it, so any class could be argued into it after the fact.

**§9's gate deliberately allows more than this table does**, and the gap is the point rather than an
inconsistency — read §9 before concluding a green check means a comment is the right length.

`CLAUDE.md` is loaded into every agent session, so its cost is paid on every task, and long
instruction files measurably degrade adherence to the instructions already in them
([Claude Code best practices](https://code.claude.com/docs/en/best-practices)). The budget is 2,500
rather than a rounder 2,000 because the file sat at 1,994 after its first pass: a budget the next
edit breaches teaches the reader that the numbers are aspirational. 2,500 leaves room for a rule or
two without a rewrite, and the checker asserts it so it cannot drift the way the count in §2 did.
Treat the file as a rules file, not an architecture manual: build commands, conventions a linter
cannot enforce, and links.

## 7. References are the compression mechanism

`(#222)` is not decoration — it is what lets the comment stop at one line.

A reference may only replace an explanation when the target actually holds it, so **write the ADR or
the issue body before trimming the comment that quotes it.** Nothing enforces that ordering: §9's
pointer check confirms the target exists, not that it says anything. This is the right trade — the
alternative is a semantic check nobody can write — but it means the ordering is a review rule, and a
pointer into an empty ADR passes the gate.

## 8. Where the `docs/` files fit

Three of the four files originally in `docs/` were ADRs in everything but name — Problem / Options /
Decision, dated, immutable — and now live in [`docs/adr/`](adr/) with numbers. Use them as the
template for new ones.

The fourth was not an ADR and was not forced into the numbering: `chess-com-client-followups.md`
was a list of parked items, not a decision, so it became issues
[#233](https://github.com/Sootopolis/ccas/issues/233)–[#236](https://github.com/Sootopolis/ccas/issues/236)
and the file was deleted. That is the general rule — a backlog belongs in the tracker, where it can
be closed, and `docs/` holds only things that are finished.

## 9. Enforcement

`scripts/check-docs.py` runs from `.githooks/pre-push` (enable per clone with
`git config core.hooksPath .githooks`). Run it directly with `python3 scripts/check-docs.py`.

1. **Comment-block length** — a block comment over 15 lines, or a run of `//` lines over 8, fails.
2. **`CLAUDE.md` size** — over the §6 word budget fails.
3. **Pointer resolution** — a markdown link, or a `docs/...` path cited from Scala, that does not
   resolve fails.
4. **ADR currency** — a superseded ADR whose status line has no forward link fails, as does a
   citation of one from code or from a doc outside `docs/adr/` that does not say it is superseded.

Rules 1 and 2 are judgement calls; 3 and 4 are correctness. Only rule 1 takes an inline waiver —
`CLAUDE.md`'s budget is one global number rather than a per-case call, so it is changed in the script
deliberately, not waived in passing. See §10.

**The gate is looser than §6 on purpose, and the gap is the point.** A regex cannot tell a member
scaladoc from a class one, and it cannot judge whether five lines of `//` earn their place — so the
ceilings catch essays while §6's numbers stay review rules. They are set off this repo's own
distribution rather than taste (see [Baseline](#baseline-august-2026)), so a failure means something
genuinely new rather than pre-existing debt.

Do not read a green check as "this comment is the right length". Read it as "this comment is not an
essay". Everything else in this document is a review rule.

**Dating a status line.** Take it from git — `git log -S<symbol> --reverse --date=short` on the
symbol the decision introduced — never from memory or from an issue number. Every date in
`docs/adr/` was wrong on first writing for exactly that reason.

## 10. Keeping this from becoming ritual

A standard fails in two directions. It rots, which §9 guards. Or it calcifies — kept because it is
written down, obeyed because disobeying it is more trouble than the contortion it demands. The second
failure is quieter, so it gets the explicit machinery.

**Judgement rules are waivable; correctness rules are not.** A length ceiling is a heuristic standing
in for a judgement a regex cannot make, so a case where it is wrong is expected, not a violation. Put

```scala
* docs-standard: allow long-block -- the 30 codec cases have to be listed in one place
```

anywhere inside the block. The reason is mandatory: an unexplained waiver is indistinguishable from a
bypass and carries nothing into the next review. A broken pointer or a bare citation of a superseded
ADR is never a judgement call, so nothing suppresses those — reach for `--no-verify` and you skip the
test suite too, which is the correct disincentive.

A marker that does not parse, or names a rule that is not waivable, **fails** rather than passing
quietly — a waiver the author believes is in force but that never took is worse than no waiver at all.

**Waivers are the evidence, not the leak.** `python3 scripts/check-docs.py --report` prints the review
agenda: open violations, live measurements against the [Baseline](#baseline-august-2026), every waiver
with its reason and location, and the rules that did not fire.

**Five waivers on one rule is the threshold.** At that point the rule is wrong, not the code: change
the number or delete the rule, and do not add a sixth. The figure is deliberately low, and it is
written here as well as in the script so the document and the tool cannot disagree about it. That is
why a waiver counts rather than merely suppressing — a bypass that leaves no trace makes the next
review an argument from memory.

**The default action of a review is deletion.** Standards only ever grow, because adding a rule feels
like diligence and removing one feels like giving up. Invert it: a rule earns its place by having
fired or been waived. One clean run proves nothing — a healthy tree is exactly when nothing fires —
but a rule that has done neither across several reviews is costing reading attention and buying
nothing. Delete it. The document earns growth; it does not accrue it.

**Nothing here outranks the work.** If following a rule would make a change worse, the rule is wrong
for that case: waive it, say why, and move on. Do not contort code to satisfy this document, and do
not block a change on a documentation rule that a review could settle later.

## 11. When to revisit this

The signal that produced this document was specific and reusable: **compliance high, consumption
zero.** Every comment was written conscientiously and none of them were read, because finding the
one relevant paragraph cost more than re-deriving it from the code.

Watch for that, not for volume. Volume is the symptom; unreadability is the disease, and a document
can rot into unreadability at constant size. When the next revision is prompted, it should be a
diagnosis rather than an accumulation of irritation.

Run `--report` when the question comes up, not on a calendar. A scheduled review with nothing to
decide is itself the ritual this section exists to prevent.

## Baseline (August 2026)

Measured once, when this document was written. Dated deliberately: these are evidence for the rules,
not claims about the present. Re-measure before citing.

- Comment volume in `src/` grew from 1% of lines (March 2026) to 16% of `src/main` (August 2026) —
  roughly ten times faster than the code itself.
- 1,847 comment blocks; 78 blocks of 10 lines or more held 23% of all comment lines. The problem was
  concentrated, not diffuse.
- 46 of 182 files in `src/main` carried no comments at all, and were none the worse for it.
- 88% of `//` blocks were within §6's budget of 3 lines; 99% were within §9's gate ceiling of 8.
- The same rationale — the `BodyStore` attempt-budget fix, the deadline nesting order, the Netty
  tail-noise filter — existed in three places each: source scaladoc, test comment, and `CLAUDE.md`,
  at three different lengths, each independently maintained.
