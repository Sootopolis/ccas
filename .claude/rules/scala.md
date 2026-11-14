---
paths:
  - "**/*.scala"
  - "**/*.sbt"
---

## Syntax

- Braces on `match`.
- Braces for a multi-line lambda or deferred effect that is a call's only argument (`f { value => ... }`, `ZIO.suspendSucceed { ... }`); a one-argument lambda that closes the call may stay in parens (`f(_.copy(...))`). scalafix's `MultilineBlockBraces` checks this.
- Parens on `if`/`else`; no Scala 3 braceless (indentation-only) syntax.
- Never embed a raw control byte (ESC and friends) in a string literal — use `\uXXXX`.

## Imports

- All imports at the top of the file.
- Import a name rather than spelling out its package at the use site, unless the short name is shadowed (`java.lang.System` under `zio.*`) or partial qualification is the idiom (`mutable.Map`).

## Calls and patterns

- If long arguments push a call past the line, bind them to values where that reads better, or split the call, if need be before the method selection. Length alone is no reason for labels.
- Prefer positional arguments where the method, values and order make the call clear. Label a role that would otherwise be ambiguous: a bare boolean, adjacent same-typed values, a record-like constructor. An identifier or lambda pattern (`ctx.clubId`, `unchanged => ...`) may already name it, and familiar calls (`require(condition, message)`, `foldZIO(error => ..., value => ...)`) read best positionally. Mix named and positional where only some labels help. Varargs have no parameter names.
- Positions say nothing of what a part is, and two same-typed parts swap silently. So a case-class pattern of more than three fields, wildcards included, binds the value (`case recruit: CliCommand.Recruit =>`) and reads its fields by name, as may one with fewer that are easy to confuse. Likewise, prefer a case class to a tuple that a caller reads apart, the more so with more parts, same-typed parts or distance from where it is built; unpacking `<&>` / `zipPar` of independent effects stays a tuple, each name beside its source.
- Match each question once. When cases repeat an outer pattern — `Some(a)` / `Some(b)`, or one pattern with and without a guard — to answer a second question, match it once and branch inside. Rows that are peers of one decision, like `ClubResolver.multi`'s table, stay flat.
- No default argument values unless a caller genuinely needs them: this is an application, not a library.

## Naming

- Suffix an `Option` whose bare name would name the value it holds (an id, a slug, an entity, a row): `clubIdOption`, never `clubId` or `clubIdOpt`, so the unwrapped binding keeps the plain name. A value whose absence is an attribute of the record (`Player.title`) or a default (`misfirePolicy`, `since`) stays bare.
- A renamed persisted or wire field keeps its column and JSON key with `@SqlName` / `@jsonField`.

## Placement

- A method that answers a question about one value goes on that value's type when we own it: `apiClub.adminUsernames`, not `ClubAdmin.extractAdminUsernames(apiClub)`, since a helper elsewhere can't be found from the type and gets written again. An extension in a class's own companion is a member written the long way (an opaque type has no other way). Exempt: a table's SQL, which its companion holds; a conversion across layers, on the side allowed to know the other (`Club.fromApi`); and a choice one caller makes, such as a report's layout or which cases an algorithm acts on.

## Values

- `Option.when(cond)(value)`, not `if (cond) Some(value) else None`; for a value that is itself an `Option`, `Option.when` inside a `flatMap` over it, not a trailing `.flatten`.
- Discard with a bare statement (`()` ends a `Unit` block); never `val _ =`, `expr: Unit` or a discarded `Try` (#347).

## ZIO idioms

- Prefer a for-comprehension to a chain of 3+ ZIO combinators, especially in a `catchAll` body.
- Don't put large blocks inside `fold` / `foldZIO` / `mapBoth` bodies; extract a named method.
- `ZIO.left(x)`, `ZIO.right(x)`, `ZIO.some(x)`, `ZIO.none` and `ZIO.unit`, not `ZIO.succeed` around the same value.
- `ZIO.whenDiscard` when the result is `Unit`, and `ZIO.whenZIODiscard(effect)(body)` rather than `effect.flatMap(b => ZIO.whenDiscard(b)(body))` when the bound value is only the predicate.
- `when` rather than `unless`, negating the predicate (`.negate` on an effect), so the condition reads without inverting it.
- `layer.build *> rest` when a layer is provided purely for its side effect (silences the `ZLayer` macro warning).
