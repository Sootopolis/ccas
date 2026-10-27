---
paths:
  - "**/*.scala"
  - "**/*.sbt"
---

## Syntax

- Braces on `match`.
- Braces, not parens, around a multi-line lambda or deferred ZIO body that is a call's only argument, unless it hugs the parens (`f(_.copy(` … `))`). scalafix's `MultilineBlockBraces` checks it.
- Parens on `if`/`else`; no Scala 3 braceless (indentation-only) syntax.
- Never embed a raw control byte (ESC and friends) in a string literal — use `\uXXXX`.

## Imports

- All imports at the top of the file, never mid-file.
- Import a name rather than spelling out its package at the use site — unless the short name is shadowed (`java.lang.System` under `zio.*`) or partial qualification is the idiom (`mutable.Map`).

## Calls and patterns

- When long arguments that compute something (a nested call, an interpolated string) are all that push a call past the line, consider binding them to values so it fits on one line; otherwise split it, named.
- Name arguments in any call already split one per line, a method as much as a construction — the vertical space is spent, so the names are free, and they matter most for adjacent same-typed `None` / `Some` arguments. Varargs (`assertTrue`, `List`) have no names to give.
- Name every argument of a call or none of them, never a mix — a half-named call reads as if the named ones were the only ones that mattered, while the reader still counts positions for the rest (Python rejects that order outright). So when a bare `true` / `false` / `None` wants a label on a one-line call, name the whole call, splitting one argument per line if it no longer fits.
- A case-class pattern that would list more than three fields, wildcards included, binds the value instead (`case recruit: CliCommand.Recruit =>`) and reads its fields by name: positions break silently when two same-typed fields swap, and the reader counts commas, as in an unnamed call.
- Match each question once. When cases repeat an outer pattern — `Some(a)` / `Some(b)`, or one pattern with and without a guard — because they answer a second question, match it once and branch inside. Rows that are peers of one decision, like `ClubResolver.multi`'s table, stay flat.
- No default argument values unless a caller genuinely needs them: this is an application, not a library.

## Naming

- Suffix an `Option` whose bare name would name the value it holds — an id, a slug, an entity, a row: `clubIdOption`, never `clubId` or `clubIdOpt`, so the unwrapped binding keeps the plain name. A value whose absence is an attribute of the record (`Player.title`) or a default (`misfirePolicy`, `since`) stays bare.
- A renamed persisted or wire field keeps its column and JSON key with `@SqlName` / `@jsonField`.

## Placement

- A method that answers a question about one value goes on that value's type when we own it: `apiClub.adminUsernames`, not `ClubAdmin.extractAdminUsernames(apiClub)`. A helper elsewhere can't be found from the type, so the next caller writes it again. An extension in a class's own companion is a member written the long way (an opaque type has no other way). Exempt: a table's SQL, which its companion holds; a conversion across layers, which goes on the side allowed to know the other (`Club.fromApi`); and a choice one caller makes, such as a report's labels or which cases an algorithm acts on.

## Values

- `Option.when(cond)(value)`, not `if (cond) Some(value) else None`; when the value is itself an `Option`, put the `Option.when` inside a `flatMap` over it rather than trailing `.flatten`.
- Discard with a bare statement (`()` ends a `Unit` block); never `val _ =`, `expr: Unit` or a discarded `Try` (#347).

## ZIO idioms

- Prefer a for-comprehension over a chain of 3+ ZIO combinators, especially inside a `catchAll` body.
- Don't put large blocks inside `fold` / `foldZIO` / `mapBoth` bodies — extract a named method.
- `ZIO.left(x)`, `ZIO.right(x)`, `ZIO.some(x)`, `ZIO.none` and `ZIO.unit`, not `ZIO.succeed` around the same value.
- `ZIO.whenDiscard` when the result is `Unit`.
- Prefer the `when` variants over the `unless` ones and negate the predicate instead — a reader should be able to follow the condition without mentally inverting it.
- Prefer `ZIO.whenZIODiscard(effect)(body)` over `effect.flatMap(b => ZIO.whenDiscard(b)(body))` whenever the bound value is used only as the predicate (`.negate` covers the inverted case); keep the `flatMap` only when the body also needs the value.
- `layer.build *> rest` when a layer is provided purely for its side effect (silences the `ZLayer` macro warning).
