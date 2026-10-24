package ccas.rules

import scala.annotation.tailrec
import scala.meta.*
import scala.meta.tokens.Token

import scalafix.v1.*

/** A call's only argument goes in braces, not parens, when it is a lambda whose body spans lines, or a multi-line body
  * passed to a ZIO combinator that runs code later (CLAUDE.md, Scala style). Rewrites `.map(x =>` to `.map { x =>` and
  * `ZIO.whenDiscard(c)(` to `ZIO.whenDiscard(c) {`, and reports the shapes a swap would get wrong for a hand edit. An
  * argument that hugs the parens, as in `f(_.copy(` … `))`, is not a block and is left alone, as is any other by-name
  * argument: a log message or a `ZIO.succeed` result is a value. See docs/adr/0025-conventions-checked-by-scalafix.md.
  */
class MultilineBlockBraces extends SyntacticRule("MultilineBlockBraces") {

  private val Bodies = Set(
    "addFinalizer",
    "attempt",
    "attemptBlocking",
    "attemptBlockingIO",
    "attemptBlockingInterrupt",
    "blocking",
    "interruptible",
    "scoped",
    "suspend",
    "suspendSucceed",
    "uninterruptible"
  )

  private val CurriedBodies = Set(
    "logAnnotate",
    "logSpan",
    "unless",
    "unlessDiscard",
    "unlessZIO",
    "unlessZIODiscard",
    "when",
    "whenDiscard",
    "whenZIO",
    "whenZIODiscard"
  )

  override def fix(implicit doc: SyntacticDocument): Patch =
    doc.tree.collect {
      case apply: Term.Apply if wantsBraces(clause = apply.argClause, body = takesBody(apply.fun)) =>
        braces(apply.argClause)
      case infix: Term.ApplyInfix if wantsBraces(clause = infix.argClause, body = false) => braces(infix.argClause)
    }.asPatch

  private def wantsBraces(clause: Term.ArgClause, body: Boolean): Boolean =
    clause.values match {
      case List(arg) =>
        clause.mod.isEmpty && inParens(clause) && !hugs(clause) &&
          ((isLambda(arg) && spansLines(arg)) || (body && spansLines(clause)))
      case _ => false
    }

  private def isLambda(arg: Term): Boolean =
    arg match {
      case _: Term.Function | _: Term.AnonymousFunction | _: Term.PartialFunction => true
      case block: Term.Block =>
        block.stats match {
          case List(_: Term.Function) => true
          case _                      => false
        }
      case _ => false
    }

  private def takesBody(fun: Term): Boolean =
    unwrapTypeArgs(fun) match {
      case Term.Select(Term.Name("ZIO"), Term.Name(name)) => Bodies.contains(name)
      case curried: Term.Apply =>
        unwrapTypeArgs(curried.fun) match {
          case Term.Select(Term.Name("ZIO"), Term.Name(name)) => CurriedBodies.contains(name)
          case _                                              => false
        }
      case _ => false
    }

  private def unwrapTypeArgs(fun: Term): Term =
    fun match {
      case applyType: Term.ApplyType => applyType.fun
      case _                         => fun
    }

  private def inParens(clause: Term.ArgClause): Boolean =
    clause.tokens.headOption.exists(_.is[Token.LeftParen]) && clause.tokens.lastOption.exists(_.is[Token.RightParen])

  private def spansLines(tree: Tree): Boolean = tree.pos.startLine != tree.pos.endLine

  // Only an inner call spans lines: the bracket closing just before `)` opens an argument list on the line of `(`, as
  // in `f(_.copy(` … `))`. A block, `if` or tuple closing there is not a call.
  private def hugs(clause: Term.ArgClause): Boolean = {
    val tokens   = clause.tokens.toVector
    val inner    = tokens.length - 2
    val argLists = clause.values.head.collect {
      case args: Term.ArgClause  => args.pos.start
      case types: Type.ArgClause => types.pos.start
    }.toSet
    inner > 0 && isCloser(tokens(inner)) &&
    opener(tokens = tokens, at = inner, depth = 0).exists { token =>
      token.pos.startLine == tokens.head.pos.startLine && argLists.contains(token.start)
    }
  }

  @tailrec
  private def opener(tokens: Vector[Token], at: Int, depth: Int): Option[Token] =
    if (at < 0) { None }
    else {
      val token = tokens(at)
      val next  = if (isCloser(token)) { depth + 1 } else if (isOpener(token)) { depth - 1 } else { depth }
      if (next == 0) { Some(token) } else { opener(tokens = tokens, at = at - 1, depth = next) }
    }

  private def isOpener(token: Token): Boolean =
    token.is[Token.LeftParen] || token.is[Token.LeftBrace] || token.is[Token.LeftBracket]

  private def isCloser(token: Token): Boolean =
    token.is[Token.RightParen] || token.is[Token.RightBrace] || token.is[Token.RightBracket]

  // `(` becomes ` {` and `)` becomes `}`, spaced so `.map(x =>` reads `.map { x =>` and `xs map (x =>` reads
  // `xs map { x =>`, and a trailing comma, legal in parens but not in braces, goes.
  private def braces(clause: Term.ArgClause)(implicit doc: SyntacticDocument): Patch =
    byHand(clause) match {
      case Some(reason) =>
        val message = s"Put this argument in braces by hand (CLAUDE.md, Scala style): $reason."
        Patch.lint(Diagnostic(id = "", message = message, position = clause.pos))
      case None =>
        val tokens  = clause.tokens.toVector
        val inside  = tokens.slice(1, tokens.length - 1)
        val before  = if (isBlank(doc.tokenList.prev(tokens.head))) { "" } else { " " }
        val after   = if (inside.headOption.exists(isBlank)) { "" } else { " " }
        val opening = s"$before{$after"
        val closing = if (inside.lastOption.exists(isBlank)) { "}" } else { " }" }
        val last    = inside.reverse.find(token => !isBlank(token) && !token.is[Token.Comment])
        val comma   = last.filter(_.is[Token.Comma])
        Patch.replaceToken(tokens.head, opening) + Patch.replaceToken(tokens.last, closing) +
          comma.fold(Patch.empty)(Patch.removeToken)
    }

  // The shapes a swap gets wrong: the argument's own braces would nest inside the new ones or close beside them
  // (`} }`), a parameter below the `(` would sit under the `{`, and in braces a `(` opening a line starts a statement.
  private def byHand(clause: Term.ArgClause): Option[String] = {
    val arg = clause.values.head
    if (ownBraces(arg) || (arg.tokens.last.is[Token.RightBrace] && arg.pos.endLine == clause.pos.endLine)) {
      Some("it has braces of its own, or its closing brace shares the line of `)`")
    } else if (arg.is[Term.Function] && arg.pos.startLine != clause.pos.startLine) {
      Some("its parameter starts below the `(`")
    } else if (arg.collect { case call: Term.Apply if opensLine(call) => call }.nonEmpty) {
      Some("an argument list opening a line would split from its call")
    } else { None }
  }

  private def ownBraces(arg: Term): Boolean =
    arg match {
      case _: Term.Block | _: Term.PartialFunction => true
      case function: Term.Function =>
        function.body.is[Term.Block] && function.body.tokens.headOption.exists(_.is[Token.LeftBrace])
      case _ => false
    }

  private def opensLine(call: Term.Apply): Boolean =
    call.argClause.tokens.headOption.exists { token =>
      token.is[Token.LeftParen] && token.pos.startLine > call.fun.pos.endLine
    }

  private def isBlank(token: Token): Boolean = token.text.forall(_.isWhitespace)
}
