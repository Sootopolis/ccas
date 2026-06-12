package ccas.cli

import zio.cli.{CliApp, CliError, HelpDoc}
import zio.{Console, ExitCode, Scope, URIO, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.info.BuildInfo
import ccas.server.CcasServer

/** Single entry point for the `ccas` binary.
  *
  * `zio-cli` parses argv against the [[CliCommand]] tree, renders `--help`/usage and shell completions, then hands the
  * parsed model to [[execute]]. `serve` boots [[CcasServer]] in this process; every other subcommand is dispatched as a
  * thin HTTP client ([[Dispatcher]]). Exit codes: 0 success / help, 1 job failure, 2 usage error.
  */
object Main extends ZIOAppDefault {

  private val cliApp: CliApp[ZIOAppArgs, Nothing, ExitCode] =
    CliApp.make(
      name = "ccas",
      version = BuildInfo.version,
      summary = HelpDoc.Span.text("Chess Club Admin System"),
      command = CliCommand.command,
      config = CliCommand.config
    )(execute)

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIOAppArgs.getArgs.flatMap { args =>
      // zio-cli has no built-in --version (its BuiltInOption is help/wizard/completions only), so handle it here.
      if (args.headOption.exists(a => a == "--version" || a == "-V")) {
        Console.printLine(s"ccas ${BuildInfo.version}").orDie *> exit(ExitCode.success)
      } else {
        cliApp.run(args.toList).foldZIO(
          {
            case _: CliError.BuiltIn => exit(ExitCode.success) // --help / completions, already rendered
            case _                   => exit(ExitCode(2))       // parse/validation error, usage already rendered
          },
          {
            case Some(code) => exit(code)
            case None       => exit(ExitCode.success)
          }
        )
      }
    }

  private def execute(cmd: CliCommand): URIO[ZIOAppArgs, ExitCode] = cmd match {
    case _: CliCommand.Serve          => serve
    case CliCommand.Completion(shell) => printCompletion(shell)
    case other                        => Dispatcher.dispatch(other)
  }

  private def serve: URIO[ZIOAppArgs, ExitCode] =
    missingServeEnv match {
      // Known missing config → a one-line message, not the raw ConfigException stack from ConfigFactory.load.
      case Some(msg) => Console.printLineError(s"error: $msg").orDie.as(ExitCode(2))
      // `.fold(_ => failure, _ => success)` would be the deprecated `ZIO#exitCode`, which swallows the cause; CcasServer
      // logs nothing itself, so map via foldCauseZIO and log the cause — else an unexpected crash exits with no diagnostic.
      case None =>
        ZIO
          .scoped(CcasServer.run)
          .foldCauseZIO(
            cause => ZIO.logErrorCause("ccas server exited abnormally", cause).as(ExitCode.failure),
            _ => ZIO.succeed(ExitCode.success)
          )
    }

  // Mandatory server config comes from the process env (the staged binary doesn't load `.env`). Surface the first
  // missing piece as a clean message instead of letting `ConfigFactory.load` throw an UnresolvedSubstitution stack.
  private def missingServeEnv: Option[String] = {
    def unset(name: String): Boolean = sys.env.get(name).forall(_.trim.isEmpty)
    if (unset("CCAS_CONTACT_EMAIL")) {
      Some(
        "CCAS_CONTACT_EMAIL is not set (required for the Chess.com API User-Agent). " +
          "Export it before 'ccas serve' — see README → Configuration."
      )
    } else if (unset("DATABASE_URL") && unset("DB_NAME")) {
      Some(
        "no database configured. Set DATABASE_URL, or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD. " +
          "See README → Configuration."
      )
    } else {
      None
    }
  }

  // Pure, offline: emit the script to stdout (so `eval "$(ccas completion bash)"` works), errors to stderr. No server.
  private def printCompletion(shell: String): URIO[Any, ExitCode] =
    CompletionEmitter.render(shell) match {
      case Right(script)  => Console.print(script).orDie.as(ExitCode.success)
      case Left(message)  => Console.printLineError(s"error: $message").orDie.as(ExitCode(2))
    }
}
