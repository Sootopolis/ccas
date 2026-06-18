package ccas.cli

import java.nio.file.Paths

import zio.cli.{CliApp, CliError, HelpDoc}
import zio.{Console, ExitCode, Scope, UIO, URIO, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.cli.config.CliConfig
import ccas.cli.serve.{Detach, PidFile, Stop}
import ccas.info.BuildInfo
import ccas.server.CcasServer

/** Single entry point for the `ccas` binary.
  *
  * `zio-cli` parses argv against the [[CliCommand]] tree, renders `--help`/usage and shell completions, then hands the
  * parsed model to [[execute]]. `serve` boots [[CcasServer]] in this process; every other subcommand is dispatched as a
  * thin HTTP client ([[Dispatcher]]). Exit codes: 0 success / help, 1 job failure, 2 usage error.
  */
object Main extends ZIOAppDefault {

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIOAppArgs.getArgs.flatMap { args =>
      // zio-cli has no built-in --version (its BuiltInOption is help/wizard/completions only), so handle it here,
      // before reading config — `--version` must work regardless of the config file's state.
      if (args.headOption.exists(a => a == "--version" || a == "-V")) {
        Console.printLine(s"ccas ${BuildInfo.version}").orDie *> exit(ExitCode.success)
      } else {
        // Resolve the CLI config first: its `api_url` becomes the `--server` default (built-in fallback if absent), and
        // `default_clubs` seeds completion. A malformed file fails fast with a message pointing at it (exit 2).
        CliConfig.load(XdgPaths.configFile).foldZIO(configLoadFailed, runResolved(args.toList, _))
      }
    }

  private def configLoadFailed(message: String): UIO[Unit] =
    Console.printLineError(s"error: $message").orDie *> exit(ExitCode(2))

  private def runResolved(argList: List[String], cfg: CliConfig): URIO[ZIOAppArgs, Unit] = {
    val cliApp = CliApp.make(
      name = "ccas",
      version = BuildInfo.version,
      summary = HelpDoc.Span.text("Chess Club Admin System"),
      command = CliCommand.command(cfg.apiUrl.getOrElse(CliCommand.DefaultServer)),
      config = CliCommand.config
    )(execute(cfg))
    CompletionCache.seedClubs(cfg.defaultClubs) *>
      cliApp.run(argList).foldZIO(
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

  private def execute(cfg: CliConfig)(cmd: CliCommand): URIO[ZIOAppArgs, ExitCode] = cmd match {
    case CliCommand.Serve(false)         => serve
    case CliCommand.Serve(true)          => detachServe(cfg)
    case CliCommand.Stop                 => Stop.run(PidFile.path)
    case CliCommand.Completion(shell)    => printCompletion(shell)
    case other: CliCommand.ServerCommand => Dispatcher.dispatch(other)
  }

  // Detached serve: same mandatory-env precheck as foreground (a detached child with missing env would just die in
  // server.log), then hand off to the spawner. `log_dir` from the CLI config sets the server's log location, defaulting
  // to `${XDG_STATE_HOME:-~/.local/state}/ccas/logs`.
  private def detachServe(cfg: CliConfig): URIO[ZIOAppArgs, ExitCode] =
    missingServeEnv match {
      case Some(msg) => Console.printLineError(s"error: $msg").orDie.as(ExitCode(2))
      case None =>
        val logDir = cfg.logDir.fold(XdgPaths.stateDir.resolve("logs"))(Paths.get(_))
        Detach.run(logDir, PidFile.path)
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
  private def printCompletion(shell: String): UIO[ExitCode] =
    CompletionEmitter.render(shell) match {
      case Right(script)  => Console.print(script).orDie.as(ExitCode.success)
      case Left(message)  => Console.printLineError(s"error: $message").orDie.as(ExitCode(2))
    }
}
