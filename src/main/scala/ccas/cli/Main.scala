package ccas.cli

import java.nio.file.{Path, Paths}

import zio.{Cause, Console, Duration, ExitCode, IO, Scope, UIO, URIO, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.cli.{CliApp, CliError, HelpDoc}

import ccas.cli.config.CliConfig
import ccas.cli.serve.{Detach, HealthProbe, ServerList, ServerStatus, ServerStop}
import ccas.info.BuildInfo
import ccas.server.{BootGuard, CcasServer}
import ccas.server.config.ServerEnvOverlay

/** Single entry point for the `ccas` binary.
  *
  * `zio-cli` parses argv against the [[CliCommand]] tree, renders `--help`/usage and shell completions, then hands the
  * parsed model to [[execute]]. `server up` boots [[CcasServer]] in this process; every other subcommand is
  * dispatched as a thin HTTP client ([[Dispatcher]]). Exit codes: 0 success / help, 1 job failure, 2 usage error.
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

  // `server up` runs the server in this JVM, a `--detach` child included, so its limit is the server's; `CcasServer`'s
  // override says why. Every other command shares it: a client stuck on exit gives up too, instead of waiting forever.
  override def gracefulShutdownTimeout: Duration = CcasServer.gracefulShutdownTimeout

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
          case _                   => exit(ExitCode(2))      // parse/validation error, usage already rendered
        },
        {
          case Some(code) => exit(code)
          case None       => exit(ExitCode.success)
        }
      )
  }

  private def execute(cfg: CliConfig)(cmd: CliCommand): URIO[ZIOAppArgs, ExitCode] = cmd match {
    // `Serve(false, Some(_))` never reaches here: `CliCommand.serverUpOptions` rejects it at parse time.
    case CliCommand.Serve(false, _)          => serve
    case CliCommand.Serve(true, timeout)     => detachServe(cfg, timeout)
    case CliCommand.Stop(portOption)         => stopServe(portOption)
    case CliCommand.ServerStatus(portOption) => statusServe(portOption)
    case CliCommand.ServerList               => listServe
    case CliCommand.Completion(shell)        => printCompletion(shell)
    case CliCommand.Use(slugs, clear)        =>
      // Same server the `--server`-less commands resolve to (config `api_url`, else the built-in default): use-club has
      // no `--server` flag of its own — its network use is a best-effort probe against the configured server, not a
      // server operation — so it inherits the same default the command tree was built with.
      UseClub.run(slugs, clear, cfg.currentClubOption, cfg.apiUrl.getOrElse(CliCommand.DefaultServer))
    case CliCommand.ConfigGet(key)       => ConfigCommand.get(key)
    case CliCommand.ConfigSet(key, v)    => ConfigCommand.set(key, v)
    case CliCommand.ConfigUnset(key)     => ConfigCommand.unset(key)
    case CliCommand.ConfigList(secrets)  => ConfigCommand.list(secrets)
    case CliCommand.ConfigPath           => ConfigCommand.path
    case CliCommand.ConfigInit           => ConfigCommand.init
    case other: CliCommand.ServerCommand => Dispatcher.dispatch(other, cfg.currentClubOption)
  }

  // Detached serve: apply the ccas.env overlay first, so a file-only SERVER_PORT reaches the port this parent JVM
  // resolves (it picks the pid file, the log and the readiness probe), then the foreground's mandatory-config precheck.
  // The detached child re-runs the overlay in its own JVM.
  private def detachServe(cfg: CliConfig, readyTimeoutSeconds: Option[Int]): URIO[ZIOAppArgs, ExitCode] =
    applyServerEnv(XdgPaths.serverEnvFile) *> ZIO.suspendSucceed {
      (missingServeEnv, Detach.resolveDeadline(readyTimeoutSeconds, cfg.readyTimeoutSeconds)) match {
        case (Some(msg), _) => Console.printLineError(s"error: $msg").orDie.as(ExitCode(2))
        // A non-positive deadline is a usage error (exit 2), from the flag or the config file alike — not a failed boot.
        case (None, Left(msg)) => Console.printLineError(s"error: $msg").orDie.as(ExitCode(2))
        case (None, Right(deadline)) =>
          val logDir = cfg.logDir.fold(XdgPaths.stateDir.resolve("logs"))(Paths.get(_))
          onServerPort(HealthProbe.resolvePort)(port => Detach.run(logDir, XdgPaths.stateDir, port, deadline))
      }
    }

  /** `server status`: the port picks both the probe and the pid file. Unlike `serve`, no mandatory-config precheck —
    * status only needs the port, not CCAS_CONTACT_EMAIL / a database.
    */
  private def statusServe(portOption: Option[Int]): URIO[ZIOAppArgs, ExitCode] =
    onServerPort(serverPort(portOption, XdgPaths.serverEnvFile))(port => ServerStatus.run(XdgPaths.stateDir, port))

  /** `server down`: the port picks the pid file, and the port the "still responding?" hint
    * (ServerStop.foregroundHint) probes — a wrong one could stop another port's server.
    */
  private def stopServe(portOption: Option[Int]): URIO[ZIOAppArgs, ExitCode] =
    onServerPort(serverPort(portOption, XdgPaths.serverEnvFile))(port => ServerStop.run(XdgPaths.stateDir, port))

  /** `server list`: the shell's port is the row it marks, and listed with no pid file when a server answers there. */
  private def listServe: URIO[ZIOAppArgs, ExitCode] =
    onServerPort(serverPort(None, XdgPaths.serverEnvFile))(port => ServerList.run(XdgPaths.stateDir, port))

  // For the commands above, which don't run the server in this JVM: a file that can't be read is a CLI warning, on
  // stderr like every other. `serve` logs it through the server's display instead.
  private[cli] def applyServerEnv(file: Path): UIO[Unit] =
    ServerEnvOverlay(file).unit.catchAll(message => Console.printLineError(s"warning: $message").orDie)

  /** The port `server status`, `down` and `list` act on: `--port` as given, else this shell's, with the ccas.env
    * overlay applied first so a file-only `SERVER_PORT` counts. With `--port`, `envFile` isn't read: these commands
    * consult it for the port alone.
    */
  private[cli] def serverPort(portOption: Option[Int], envFile: Path): IO[String, Int] =
    portOption.fold(applyServerEnv(envFile) *> HealthProbe.resolvePort)(ZIO.succeed(_))

  /** An unreadable port is a usage error, never the default's: the port picks which server's pid file is used (#326). */
  private def onServerPort(port: IO[String, Int])(withPort: Int => UIO[ExitCode]): UIO[ExitCode] =
    port.foldZIO(msg => Console.printLineError(s"error: $msg").orDie.as(ExitCode(2)), withPort)

  // Apply the ccas.env overlay (file → system properties for any env var not already set) BEFORE checking mandatory
  // config or booting CcasServer, so the server can boot from `ccas config`'s file without hand-exported env vars.
  // `suspendSucceed` defers `missingServeEnv` until after the overlay has run (it reads the system properties just set).
  private def serve: URIO[ZIOAppArgs, ExitCode] =
    CcasServer
      .withServerEnv(XdgPaths.serverEnvFile) {
        ZIO.suspendSucceed {
          missingServeEnv match {
            // Known missing config → a one-line message, not the raw ConfigException stack from ConfigFactory.load.
            case Some(msg) => Console.printLineError(s"error: $msg").orDie.as(ExitCode(2))
            case None      => ZIO.scoped(CcasServer.serve).as(ExitCode.success)
          }
        }
      }
      // `.fold(_ => failure, _ => success)` would be the deprecated `ZIO#exitCode`, which swallows the cause; CcasServer
      // logs nothing itself, so `serverExit` does. Outside the display, since a crash's cause is the diagnostic and the
      // display truncates causes.
      .foldCauseZIO(serverExit, ZIO.succeed(_))

  // A declined boot prompt is the operator's answer, already acknowledged on the terminal, not a crash to log.
  private def serverExit(cause: Cause[Any]): UIO[ExitCode] =
    cause.failureOption match {
      case Some(BootGuard.Declined) => ZIO.succeed(ExitCode(1))
      case _                        => ZIO.logErrorCause("ccas server exited abnormally", cause).as(ExitCode.failure)
    }

  // Mandatory server config resolves from the process env OR the ccas.env overlay (applied as system properties by
  // `ServerEnvOverlay` before this runs). Surface the first missing piece as a clean message instead of letting
  // `ConfigFactory.load` throw an UnresolvedSubstitution stack.
  private def missingServeEnv: Option[String] = {
    def unset(name: String): Boolean =
      sys.env.get(name).orElse(Option(System.getProperty(name))).forall(_.trim.isEmpty)
    if (unset("CCAS_CONTACT_EMAIL")) {
      Some(
        "CCAS_CONTACT_EMAIL is not set (required for the Chess.com API User-Agent). " +
          "Set it with 'ccas config init' (or 'ccas config set'), or export it — see README → Configuration."
      )
    } else if (unset("DATABASE_URL") && unset("DB_NAME")) {
      Some(
        "no database configured. Set DATABASE_URL, or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD, " +
          "via 'ccas config init'. See README → Configuration."
      )
    } else {
      None
    }
  }

  // Pure, offline: emit the script to stdout (so `eval "$(ccas completion bash)"` works), errors to stderr. No server.
  private def printCompletion(shell: String): UIO[ExitCode] =
    CompletionEmitter.render(shell) match {
      case Right(script) => Console.print(script).orDie.as(ExitCode.success)
      case Left(message) => Console.printLineError(s"error: $message").orDie.as(ExitCode(2))
    }
}
