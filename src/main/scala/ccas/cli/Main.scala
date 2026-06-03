package ccas.cli

import zio.cli.{CliApp, CliError, HelpDoc}
import zio.{ExitCode, Scope, URIO, ZIO, ZIOAppArgs, ZIOAppDefault}

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
      summary = HelpDoc.Span.text("Chess.com club admin system"),
      command = CliCommand.command
    )(execute)

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIOAppArgs.getArgs.flatMap { args =>
      cliApp.run(args.toList).foldZIO(
        {
          case _: CliError.BuiltIn => exit(ExitCode.success) // --help / --version / completions, already rendered
          case _                   => exit(ExitCode(2))       // parse/validation error, usage already rendered
        },
        {
          case Some(code) => exit(code)
          case None       => exit(ExitCode.success)
        }
      )
    }

  private def execute(cmd: CliCommand): URIO[ZIOAppArgs, ExitCode] = cmd match {
    case _: CliCommand.Serve => ZIO.scoped(CcasServer.run).fold(_ => ExitCode.failure, _ => ExitCode.success)
    case other               => Dispatcher.dispatch(other)
  }
}
