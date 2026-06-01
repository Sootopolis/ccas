package ccas.cli

import zio.{Console, ExitCode, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.server.CcasServer

/** Single entry point for the `ccas` binary.
  *
  * Dispatches on the first argument: `serve` boots [[CcasServer]]; every other command is a
  * placeholder until the decline subcommand tree lands (Sootopolis/ccas#46).
  */
object Main extends ZIOAppDefault {

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIOAppArgs.getArgs.flatMap { args =>
      args.headOption match {
        case Some("serve") => CcasServer.run
        case other         => notImplemented(other)
      }
    }

  private def notImplemented(command: Option[String]): ZIO[Any, Nothing, Unit] = {
    val label = command.getOrElse("<none>")
    val msg   = s"ccas: command '$label' not implemented yet (known commands: serve)"
    Console.printLineError(msg).orDie *> exit(ExitCode.failure)
  }
}
