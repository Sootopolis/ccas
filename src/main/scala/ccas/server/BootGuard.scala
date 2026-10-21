package ccas.server

import zio.{UIO, ZIO}

import ccas.utils.TerminalQuestion
import ccas.utils.sql.{DatabaseAddress, PostgresClient}

/** Asks before a boot against a database whose address isn't loopback. A boot's first effects — `ensureTables`, then
  * marking every `Running` job Failed — land before the port binds, so a server started from the wrong shell has done
  * its damage before any status line could be read (#320). Local boots never ask, which keeps a question rare enough
  * to be read. It asks on the terminal ([[TerminalQuestion]]); with none (systemd, cron) a boot goes ahead, since a
  * supervised boot must not wait on an answer nobody can give.
  */
object BootGuard {

  /** The operator answered no. [[CcasServer.serve]] fails with it, and both entry points exit 1 on it without logging
    * an error, since the terminal has already said `aborted`.
    */
  case object Declined extends Exception("boot declined at the database prompt")

  /** True when the boot may go ahead: nothing needed asking, or the operator said yes. Reads the same config the
    * server's pool will, so it must run after the ccas.env overlay.
    *
    * @param detachedChild this process is a `--detach` child, whose parent already asked
    */
  def confirmed(detachedChild: Boolean): UIO[Boolean] =
    PostgresClient.configuredAddress.either.flatMap { resolved =>
      question(resolved, detachedChild).fold(ZIO.succeed(true))(TerminalQuestion.ask(_).map(_.getOrElse(true)))
    }

  /** What to ask before booting, if anything: any address but a loopback one asks, an unreadable one included, unless a
    * parent already asked. Pure.
    *
    * @param resolved what [[PostgresClient.configuredAddress]] gave
    */
  def question(resolved: Either[Throwable, Option[DatabaseAddress]], detachedChild: Boolean): Option[String] =
    resolved match {
      // A config that doesn't resolve fails `PostgresClient.live` before it connects, with its own message.
      case Left(_) => None
      case Right(addressOption) =>
        Option.when(!detachedChild && !addressOption.exists(_.isLoopback)) {
          val target = addressOption.fold("a database whose address can't be read from the configuration")(_.render)
          s"Boot against $target? [y/N] "
        }
    }

}
