package ccas.server

import zio.{UIO, URIO, ZIO}

import ccas.utils.TerminalQuestion
import ccas.utils.config.Settings
import ccas.utils.sql.{DatabaseAddress, DatabaseConfig, PostgresClient}

/** Asks before a boot against a database whose address isn't loopback. A boot's first effects — `ensureTables`, then
  * marking every `Running` job Failed — land before the port binds, so a server started from the wrong shell has done
  * its damage before any status line could be read (#320). Local boots never ask, which keeps a question rare enough
  * to be read. It asks on the terminal ([[TerminalQuestion]]); with none (systemd, cron) a boot goes ahead, since a
  * supervised boot must not wait on an answer nobody can give.
  */
object BootGuard {

  /** The boot stopped before doing anything: the operator answered no, or it can't listen on its port.
    * [[CcasServer.serve]] fails with it once the reason has been given, so both entry points exit 1 on it without
    * logging an error.
    */
  case object Refused extends Exception("boot refused before it started")

  /** True when the boot may go ahead: nothing needed asking, or the operator said yes.
    *
    * @param database the configuration the server's pool will be built from
    * @param detachedChild this process is a `--detach` child, whose parent already asked
    */
  def confirmed(database: DatabaseConfig, detachedChild: Boolean): UIO[Boolean] =
    question(PostgresClient.configuredAddress(database), detachedChild)
      .fold(ZIO.succeed(true))(TerminalQuestion.ask(_).map(_.getOrElse(true)))

  /** [[confirmed]] on the `database` section of the [[Settings]]. A section that doesn't load asks nothing, as an
    * address that doesn't resolve doesn't ([[question]]): the pool then fails at its own boot step, which names it.
    */
  def confirmedFor(detachedChild: Boolean): URIO[Settings, Boolean] =
    DatabaseConfig.load.foldZIO(_ => ZIO.succeed(true), confirmed(_, detachedChild))

  /** What to ask before booting, if anything: any address but a loopback one asks, an unreadable one included, unless a
    * parent already asked. Pure.
    *
    * @param resolved what [[PostgresClient.configuredAddress]] gave
    */
  def question(resolved: Either[String, Option[DatabaseAddress]], detachedChild: Boolean): Option[String] =
    resolved match {
      // A config that doesn't resolve fails `PostgresClient.live` before it connects, with its own message.
      case Left(_) => None
      case Right(addressOption) =>
        Option.when(!detachedChild && !addressOption.exists(_.isLoopback)) {
          val target = addressOption.fold("a database whose address can't be read from the configuration")(_.render)
          s"Boot against $target?"
        }
    }

}
