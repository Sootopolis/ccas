package ccas.server

import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets

import scala.util.{Try, Using}

import zio.{UIO, ZIO}

import ccas.utils.sql.{DatabaseAddress, PostgresClient}

/** Asks before a boot against a database whose address isn't loopback. A boot's first effects — `ensureTables`, then
  * marking every `Running` job Failed — land before the port binds, so a server started from the wrong shell has done
  * its damage before any status line could be read (#320). Local boots never ask, which keeps a question rare enough
  * to be read. It asks on the controlling terminal, as `sudo` does, so redirecting a server's output can't switch it
  * off; with no terminal (systemd, cron) there is nobody to ask.
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
    PostgresClient.configuredAddress.either.flatMap(resolved =>
      question(resolved, detachedChild).fold(ZIO.succeed(true))(ask)
    )

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

  /** Only `y` or `yes` boots; a blank answer does not, nor does EOF (`None`). Pure. */
  def accepts(answerOption: Option[String]): Boolean =
    answerOption.exists(answer => Set("y", "yes").contains(answer.trim.toLowerCase))

  // On a daemon thread of its own: a blocked terminal read ignores interruption, and at exit ZIOApp waits for every
  // interrupted fiber, so a fiber parked in the read would turn Ctrl-C at the prompt into a hang until Enter.
  private def ask(question: String): UIO[Boolean] =
    ZIO
      .async[Any, Nothing, Option[Boolean]] { callback =>
        val asker = new Thread(() => {
          val answer = askOnTerminal(question)
          callback(ZIO.succeed(answer))
        })
        asker.setDaemon(true)
        asker.start()
      }
      .map(_.getOrElse(true))

  // `None` when there is no controlling terminal to open. Never throws, since the asking thread must always call back;
  // a terminal that fails mid-question is a no.
  private def askOnTerminal(question: String): Option[Boolean] =
    Try(new RandomAccessFile("/dev/tty", "rw")).toOption.map { terminal =>
      Using(terminal) { tty =>
        tty.write(question.getBytes(StandardCharsets.UTF_8))
        val yes = accepts(Option(tty.readLine()))
        if (!yes) { tty.write("aborted\n".getBytes(StandardCharsets.UTF_8)) }
        yes
      }.getOrElse(false)
    }
}
