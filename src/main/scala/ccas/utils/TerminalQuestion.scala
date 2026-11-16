package ccas.utils

import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets

import scala.annotation.tailrec
import scala.util.{Try, Using}

import zio.{UIO, ZIO}

/** A yes/no question on the controlling terminal, as `sudo` asks, so redirecting a command's output can't switch it
  * off. Shared by the boot question and `ccas server down`'s, so both ask the same way.
  */
object TerminalQuestion {

  /** The operator's answer to `question`, which gets its `[y/N]` here, or `None` when there is no terminal to ask on
    * (systemd, cron, an agent's shell): what that means is the caller's to decide. A no writes `aborted` to the terminal.
    */
  def ask(question: String): UIO[Option[Boolean]] =
    // On a daemon thread of its own: a blocked terminal read ignores interruption, and at exit ZIOApp waits for every
    // interrupted fiber, so a fiber parked in the read would turn Ctrl-C at the prompt into a hang until Enter.
    ZIO
      .async[Any, Nothing, Option[Boolean]] { callback =>
        val asker = new Thread(() => {
          val answer = askOnTerminal(question)
          callback(ZIO.succeed(answer))
        })
        asker.setDaemon(true)
        asker.start()
      }

  /** Asks `question`, default no, until [[YesNo]] reads an answer, through `readLine` (`None` once input has ended,
    * which is no) and `write`. A no writes `aborted`.
    */
  private[utils] def converse(question: String, readLine: () => Option[String], write: String => Unit): Boolean = {
    @tailrec
    def answer(): Boolean = {
      write(YesNo.prompt(question, default = false))
      readLine().fold(Some(false))(YesNo.read(_, default = false)) match {
        case Some(yes) => yes
        case None =>
          write(YesNo.AskAgain + "\n")
          answer()
      }
    }
    val yes = answer()
    if (!yes) { write("aborted\n") }
    yes
  }

  // `None` when there is no controlling terminal to open. Never throws, since the asking thread must always call back;
  // a terminal that fails mid-question is a no.
  private def askOnTerminal(question: String): Option[Boolean] =
    Try(new RandomAccessFile("/dev/tty", "rw")).toOption.map { terminal =>
      Using(terminal) { tty =>
        converse(question, () => Option(tty.readLine()), text => tty.write(text.getBytes(StandardCharsets.UTF_8)))
      }.getOrElse(false)
    }
}
