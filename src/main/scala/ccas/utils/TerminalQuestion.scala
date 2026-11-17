package ccas.utils

import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets

import scala.util.{Try, Using}

import zio.{Exit, Runtime, RuntimeFlag, RuntimeFlags, Task, UIO, Unsafe, ZIO}

/** A yes/no question on the controlling terminal, as `sudo` asks, so redirecting a command's output can't switch it
  * off. Shared by the boot question and `ccas server down`'s, so both ask the same way.
  */
object TerminalQuestion {

  /** The operator's answer to `question`, which gets its `[y/N]` here, or `None` when there is no terminal to ask on
    * (systemd, cron, an agent's shell): what that means is the caller's to decide. A no writes `aborted` to the terminal.
    */
  def ask(question: String): UIO[Option[Boolean]] =
    Abandonable.run(askOnTerminal(question)).orDie

  /** Asks `question`, default no, through [[YesNo.askWith]], reading with `readLine` (`None` once input has ended,
    * which is no) and writing with `write`. A no writes `aborted`.
    */
  private[utils] def converse(
    question: String,
    readLine: Task[Option[String]],
    write: String => Task[Unit]
  ): Task[Boolean] =
    YesNo
      .askWith(YesNo.prompt(question, default = false), default = false, readLine, write, ZIO.succeed(false))
      .tap(yes => ZIO.whenDiscard(!yes)(write("aborted\n")))

  /** Runs `task` to its end on the calling thread. With cooperative yielding off, a task with no async step never moves
    * to a ZIO worker, so the blocking terminal read stays on the asker's own thread, for the reason
    * [[Abandonable.run]] gives.
    */
  private[utils] def runOnThisThread[A](task: Task[A]): Exit[Throwable, A] =
    Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe.run(task.withRuntimeFlags(RuntimeFlags.disable(RuntimeFlag.CooperativeYielding)))
    }

  // `None` when there is no controlling terminal to open. A terminal that fails mid-question is a no, so this never
  // throws and `ask`'s `orDie` never fires.
  private def askOnTerminal(question: String): Option[Boolean] =
    Try(new RandomAccessFile("/dev/tty", "rw")).toOption.map { terminal =>
      Using(terminal) { tty =>
        val readLine            = ZIO.attempt(Option(tty.readLine()))
        def write(text: String) = ZIO.attempt(tty.write(text.getBytes(StandardCharsets.UTF_8)))
        runOnThisThread(converse(question, readLine, write)).getOrElse(_ => false)
      }.getOrElse(false)
    }
}
