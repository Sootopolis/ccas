package ccas.cli.serve

import java.nio.file.{Files, Path}

import scala.jdk.OptionConverters.*

import zio.{durationInt, Console, ExitCode, UIO, ZIO}

import ccas.utils.TerminalQuestion
import ccas.utils.errors.safeMessage
import ccas.utils.sql.DatabaseAddress

/** `ccas server down`: read the pid file written by a detached server, send SIGTERM, wait for a clean exit. The
  * server's own shutdown finalizer removes the pid file (and runs the ZIO scope finalizers — Hikari pool close,
  * in-flight job interrupt); `down` removes the file as a backstop. Exits non-zero with a clear message when there's
  * nothing to stop (no pid file, or the recorded pid is dead). Recycled-pid risk is accepted (single-user local
  * model, #40). Before signalling a server whose database isn't loopback it asks, as a boot does ([[stopQuestion]]).
  *
  * `down` only manages *detached* servers, since a foreground `ccas server up` writes no pid file. But `ccas server
  * status` probes the health endpoint and so reports *any* server up — including a foreground one — which makes a
  * bare "no/stale pid file" reply look like `status` and `down` disagree. To close that gap, the three "nothing to
  * signal" branches additionally probe the port themselves and, when a server still answers, point the user at the
  * likely foreground instance the pid file can't reach (see [[foregroundHint]]).
  */
object ServerStop {

  private[cli] val TermTimeout = 20.seconds
  private val KillTimeout      = 5.seconds

  /** The line printed when `down` has no detached server to stop yet a server is still answering `port` — almost
    * always a foreground `ccas server up`, which writes no pid file and so can't be reached via the pid file. Points at
    * the pid to inspect rather than a blind `kill $(…)`, since `lsof` can list more than the one process. Pure so the
    * wording is unit-testable without a live server (same split as [[ServerStatus.describe]]).
    */
  def foregroundHint(port: Int): String =
    s"note: a server is still responding on 127.0.0.1:$port but is not a detached instance this CLI manages " +
      s"(no pid file) — likely a foreground `ccas server up`. Stop it with Ctrl-C in its terminal, or find its pid " +
      s"with `lsof -ti tcp:$port` and kill that."

  /** The database the server on `port` names, by the probe `status` uses. A server that doesn't answer names none. */
  private def serverDatabase(port: Int): UIO[Option[String]] = HealthProbe.readiness(port).map(_.databaseOption)

  /** What to ask before stopping the server on `port`, if anything: any database but a loopback one asks, one the
    * server doesn't name included, as at boot (`BootGuard.question`). A wrong shell's `down` would otherwise stop the
    * server its port leads to without a word. Pure.
    */
  def stopQuestion(port: Int, pid: Long, databaseOption: Option[String]): Option[String] =
    Option.when(!databaseOption.flatMap(DatabaseAddress.fromRendered).exists(_.isLoopback)) {
      s"Stop ${describeServer(port, pid, databaseOption)}? [y/N] "
    }

  private def describeServer(port: Int, pid: Long, databaseOption: Option[String]): String =
    s"the server on 127.0.0.1:$port (pid $pid, ${HealthProbe.databaseLabel(databaseOption)})"

  def run(stateDir: Path, port: Int): UIO[ExitCode] =
    run(
      stateDir = stateDir,
      port = port,
      // same liveness probe `status` uses, so the hint fires exactly when it says "running"
      serverAnswering = HealthProbe.isUp(port),
      serverDatabase = serverDatabase(port),
      ask = TerminalQuestion.ask
    )

  /** The three effects are injected so unit tests exercise every branch with neither a network nor a terminal.
    *
    * @param serverAnswering whether a server still answers on `port`, for the foreground hint
    * @param serverDatabase  the database the server on `port` names, for [[stopQuestion]] and the `stopped` line
    * @param ask             asks the operator a yes/no question, as [[TerminalQuestion.ask]] does
    */
  private[serve] def run(
    stateDir: Path,
    port: Int,
    serverAnswering: UIO[Boolean],
    serverDatabase: UIO[Option[String]],
    ask: String => UIO[Option[Boolean]]
  ): UIO[ExitCode] = {
    val pidPath = PidFile.path(stateDir, port)
    def nothing(message: String, removeFile: Boolean) =
      nothingToStop(
        message = message,
        removeFile = removeFile,
        pidPath = pidPath,
        port = port,
        serverAnswering = serverAnswering
      )
    val program =
      ZIO.attemptBlocking(Files.exists(pidPath)).flatMap {
        case false => nothing(message = s"no detached server running (no pid file at $pidPath)", removeFile = false)
        case true =>
          ZIO.attemptBlocking(PidFile.read(pidPath)).flatMap {
            case None => nothing(message = s"corrupt pid file at $pidPath; removing", removeFile = true)
            case Some(pid) =>
              ZIO.attemptBlocking(ProcessHandle.of(pid).toScala).flatMap {
                case Some(handle) if handle.isAlive =>
                  serverDatabase.flatMap(databaseOption => stopPid(handle, pid, pidPath, port, databaseOption, ask))
                case _ =>
                  nothing(message = s"stale pid file at $pidPath (no live process $pid); removing", removeFile = true)
              }
          }
      }
    program.catchAllCause(c => Console.printLineError(s"error: ${c.squash.safeMessage}").orDie.as(ExitCode(1)))
  }

  // A question nobody is there to answer is a no: a `down` run with no terminal (a script, an agent's shell) is the
  // likeliest to be in the wrong environment, and a boot's reason to go ahead regardless (systemd) doesn't apply here.
  private def stopPid(
    handle: ProcessHandle,
    pid: Long,
    pidPath: Path,
    port: Int,
    databaseOption: Option[String],
    ask: String => UIO[Option[Boolean]]
  ): ZIO[Any, Throwable, ExitCode] = {
    val server = describeServer(port, pid, databaseOption)
    stopQuestion(port, pid, databaseOption).fold(ZIO.some(true))(ask).flatMap {
      case Some(true)  => terminate(handle, pid, pidPath, server)
      case Some(false) => Console.printLineError("not stopped").orDie.as(ExitCode(1))
      case None =>
        val refusal = s"not stopping $server without asking, and there is no terminal to ask on: run it in one"
        Console.printLineError(refusal).orDie.as(ExitCode(1))
    }
  }

  /** Common tail for the three "no live detached server to signal" cases: print the reason, optionally clear the pid
    * file, then — if a server is still answering the port — append [[foregroundHint]]. Always exits 1: `down` stopped
    * nothing it manages, even when a foreground server is up.
    */
  private def nothingToStop(
    message: String,
    removeFile: Boolean,
    pidPath: Path,
    port: Int,
    serverAnswering: UIO[Boolean]
  ): ZIO[Any, Throwable, ExitCode] =
    for {
      _         <- Console.printLineError(message).orDie
      _         <- ZIO.whenDiscard(removeFile)(ZIO.attemptBlocking(PidFile.remove(pidPath)).ignore)
      answering <- serverAnswering
      _         <- ZIO.whenDiscard(answering)(Console.printLineError(foregroundHint(port)).orDie)
    } yield ExitCode(1)

  private def terminate(
    handle: ProcessHandle,
    pid: Long,
    pidPath: Path,
    server: String
  ): ZIO[Any, Throwable, ExitCode] = {
    val removePid = ZIO.attemptBlocking(PidFile.remove(pidPath)).ignore
    for {
      _      <- ZIO.attemptBlocking(handle.destroy()) // SIGTERM → ZIO shutdown hook → clean finalisers
      exited <- ZIO.fromCompletableFuture(handle.onExit()).timeout(TermTimeout)
      code <- exited match {
        case Some(_) => removePid *> Console.printLine(s"stopped $server").orDie.as(ExitCode.success)
        case None =>
          for {
            _ <- Console.printLineError(
              s"pid $pid did not exit within ${TermTimeout.getSeconds}s; sending SIGKILL"
            ).orDie
            _ <- ZIO.attemptBlocking(handle.destroyForcibly())
            _ <- ZIO.fromCompletableFuture(handle.onExit()).timeout(KillTimeout).ignore
            _ <- removePid
            _ <- Console.printLine(s"stopped $server, forced").orDie
          } yield ExitCode.success
      }
    } yield code
  }
}
