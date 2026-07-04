package ccas.cli.serve

import java.nio.file.{Files, Path}

import scala.jdk.OptionConverters.*

import zio.{Console, durationInt, ExitCode, UIO, ZIO}

/** `ccas server down`: read the pid file written by a detached server, send SIGTERM, wait for a clean exit. The
  * server's own shutdown finalizer removes the pid file (and runs the ZIO scope finalizers — Hikari pool close,
  * in-flight job interrupt); `down` removes the file as a backstop. Exits non-zero with a clear message when there's
  * nothing to stop (no pid file, or the recorded pid is dead). Recycled-pid risk is accepted (single-user local
  * model, #40).
  *
  * `down` only manages *detached* servers, since a foreground `ccas server up` writes no pid file. But `ccas server
  * status` probes the health endpoint and so reports *any* server up — including a foreground one — which makes a
  * bare "no/stale pid file" reply look like `status` and `down` disagree. To close that gap, the three "nothing to
  * signal" branches additionally probe the port themselves and, when a server still answers, point the user at the
  * likely foreground instance the pid file can't reach (see [[foregroundHint]]).
  */
object Stop {

  private val TermTimeout = 20.seconds
  private val KillTimeout = 5.seconds

  /** The line printed when `down` has no detached server to stop yet a server is still answering `port` — almost
    * always a foreground `ccas server up`, which writes no pid file and so can't be reached via the pid file. Points at
    * the pid to inspect rather than a blind `kill $(…)`, since `lsof` can list more than the one process. Pure so the
    * wording is unit-testable without a live server (same split as [[Status.describe]]). */
  def foregroundHint(port: Int): String =
    s"note: a server is still responding on 127.0.0.1:$port but is not a detached instance this CLI manages " +
      s"(no pid file) — likely a foreground `ccas server up`. Stop it with Ctrl-C in its terminal, or find its pid " +
      s"with `lsof -ti tcp:$port` and kill that."

  /** The port of a server still answering `/health`, else None. Split out as the seam [[run]] threads through so the
    * pid-file logic is testable without touching the network (tests pass a fixed `Some`/`None`). */
  private def liveServerPort: UIO[Option[Int]] =
    for {
      port <- HealthProbe.resolvePort
      up   <- HealthProbe.isUp(port) // same liveness probe `status` uses, so the hint fires exactly when it says "running"
    } yield Option.when(up)(port)

  def run(pidPath: Path): UIO[ExitCode] = run(pidPath, liveServerPort)

  /** @param liveServerPort effect yielding the port of any still-running server (for the foreground hint); injected so
    *                       unit tests exercise the pid-file branches without a real probe. */
  private[serve] def run(pidPath: Path, liveServerPort: UIO[Option[Int]]): UIO[ExitCode] = {
    val program =
      ZIO.attemptBlocking(Files.exists(pidPath)).flatMap {
        case false =>
          nothingToStop(s"no detached server running (no pid file at $pidPath)", removeFile = false, pidPath, liveServerPort)
        case true =>
          ZIO.attemptBlocking(PidFile.read(pidPath)).flatMap {
            case None      => nothingToStop(s"corrupt pid file at $pidPath; removing", removeFile = true, pidPath, liveServerPort)
            case Some(pid) => stopPid(pid, pidPath, liveServerPort)
          }
      }
    program.catchAllCause(c => Console.printLineError(s"error: ${rootMessage(c.squash)}").orDie.as(ExitCode(1)))
  }

  private def stopPid(pid: Long, pidPath: Path, liveServerPort: UIO[Option[Int]]): ZIO[Any, Throwable, ExitCode] =
    ZIO.attemptBlocking(ProcessHandle.of(pid).toScala).flatMap {
      case Some(handle) if handle.isAlive => terminate(handle, pid, pidPath)
      case _ =>
        nothingToStop(s"stale pid file at $pidPath (no live process $pid); removing", removeFile = true, pidPath, liveServerPort)
    }

  /** Common tail for the three "no live detached server to signal" cases: print the reason, optionally clear the pid
    * file, then — if a server is still answering the port — append [[foregroundHint]]. Always exits 1: `down` stopped
    * nothing it manages, even when a foreground server is up. */
  private def nothingToStop(
    message: String,
    removeFile: Boolean,
    pidPath: Path,
    liveServerPort: UIO[Option[Int]]
  ): ZIO[Any, Throwable, ExitCode] =
    for {
      _    <- Console.printLineError(message).orDie
      _    <- ZIO.whenDiscard(removeFile)(ZIO.attemptBlocking(PidFile.remove(pidPath)).ignore)
      live <- liveServerPort
      _    <- ZIO.foreachDiscard(live)(port => Console.printLineError(foregroundHint(port)).orDie)
    } yield ExitCode(1)

  private def terminate(handle: ProcessHandle, pid: Long, pidPath: Path): ZIO[Any, Throwable, ExitCode] = {
    val removePid = ZIO.attemptBlocking(PidFile.remove(pidPath)).ignore
    for {
      _      <- ZIO.attemptBlocking(handle.destroy()) // SIGTERM → ZIO shutdown hook → clean finalizers
      exited <- ZIO.fromCompletableFuture(handle.onExit()).timeout(TermTimeout)
      code <- exited match {
        case Some(_) => removePid *> Console.printLine(s"stopped (pid $pid)").orDie.as(ExitCode.success)
        case None =>
          for {
            _ <- Console.printLineError(s"pid $pid did not exit within ${TermTimeout.getSeconds}s; sending SIGKILL").orDie
            _ <- ZIO.attemptBlocking(handle.destroyForcibly())
            _ <- ZIO.fromCompletableFuture(handle.onExit()).timeout(KillTimeout).ignore
            _ <- removePid
            _ <- Console.printLine(s"stopped (pid $pid, forced)").orDie
          } yield ExitCode.success
      }
    } yield code
  }

  private def rootMessage(t: Throwable): String = Option(t.getMessage).getOrElse(t.getClass.getSimpleName)
}
