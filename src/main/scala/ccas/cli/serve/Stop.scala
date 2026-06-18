package ccas.cli.serve

import java.nio.file.{Files, Path}

import scala.jdk.OptionConverters.*

import zio.{Console, durationInt, ExitCode, UIO, ZIO}

/** `ccas stop`: read the pid file written by a detached server, send SIGTERM, wait for a clean exit. The server's own
  * shutdown finalizer removes the pid file (and runs the ZIO scope finalizers — Hikari pool close, in-flight job
  * interrupt); `stop` removes the file as a backstop. Exits non-zero with a clear message when there's nothing to stop
  * (no pid file, or the recorded pid is dead). Recycled-pid risk is accepted (single-user local model, #40).
  */
object Stop {

  private val TermTimeout = 20.seconds
  private val KillTimeout = 5.seconds

  def run(pidPath: Path): UIO[ExitCode] = {
    val program =
      ZIO.attemptBlocking(Files.exists(pidPath)).flatMap {
        case false => Console.printLineError(s"no detached server running (no pid file at $pidPath)").orDie.as(ExitCode(1))
        case true =>
          ZIO.attemptBlocking(PidFile.read(pidPath)).flatMap {
            case None =>
              Console.printLineError(s"corrupt pid file at $pidPath; removing").orDie *>
                ZIO.attemptBlocking(PidFile.remove(pidPath)).ignore.as(ExitCode(1))
            case Some(pid) => stopPid(pid, pidPath)
          }
      }
    program.catchAllCause(c => Console.printLineError(s"error: ${rootMessage(c.squash)}").orDie.as(ExitCode(1)))
  }

  private def stopPid(pid: Long, pidPath: Path): ZIO[Any, Throwable, ExitCode] =
    ZIO.attemptBlocking(ProcessHandle.of(pid).toScala).flatMap {
      case Some(handle) if handle.isAlive => terminate(handle, pid, pidPath)
      case _ =>
        Console.printLineError(s"stale pid file at $pidPath (no live process $pid); removing").orDie *>
          ZIO.attemptBlocking(PidFile.remove(pidPath)).ignore.as(ExitCode(1))
    }

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
