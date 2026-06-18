package ccas.cli.serve

import java.nio.file.{Files, Path}

import ccas.cli.XdgPaths

/** Pid-file helpers shared by the detached-serve parent ([[Detach]]) and [[Stop]]. The pure pieces ([[parse]],
  * [[alreadyRunning]]) carry the decision logic so they can be unit-tested without a real process or filesystem; the
  * effectful reads/writes are wrapped in `ZIO.attemptBlocking` at the call sites.
  */
object PidFile {

  /** Default location: `${XDG_STATE_HOME:-~/.local/state}/ccas/ccas.pid`. Callers may pass an explicit path (tests). */
  def path: Path = XdgPaths.pidFile

  /** Parse pid-file content to a positive pid; `None` for blank or non-numeric content. Pure. */
  def parse(content: String): Option[Long] = content.trim.toLongOption.filter(_ > 0)

  /** Read and parse the pid file; `None` if the file is absent or its content isn't a positive integer. */
  def read(p: Path): Option[Long] =
    if (Files.exists(p)) parse(Files.readString(p)) else None

  /** True if a live OS process currently has this pid. */
  def isAlive(pid: Long): Boolean = ProcessHandle.of(pid).filter(_.isAlive).isPresent

  /** Decide whether a detached server is already running: the recorded pid, only when it maps to a live process.
    * `alive` is injected so the branch logic is unit-testable. Pure.
    */
  def alreadyRunning(pid: Option[Long], alive: Long => Boolean): Option[Long] = pid.filter(alive)

  /** Best-effort delete; absent file is fine. */
  def remove(p: Path): Unit = { Files.deleteIfExists(p); () }
}
