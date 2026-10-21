package ccas.cli.serve

import java.nio.file.Path

import zio.{Console, ExitCode, UIO, ZIO}

import ccas.cli.serve.HealthProbe.Readiness

/** `ccas server status`: report whether the local ccas server is up. Combines two signals — an HTTP probe of the
  * loopback health endpoints ([[HealthProbe]]; universal: catches a foreground server that writes no pid file) and the
  * pid file (adds the detached server's pid). The decision logic lives in the pure [[describe]] so it is unit-testable
  * without a real server or process (same pattern as [[PidFile.alreadyRunning]]); [[run]] only wires the effects.
  */
object ServerStatus {

  /** Map the probe + pid signals to a one-line message and exit code. Exit 0 only when the server is ready (db ok);
    * every other state (degraded, starting, down) is exit 1, so `ccas server status && …` gates on a healthy server.
    * The database is the one the server names, never this shell's config: the two shells may differ (#320).
    *
    * @param readiness what `/health/ready` answered
    * @param up       `/health` returned 200 (server process responding; db state unknown); read only on `NoAnswer`
    * @param livePidOption  the pid from the pid file, only when it maps to a live process
    * @param stalePid the pid file exists but its pid is dead
    */
  def describe(
    readiness: Readiness,
    up: Boolean,
    livePidOption: Option[Long],
    stalePid: Boolean,
    port: Int
  ): (String, ExitCode) = {
    val server = livePidOption.fold("")(p => s"pid $p  ") + s"127.0.0.1:$port"
    readiness match {
      case Readiness.Ready(databaseOption) =>
        (s"running (ready)  $server  db ok (${named(databaseOption)})", ExitCode.success)
      case Readiness.Unready(databaseOption) =>
        (s"running (db unavailable)  $server  db down (${named(databaseOption)})", ExitCode(1))
      case Readiness.NoAnswer if up => (s"running (db unavailable)  $server", ExitCode(1))
      case Readiness.NoAnswer =>
        livePidOption match {
          case Some(p) => (s"starting or unhealthy (pid $p, no response on :$port)", ExitCode(1))
          case None    => (if (stalePid) { "not running (stale pid file)" } else { "not running" }, ExitCode(1))
        }
    }
  }

  private def named(databaseOption: Option[String]): String = databaseOption.getOrElse("database not reported")

  def run(stateDir: Path, port: Int): UIO[ExitCode] =
    for {
      readiness <- HealthProbe.readiness(port)
      up <- readiness match {
        case Readiness.NoAnswer => HealthProbe.isUp(port)
        case _                  => ZIO.succeed(true)
      }
      pidRead     <- ZIO.attemptBlocking(PidFile.read(PidFile.path(stateDir, port))).orElseSucceed(Option.empty[Long])
      alive        = pidRead.exists(PidFile.isAlive)
      (msg, code)  = describe(readiness, up, pidRead.filter(_ => alive), pidRead.isDefined && !alive, port)
      _           <- Console.printLine(msg).orDie
    } yield code
}
