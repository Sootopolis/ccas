package ccas.cli.serve

import java.nio.file.Path

import zio.{Console, ExitCode, UIO, ZIO}

/** `ccas server status`: report whether the local ccas server is up. Combines two signals — an HTTP probe of the
  * loopback health endpoints ([[HealthProbe]]; universal: catches a foreground server that writes no pid file) and the
  * pid file (adds the detached server's pid). The decision logic lives in the pure [[describe]] so it is unit-testable
  * without a real server or process (same pattern as [[PidFile.alreadyRunning]]); [[run]] only wires the effects.
  */
object Status {

  /** Map the probe + pid signals to a one-line message and exit code. Exit 0 only when the server is ready (db ok);
    * every other state (degraded, starting, down) is exit 1, so `ccas server status && …` gates on a healthy server.
    *
    * @param ready    `/health/ready` returned 200 (server up AND database reachable)
    * @param up       `/health` returned 200 (server process responding; db state unknown)
    * @param livePid  the pid from the pid file, only when it maps to a live process
    * @param stalePid the pid file exists but its pid is dead
    */
  def describe(ready: Boolean, up: Boolean, livePid: Option[Long], stalePid: Boolean, port: Int): (String, ExitCode) = {
    val addr   = s"127.0.0.1:$port"
    val pidStr = livePid.map(p => s"pid $p  ").getOrElse("")
    if (ready) {
      (s"running (ready)  ${pidStr}${addr}  db ok", ExitCode.success)
    } else if (up) {
      (s"running (db unavailable)  ${pidStr}${addr}", ExitCode(1))
    } else {
      livePid match {
        case Some(p) => (s"starting or unhealthy (pid $p, no response on :$port)", ExitCode(1))
        case None    => (if (stalePid) { "not running (stale pid file)" } else { "not running" }, ExitCode(1))
      }
    }
  }

  def run(pidPath: Path): UIO[ExitCode] =
    for {
      port         <- HealthProbe.resolvePort
      ready        <- HealthProbe.isReady(port)
      up           <- if (ready) { ZIO.succeed(true) } else { HealthProbe.isUp(port) }
      pidRead      <- ZIO.attemptBlocking(PidFile.read(pidPath)).orElseSucceed(Option.empty[Long])
      alive         = pidRead.exists(PidFile.isAlive)
      (msg, code)   = describe(ready, up, pidRead.filter(_ => alive), pidRead.isDefined && !alive, port)
      _            <- Console.printLine(msg).orDie
    } yield code
}
