package ccas.cli.serve

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration => JDuration}

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

import zio.{Console, durationInt, ExitCode, UIO, ZIO}

/** Parent side of `ccas server up --detach`: spawn the server as a background process, wait for it to become ready,
  * then return to the shell with the pid printed. The JVM can't `fork()`, so we re-exec this same binary's
  * `server up` (foreground) as a child under `setsid` (a new session detached from the controlling terminal — the pragmatic
  * equivalent of a daemon double-fork; the child survives the parent/shell exiting). The child writes its own pid file
  * (path handed over via `CCAS_PID_FILE`); the parent only reads it back to report.
  */
object Detach {

  private val MainClass     = "ccas.cli.Main"
  private val ReadyDeadline = 30.seconds
  private val PollInterval  = 250.millis
  private val HttpTimeout   = JDuration.ofSeconds(2)
  // Consecutive "not alive" polls (~2s) before declaring the child dead. Tolerates the brief early-boot window in the
  // setsid-fork case where the spawn process has exited but the server JVM hasn't written its pid file yet.
  private val DeadStreakLimit = 8

  /** Rebuild the child command line from the current process's `java` executable + arguments. Keep everything up to
    * and including the `ccas.cli.Main` token (preserves `-cp <classpath>`, `-D…`, `--enable-native-access`, etc.) and
    * append `server up` — dropping whatever CLI args (`server up --detach`) followed the main class. `None` when
    * the main-class token isn't present (caller falls back to a from-scratch reconstruction). Pure.
    */
  def reconstruct(command: String, arguments: List[String]): Option[List[String]] = {
    val idx = arguments.indexOf(MainClass)
    if (idx < 0) { None }
    else { Some(command :: (arguments.take(idx + 1) ::: List("server", "up"))) }
  }

  // Last-resort reconstruction if ProcessHandle can't report the live command (loses launcher JVM flags; harmless).
  private def fallbackCommand: List[String] =
    List(
      Paths.get(System.getProperty("java.home"), "bin", "java").toString,
      "-cp",
      System.getProperty("java.class.path"),
      MainClass,
      "server",
      "up"
    )

  private def baseCommand: List[String] = {
    val info = ProcessHandle.current().info()
    (info.command().toScala, info.arguments().toScala) match {
      case (Some(cmd), Some(args)) => reconstruct(cmd, args.toList).getOrElse(fallbackCommand)
      case _                       => fallbackCommand
    }
  }

  def run(logDir: Path, pidPath: Path): UIO[ExitCode] = {
    val program =
      for {
        running <- ZIO.attemptBlocking(PidFile.alreadyRunning(PidFile.read(pidPath), PidFile.isAlive))
        code <- running match {
          case Some(pid) => Console.printLineError(s"already running, pid=$pid").orDie.as(ExitCode(1))
          case None      => start(logDir, pidPath)
        }
      } yield code

    program.catchAllCause(c =>
      Console.printLineError(s"error: ${rootMessage(c.squash)}").orDie.as(ExitCode(1))
    )
  }

  private def start(logDir: Path, pidPath: Path): ZIO[Any, Throwable, ExitCode] =
    for {
      port    <- HealthProbe.resolvePort
      logFile  = logDir.resolve("server.log")
      _       <- ZIO.attemptBlocking(Files.createDirectories(logDir))
      cmd     <- ZIO.attempt(baseCommand)
      process <- spawn(cmd, logFile, pidPath)
      code    <- awaitReady(process, port, logFile, pidPath)
    } yield code

  // Try setsid (new session — full terminal detach), then nohup (SIGHUP-immune), then bare. Each prefix is only
  // retried when the launcher itself is missing (ProcessBuilder.start throws); a child that starts but then crashes
  // is the readiness loop's problem, not a reason to fall back.
  private def spawn(base: List[String], logFile: Path, pidPath: Path): ZIO[Any, Throwable, Process] = {
    def attempt(prefix: List[String]): ZIO[Any, Throwable, Process] =
      ZIO.attemptBlocking {
        val pb = new ProcessBuilder((prefix ++ base)*)
        pb.redirectErrorStream(true)
        // Truncate per run: each detached start gets a fresh log, so the file can't grow unbounded across restarts
        // (the alreadyRunning guard means we never reach here while a server is live writing to it).
        pb.redirectOutput(Redirect.to(logFile.toFile))
        pb.redirectInput(Redirect.from(new File("/dev/null"))) // Linux-only sink; matches the loopback-local model
        pb.environment().put("CCAS_PID_FILE", pidPath.toString)
        pb.start()
      }
    attempt(List("setsid")).orElse(attempt(List("nohup"))).orElse(attempt(Nil))
  }

  private def awaitReady(process: Process, port: Int, logFile: Path, pidPath: Path): UIO[ExitCode] = {
    val client    = HttpClient.newBuilder().connectTimeout(HttpTimeout).build()
    val readyUri  = URI.create(s"http://127.0.0.1:$port/health/ready")
    val pollReady = ZIO
      .attemptBlocking {
        val req = HttpRequest.newBuilder(readyUri).timeout(HttpTimeout).GET().build()
        client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode() == 200
      }
      .orElseSucceed(false)

    // Liveness is the spawn process OR a live pid in the pid file. The pid-file signal makes this independent of
    // whether `setsid` exec-replaced in place (pid preserved → `process` is the server) or forked (the spawn process
    // is a short-lived shim and only the pid file tracks the real server). On a read error, assume alive (the overall
    // deadline still bounds the wait) rather than risk a false crash report.
    val serverAlive =
      ZIO.attemptBlocking(process.isAlive || PidFile.read(pidPath).exists(PidFile.isAlive)).orElseSucceed(true)

    // Succeeds when the server reports ready; fails (ProcessDied) after DeadStreakLimit consecutive not-alive polls.
    def waitLoop(deadStreak: Int): ZIO[Any, ProcessDied.type, Unit] =
      pollReady.flatMap {
        case true => ZIO.unit
        case false =>
          serverAlive.flatMap {
            case true                                   => ZIO.sleep(PollInterval) *> waitLoop(0)
            case false if deadStreak + 1 >= DeadStreakLimit => ZIO.fail(ProcessDied)
            case false                                  => ZIO.sleep(PollInterval) *> waitLoop(deadStreak + 1)
          }
      }

    waitLoop(0).timeout(ReadyDeadline).foldZIO(
      _ => childDied(logFile),
      {
        case Some(_) => ready(pidPath, logFile)
        case None    => timedOut(process, pidPath, logFile)
      }
    )
  }

  private def ready(pidPath: Path, logFile: Path): UIO[ExitCode] =
    readPid(pidPath, 3).flatMap {
      case Some(pid) =>
        Console.printLine(s"ccas server started, pid=$pid (logs: $logFile)").orDie.as(ExitCode.success)
      case None =>
        Console.printLine(s"ccas server started (pid file not yet readable at $pidPath; logs: $logFile)").orDie
          .as(ExitCode.success)
    }

  private def childDied(logFile: Path): UIO[ExitCode] =
    tailLog(logFile).flatMap(tail =>
      Console.printLineError(s"error: server exited during startup. Last log lines:\n$tail").orDie.as(ExitCode(1))
    )

  private def timedOut(process: Process, pidPath: Path, logFile: Path): UIO[ExitCode] =
    killServer(process, pidPath) *>
      tailLog(logFile).flatMap(tail =>
        Console
          .printLineError(s"error: server did not become ready within ${ReadyDeadline.getSeconds}s. Last log lines:\n$tail")
          .orDie
          .as(ExitCode(1))
      )

  // Kill the real server by its pid-file pid (works whether setsid forked or not); fall back to the spawn process if
  // the pid file isn't there. Avoids leaving an orphaned half-started server when `process` is a dead setsid shim.
  private def killServer(process: Process, pidPath: Path): UIO[Unit] =
    ZIO
      .attemptBlocking {
        PidFile.read(pidPath).flatMap(p => ProcessHandle.of(p).toScala) match {
          case Some(handle) => handle.destroy()
          case None         => process.destroy()
        }
      }
      .ignore

  private def readPid(pidPath: Path, attempts: Int): UIO[Option[Long]] =
    ZIO.attemptBlocking(PidFile.read(pidPath)).orElseSucceed(None).flatMap {
      case some @ Some(_)       => ZIO.succeed(some)
      case None if attempts > 0 => ZIO.sleep(100.millis) *> readPid(pidPath, attempts - 1)
      case None                 => ZIO.none
    }

  private def tailLog(logFile: Path, lines: Int = 20): UIO[String] =
    ZIO
      .attemptBlocking {
        if (Files.exists(logFile)) { Files.readAllLines(logFile).asScala.toList.takeRight(lines).mkString("\n") }
        else { "" }
      }
      .orElseSucceed("")

  private def rootMessage(t: Throwable): String = Option(t.getMessage).getOrElse(t.getClass.getSimpleName)

  private case object ProcessDied
}
