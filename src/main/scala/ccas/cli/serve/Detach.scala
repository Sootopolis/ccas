package ccas.cli.serve

import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration => JDuration}

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

import zio.{Console, Duration, durationInt, ExitCode, UIO, ZIO}

/** Parent side of `ccas server up --detach`: spawn the server as a background process, wait for it to become ready,
  * then return to the shell with the pid printed. The JVM can't `fork()`, so we re-exec this same binary's
  * `server up` (foreground) as a child under `setsid` (a new session detached from the controlling terminal — the pragmatic
  * equivalent of a daemon double-fork; the child survives the parent/shell exiting). The child writes its own pid file
  * (path handed over via `CCAS_PID_FILE`); the parent only reads it back to report.
  */
object Detach {

  private val MainClass    = "ccas.cli.Main"
  private val PollInterval = 250.millis
  private val HttpTimeout  = JDuration.ofSeconds(2)

  /** Readiness wait when neither `--ready-timeout-seconds` nor `ready_timeout_seconds` is set. Generous for a local Postgres,
    * tight for a serverless provider that suspends after minutes of idle — hence [[resolveDeadline]].
    */
  val DefaultReadyDeadline: Duration = 30.seconds
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

  /** Readiness deadline for one `--detach` run: `--ready-timeout-seconds` beats the config's `ready_timeout_seconds`, which
    * beats [[DefaultReadyDeadline]]. `Left` is a ready-to-print message naming the source that supplied the bad value,
    * since a non-positive deadline means "give up before the first poll" — never what the operator meant. Pure.
    */
  def resolveDeadline(flag: Option[Int], configured: Option[Int]): Either[String, Duration] =
    (flag, configured) match {
      case (Some(seconds), _)    => positiveSeconds(seconds, "--ready-timeout-seconds")
      case (None, Some(seconds)) => positiveSeconds(seconds, "ready_timeout_seconds in the CLI config file")
      case (None, None)          => Right(DefaultReadyDeadline)
    }

  private def positiveSeconds(seconds: Int, source: String): Either[String, Duration] =
    if (seconds > 0) { Right(seconds.seconds) }
    else { Left(s"$source must be a positive number of seconds (got $seconds)") }

  def run(logDir: Path, pidPath: Path, readyDeadline: Duration): UIO[ExitCode] = {
    val program =
      for {
        running <- ZIO.attemptBlocking(PidFile.alreadyRunning(PidFile.read(pidPath), PidFile.isAlive))
        code <- running match {
          case Some(pid) => Console.printLineError(s"already running, pid=$pid").orDie.as(ExitCode(1))
          case None      => start(logDir, pidPath, readyDeadline)
        }
      } yield code

    program.catchAllCause(c =>
      Console.printLineError(s"error: ${rootMessage(c.squash)}").orDie.as(ExitCode(1))
    )
  }

  private def start(logDir: Path, pidPath: Path, readyDeadline: Duration): ZIO[Any, Throwable, ExitCode] =
    for {
      port    <- HealthProbe.resolvePort
      logFile  = logDir.resolve("server.log")
      _       <- ZIO.attemptBlocking(Files.createDirectories(logDir))
      cmd     <- ZIO.attempt(baseCommand)
      process <- spawn(cmd, logFile, pidPath)
      code    <- awaitReady(process, port, logFile, pidPath, readyDeadline)
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

  private def awaitReady(
    process: Process,
    port: Int,
    logFile: Path,
    pidPath: Path,
    readyDeadline: Duration
  ): UIO[ExitCode] = {
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
    // is a short-lived shim and only the pid file tracks the real server). `None` is a failed read.
    val liveness = ZIO.attemptBlocking(process.isAlive || PidFile.read(pidPath).exists(PidFile.isAlive)).option

    // The loop assumes alive on a read error (the overall deadline still bounds the wait) rather than risk a false
    // crash report; the timeout message keeps the unknown instead of reporting the assumption as fact.
    val serverAlive = liveness.map(_.getOrElse(true))

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

    // Liveness is read BEFORE the kill, so the timeout message can separate a slow boot from a dead child — the very
    // distinction the old empty "Last log lines:" left ambiguous.
    def onTimeout: UIO[ExitCode] =
      for {
        alive <- liveness
        _     <- killServer(process, pidPath)
        tail  <- tailLog(logFile)
        code  <- fail(timeoutMessage(readyDeadline, alive, logFile, tail))
      } yield code

    waitLoop(0).timeout(readyDeadline).foldZIO(
      _ => childDied(logFile),
      {
        case Some(_) => ready(pidPath, logFile)
        case None    => onTimeout
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
    tailLog(logFile).flatMap(tail => fail(diedMessage(logFile, tail)))

  private def fail(message: String): UIO[ExitCode] =
    Console.printLineError(s"error: $message").orDie.as(ExitCode(1))

  /** Failure text for a readiness timeout. `alive` picks the advice — `None` being a liveness read that failed, which
    * is reported as the unknown it is rather than as the loop's assume-alive default. A process still running was
    * probably only slow to boot, so the deadline is worth raising; one already gone will not come up however long we
    * wait. Pure.
    */
  def timeoutMessage(deadline: Duration, alive: Option[Boolean], logFile: Path, tail: String): String = {
    val seconds = deadline.getSeconds
    val slowBoot =
      s"It may only have needed longer to start (a suspended serverless compute can take well over ${seconds}s to " +
        s"wake). Retry with `ccas server up --detach --ready-timeout-seconds ${seconds * 2}`, set " +
        "`ready_timeout_seconds` in the CLI config file, or run `ccas server up` in the foreground to watch it boot."
    val advice = alive match {
      case Some(true) => s"The server process was still running. $slowBoot"
      case None       => s"The server process's state could not be read. $slowBoot"
      case Some(false) =>
        "The server process was already gone, so a longer deadline will not help. " +
          "Run `ccas server up` in the foreground to see why it died."
    }
    s"server did not become ready within ${seconds}s; stopped it.\n${logEvidence(logFile, tail)}\n$advice"
  }

  /** Failure text for a child that exited before reporting ready. With no log content there is nothing to report but
    * the path, so say that instead of printing an empty "Last log lines:" section. Pure.
    */
  def diedMessage(logFile: Path, tail: String): String = {
    val evidence = logEvidence(logFile, tail)
    if (tail.nonEmpty) { s"server exited during startup.\n$evidence" }
    else { s"server exited during startup.\n$evidence Run `ccas server up` in the foreground to see why." }
  }

  // Never promise evidence we don't have: an empty tail names the file the operator should watch instead.
  private def logEvidence(logFile: Path, tail: String): String =
    if (tail.nonEmpty) { s"Last log lines from $logFile:\n$tail" }
    else { s"Nothing has been written to $logFile yet." }

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
