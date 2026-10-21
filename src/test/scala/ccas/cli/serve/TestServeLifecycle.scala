package ccas.cli.serve

import java.nio.file.{Files, Path}

import com.typesafe.config.ConfigFactory
import zio.{durationInt, ExitCode, Ref, Scope, UIO, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

import ccas.cli.serve.HealthProbe.Readiness

/** Deterministic, DB-free unit tests for the `ccas server up --detach` / `status` / `down` lifecycle. The pure decision
  * logic ([[Detach.reconstruct]], [[Detach.resolveDeadline]], [[Detach.timeoutMessage]], [[Detach.diedMessage]],
  * [[Detach.startedMessage]], [[PidFile.parse]], [[PidFile.alreadyRunning]], [[ServerStatus.describe]],
  * [[HealthProbe.portIn]], [[HealthProbe.classify]]) is tested directly; the effectful pid-file
  * paths are exercised against an explicit temp path (the `XdgPaths`-backed default reads `System.getenv`, which can't
  * be set in-process). The full spawn/health-poll round-trip is covered by manual e2e, not here.
  */
object TestServeLifecycle extends ZIOSpecDefault {

  private def withTempDir[A](f: Path => A): A = {
    val dir = Files.createTempDirectory("ccas-serve-test")
    dir.toFile.deleteOnExit()
    f(dir)
  }

  // A pid guaranteed not to map to a live process: spawn a trivial command, wait for it to exit, reuse its pid.
  private def deadPid(): Long = {
    val p = new ProcessBuilder("sh", "-c", "exit 0").start()
    p.waitFor()
    p.pid()
  }

  // A live process to stand in for a running server: a harmless one, should a test wrongly signal it.
  private val liveProcess: ZIO[Scope, Throwable, Process] =
    ZIO.acquireRelease(ZIO.attempt(new ProcessBuilder("sleep", "60").start())) { p =>
      ZIO.succeed(p.destroyForcibly()).unit
    }

  // `down`'s probes for a server that is local: nothing to ask, and any question answered yes.
  private val localDatabase: UIO[Option[String]]        = ZIO.some("localhost:5432/ccas")
  private val answerYes: String => UIO[Option[Boolean]] = _ => ZIO.some(true)

  override def spec: Spec[Any, Any] = suite("TestServeLifecycle")(
    suite("PidFile.parse")(
      test("parses a positive pid, trimming whitespace") {
        assertTrue(PidFile.parse("1234\n").contains(1234L), PidFile.parse("  77 ").contains(77L))
      },
      test("rejects blank, non-numeric, zero, and negative") {
        assertTrue(
          PidFile.parse("").isEmpty,
          PidFile.parse("   ").isEmpty,
          PidFile.parse("abc").isEmpty,
          PidFile.parse("0").isEmpty,
          PidFile.parse("-5").isEmpty
        )
      }
    ),
    suite("PidFile.alreadyRunning")(
      test("no pid -> not running") {
        assertTrue(PidFile.alreadyRunning(None, _ => true).isEmpty)
      },
      test("pid present but dead -> not running") {
        assertTrue(PidFile.alreadyRunning(Some(5L), _ => false).isEmpty)
      },
      test("pid present and alive -> running") {
        assertTrue(PidFile.alreadyRunning(Some(5L), _ => true).contains(5L))
      }
    ),
    suite("PidFile.path")(
      test("two ports never share a pid file") {
        withTempDir { dir =>
          val main = PidFile.path(dir, 8080)
          val wip  = PidFile.path(dir, 8081)
          assertTrue(main != wip, main.getParent == dir, wip.getParent == dir)
        }
      }
    ),
    suite("PidFile.read")(
      test("reads a written pid and returns None for an absent file") {
        withTempDir { dir =>
          val present = dir.resolve("ccas-8080.pid")
          Files.writeString(present, "4242\n")
          assertTrue(PidFile.read(present).contains(4242L), PidFile.read(dir.resolve("absent.pid")).isEmpty)
        }
      }
    ),
    suite("Detach.reconstruct")(
      test("truncates at ccas.cli.Main, drops trailing CLI args, appends server up") {
        val args =
          List("--enable-native-access=ALL-UNNAMED", "-cp", "/a.jar:/b.jar", "ccas.cli.Main", "server", "up", "--detach")
        assertTrue(
          Detach.reconstruct("/usr/bin/java", args).contains(
            List("/usr/bin/java", "--enable-native-access=ALL-UNNAMED", "-cp", "/a.jar:/b.jar", "ccas.cli.Main", "server",
              "up")
          )
        )
      },
      test("returns None when the main-class token is absent") {
        assertTrue(Detach.reconstruct("/usr/bin/java", List("-cp", "/a.jar", "some.Other.Main", "x")).isEmpty)
      }
    ),
    suite("Detach.resolveDeadline")(
      test("no flag and no config -> the built-in default") {
        assertTrue(Detach.resolveDeadline(None, None).contains(Detach.DefaultReadyDeadline))
      },
      test("config supplies the deadline when the flag is absent") {
        assertTrue(Detach.resolveDeadline(None, Some(120)).contains(120.seconds))
      },
      test("the flag wins over the config") {
        assertTrue(Detach.resolveDeadline(Some(5), Some(120)).contains(5.seconds))
      },
      test("a non-positive flag is rejected, naming the flag") {
        val left = Detach.resolveDeadline(Some(0), None).swap.toOption
        assertTrue(left.exists(_.contains("--ready-timeout-seconds")), left.exists(_.contains("(got 0)")))
      },
      test("a non-positive config value is rejected, naming the key") {
        val left = Detach.resolveDeadline(None, Some(-5)).swap.toOption
        assertTrue(left.exists(_.contains("ready_timeout_seconds")), left.exists(_.contains("(got -5)")))
      }
    ),
    suite("Detach.startRefusal")(
      test("a live pid file for this port refuses the start, naming the pid and port") {
        withTempDir { dir =>
          val pidPath = PidFile.path(dir, 8081)
          ZIO.scoped {
            for {
              server  <- liveProcess
              _       <- ZIO.attempt(Files.writeString(pidPath, server.pid().toString + "\n"))
              refusal <- Detach.startRefusal(pidPath = pidPath, port = 8081, portAnswering = ZIO.succeed(false))
            } yield assertTrue(refusal.contains(s"already running on 127.0.0.1:8081, pid=${server.pid()}"))
          }
        }
      },
      test("a server answering this port with no pid file refuses the start instead of passing as the new one") {
        withTempDir { dir =>
          val pidPath = PidFile.path(dir, 8080)
          Detach.startRefusal(pidPath = pidPath, port = 8080, portAnswering = ZIO.succeed(true)).map { refusal =>
            assertTrue(
              refusal.exists(_.contains("already answering on 127.0.0.1:8080")),
              refusal.exists(_.contains("SERVER_PORT"))
            )
          }
        }
      },
      test("a free port with no live pid file starts") {
        withTempDir { dir =>
          val pidPath = PidFile.path(dir, 8080)
          for {
            _       <- ZIO.attempt(Files.writeString(pidPath, deadPid().toString + "\n"))
            refusal <- Detach.startRefusal(pidPath = pidPath, port = 8080, portAnswering = ZIO.succeed(false))
          } yield assertTrue(refusal.isEmpty)
        }
      }
    ),
    suite("Detach.serverLog")(
      test("two ports never share a server log") {
        val logDir = Path.of("/state/logs")
        assertTrue(Detach.serverLog(logDir, 8080) != Detach.serverLog(logDir, 8081))
      }
    ),
    suite("Detach failure messages")(
      // The regression this guards: an empty tail used to render as "Last log lines:" followed by nothing, which
      // promised evidence the CLI did not have and left the operator with no next move.
      test("an empty log names the file instead of an empty 'last log lines' section") {
        val msg = Detach.timeoutMessage(30.seconds, Some(true), Path.of("/state/server-8080.log"), "")
        assertTrue(
          msg.contains("Nothing has been written to /state/server-8080.log yet."),
          !msg.contains("Last log lines")
        )
      },
      test("a non-empty log is quoted and attributed to the file") {
        val msg = Detach.timeoutMessage(30.seconds, Some(false), Path.of("/state/server-8080.log"), "boom")
        assertTrue(msg.contains("Last log lines from /state/server-8080.log:\nboom"))
      },
      test("a still-running server is told the deadline is raisable, with a concrete larger value") {
        val msg = Detach.timeoutMessage(45.seconds, Some(true), Path.of("/state/server-8080.log"), "")
        assertTrue(
          msg.contains("did not become ready within 45s"),
          msg.contains("still running"),
          msg.contains("--ready-timeout-seconds 90"),
          msg.contains("ready_timeout_seconds"),
          msg.contains("in the foreground to watch it boot")
        )
      },
      test("a failed liveness read says so rather than claiming the server was up") {
        val msg = Detach.timeoutMessage(30.seconds, None, Path.of("/state/server-8080.log"), "")
        assertTrue(
          msg.contains("state could not be read"),
          !msg.contains("was still running"),
          msg.contains("--ready-timeout-seconds 60")
        )
      },
      test("a dead server is told a longer deadline will not help") {
        val msg = Detach.timeoutMessage(30.seconds, Some(false), Path.of("/state/server-8080.log"), "")
        assertTrue(
          msg.contains("already gone"),
          msg.contains("longer deadline will not help"),
          !msg.contains("--ready-timeout-seconds 60")
        )
      },
      test("an exit during startup with no log suggests the foreground run") {
        val msg = Detach.diedMessage(Path.of("/state/server-8080.log"), "")
        assertTrue(msg.contains("Nothing has been written"), msg.contains("in the foreground"))
      },
      test("an exit during startup with a log leaves the log to speak") {
        val msg = Detach.diedMessage(Path.of("/state/server-8080.log"), "Caused by: ConnectException")
        assertTrue(msg.contains("Caused by: ConnectException"), !msg.contains("in the foreground"))
      }
    ),
    suite("ServerStop.stopQuestion")(
      test("a server on a loopback database stops without asking") {
        assertTrue(
          ServerStop.stopQuestion(port = 8081, pid = 4821L, databaseOption = Some("localhost:5432/ccas")).isEmpty,
          ServerStop.stopQuestion(port = 8081, pid = 4821L, databaseOption = Some("127.0.0.1:5432/ccas")).isEmpty
        )
      },
      test("a server on any other database asks, naming its port, pid and database") {
        assertTrue(
          ServerStop.stopQuestion(port = 8080, pid = 4821L, databaseOption = Some("ep-x.neon.tech/neondb")).contains(
            "Stop the server on 127.0.0.1:8080 (pid 4821, db ep-x.neon.tech/neondb)? [y/N] "
          )
        )
      },
      test("a server that names no database asks") {
        val question = ServerStop.stopQuestion(port = 8080, pid = 4821L, databaseOption = None)
        assertTrue(question.exists(_.contains("database not reported")))
      }
    ),
    suite("ServerStop.run")(
      // The probes are injected so these exercise every branch without touching the network or a terminal — no
      // dependence on whether anything happens to be listening on the port. Where a test is about the pid file, the
      // server is local and the answer yes, so nothing but the pid file stands between a bug and the signal.
      test("no pid file, no live server -> non-zero, clear message, no foreground hint") {
        withTempDir { dir =>
          for {
            code <- ServerStop.run(
              stateDir = dir,
              port = 8080,
              serverAnswering = ZIO.succeed(false),
              serverDatabase = localDatabase,
              ask = answerYes
            )
            err <- TestConsole.outputErr
          } yield assertTrue(
            code == ExitCode(1),
            err.exists(_.contains("no detached server running")),
            !err.exists(_.contains("still responding"))
          )
        }
      },
      test("stale pid file, no live server -> non-zero, message, and the file is removed") {
        withTempDir { dir =>
          val stale = PidFile.path(dir, 8080)
          for {
            _ <- ZIO.attempt(Files.writeString(stale, deadPid().toString + "\n"))
            code <- ServerStop.run(
              stateDir = dir,
              port = 8080,
              serverAnswering = ZIO.succeed(false),
              serverDatabase = localDatabase,
              ask = answerYes
            )
            err     <- TestConsole.outputErr
            removed <- ZIO.attempt(!Files.exists(stale))
          } yield assertTrue(code == ExitCode(1), err.exists(_.contains("stale pid file")), removed)
        }
      },
      test("no detached pid but a server is still up -> appends the foreground hint naming its port") {
        withTempDir { dir =>
          for {
            code <- ServerStop.run(
              stateDir = dir,
              port = 9137,
              serverAnswering = ZIO.succeed(true),
              serverDatabase = localDatabase,
              ask = answerYes
            )
            err <- TestConsole.outputErr
          } yield assertTrue(code == ExitCode(1), err.exists(_.contains("still responding on 127.0.0.1:9137")))
        }
      },
      // The bug in #326: one shared pid file let `down` in the 8081 shell stop the 8080 server, possibly production.
      test("down for one port leaves another port's server and pid file alone") {
        withTempDir { dir =>
          val otherPidFile = PidFile.path(dir, 8080)
          ZIO.scoped {
            for {
              server <- liveProcess
              _      <- ZIO.attempt(Files.writeString(otherPidFile, server.pid().toString + "\n"))
              code <- ServerStop.run(
                stateDir = dir,
                port = 8081,
                serverAnswering = ZIO.succeed(false),
                serverDatabase = localDatabase,
                ask = answerYes
              )
              err      <- TestConsole.outputErr
              alive    <- ZIO.attempt(server.isAlive)
              fileKept <- ZIO.attempt(Files.exists(otherPidFile))
            } yield assertTrue(
              code == ExitCode(1),
              err.exists(_.contains("no detached server running")),
              alive,
              fileKept
            )
          }
        }
      },
      test("a declined stop leaves the server running and its pid file in place") {
        withTempDir { dir =>
          val pidPath = PidFile.path(dir, 8080)
          ZIO.scoped {
            for {
              asked  <- Ref.make(Option.empty[String])
              server <- liveProcess
              _      <- ZIO.attempt(Files.writeString(pidPath, server.pid().toString + "\n"))
              code <- ServerStop.run(
                stateDir = dir,
                port = 8080,
                serverAnswering = ZIO.succeed(false),
                serverDatabase = ZIO.some("ep-x.neon.tech/neondb"),
                ask = question => asked.set(Some(question)).as(Some(false))
              )
              question <- asked.get
              out      <- TestConsole.output
              err      <- TestConsole.outputErr
              alive    <- ZIO.attempt(server.isAlive)
              fileKept <- ZIO.attempt(Files.exists(pidPath))
            } yield assertTrue(
              code == ExitCode(1),
              question.exists(_.contains("db ep-x.neon.tech/neondb")),
              err.exists(_.contains("not stopped")),
              !out.exists(_.contains("stopped")),
              alive,
              fileKept
            )
          }
        }
      },
      test("with no terminal to ask on, a server on another database is not stopped") {
        withTempDir { dir =>
          val pidPath = PidFile.path(dir, 8080)
          ZIO.scoped {
            for {
              server <- liveProcess
              _      <- ZIO.attempt(Files.writeString(pidPath, server.pid().toString + "\n"))
              code <- ServerStop.run(
                stateDir = dir,
                port = 8080,
                serverAnswering = ZIO.succeed(false),
                serverDatabase = ZIO.some("ep-x.neon.tech/neondb"),
                ask = _ => ZIO.none
              )
              err      <- TestConsole.outputErr
              alive    <- ZIO.attempt(server.isAlive)
              fileKept <- ZIO.attempt(Files.exists(pidPath))
            } yield assertTrue(code == ExitCode(1), err.exists(_.contains("no terminal to ask on")), alive, fileKept)
          }
        }
      },
      test("a confirmed stop signals the server and names the port and database it stopped") {
        withTempDir { dir =>
          val pidPath = PidFile.path(dir, 8080)
          ZIO.scoped {
            for {
              asked  <- Ref.make(Option.empty[String])
              server <- liveProcess
              _      <- ZIO.attempt(Files.writeString(pidPath, server.pid().toString + "\n"))
              code <- ServerStop.run(
                stateDir = dir,
                port = 8080,
                serverAnswering = ZIO.succeed(false),
                serverDatabase = ZIO.some("ep-x.neon.tech/neondb"),
                ask = question => asked.set(Some(question)).as(Some(true))
              )
              question <- asked.get
              out      <- TestConsole.output
              alive    <- ZIO.attempt(server.isAlive)
              removed  <- ZIO.attempt(!Files.exists(pidPath))
              stopped   = s"stopped the server on 127.0.0.1:8080 (pid ${server.pid()}, db ep-x.neon.tech/neondb)"
            } yield assertTrue(code == ExitCode.success, question.isDefined, out.exists(_.contains(stopped)), !alive, removed)
          }
        }
      },
      test("foregroundHint names the probed port and both ways to stop a foreground server") {
        val hint = ServerStop.foregroundHint(9137)
        assertTrue(
          hint.contains("127.0.0.1:9137"),
          hint.contains("Ctrl-C"),
          hint.contains("lsof -ti tcp:9137")
        )
      }
    ),
    suite("ServerStatus.describe")(
      test("ready with a live pid -> success, shows pid and the database the server names") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.Ready(Some("localhost:5432/ccas")),
          up = true,
          livePidOption = Some(48213L),
          stalePid = false,
          port = 8080
        )
        assertTrue(
          code == ExitCode.success,
          msg == "running (ready)  pid 48213  127.0.0.1:8080  db ok (localhost:5432/ccas)"
        )
      },
      test("ready with no pid file (foreground server) -> success, no pid shown") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.Ready(Some("localhost:5432/ccas")),
          up = true,
          livePidOption = None,
          stalePid = false,
          port = 9000
        )
        assertTrue(code == ExitCode.success, msg == "running (ready)  127.0.0.1:9000  db ok (localhost:5432/ccas)")
      },
      test("a server that names no database says so rather than printing a bare db ok") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.Ready(None),
          up = true,
          livePidOption = None,
          stalePid = false,
          port = 9000
        )
        assertTrue(code == ExitCode.success, msg == "running (ready)  127.0.0.1:9000  db ok (database not reported)")
      },
      test("db unavailable names the database the server could not reach") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.Unready(Some("ep-x.neon.tech/neondb")),
          up = true,
          livePidOption = Some(7L),
          stalePid = false,
          port = 8080
        )
        assertTrue(
          code == ExitCode(1),
          msg == "running (db unavailable)  pid 7  127.0.0.1:8080  db down (ep-x.neon.tech/neondb)"
        )
      },
      test("readiness unanswered but /health up -> db unavailable, non-zero") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.NoAnswer,
          up = true,
          livePidOption = Some(7L),
          stalePid = false,
          port = 8080
        )
        assertTrue(code == ExitCode(1), msg == "running (db unavailable)  pid 7  127.0.0.1:8080")
      },
      test("not responding but pid alive -> starting/unhealthy, non-zero") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.NoAnswer,
          up = false,
          livePidOption = Some(7L),
          stalePid = false,
          port = 8080
        )
        assertTrue(code == ExitCode(1), msg == "starting or unhealthy (pid 7, no response on :8080)")
      },
      test("not responding, no pid -> not running, non-zero") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.NoAnswer,
          up = false,
          livePidOption = None,
          stalePid = false,
          port = 8080
        )
        assertTrue(code == ExitCode(1), msg == "not running")
      },
      test("not responding, stale pid file -> not running (stale pid file), non-zero") {
        val (msg, code) = ServerStatus.describe(
          readiness = Readiness.NoAnswer,
          up = false,
          livePidOption = None,
          stalePid = true,
          port = 8080
        )
        assertTrue(code == ExitCode(1), msg == "not running (stale pid file)")
      }
    ),
    suite("HealthProbe.portIn")(
      test("a port that isn't 1 to 65535 is an error, never the default port") {
        def portIn(value: String) = HealthProbe.portIn(ConfigFactory.parseString(s"server.port = $value"))
        assertTrue(
          portIn("808l").left.exists(_.contains("server.port")),
          portIn("0").isLeft,
          portIn("70000").isLeft,
          portIn("8081") == Right(8081)
        )
      }
    ),
    suite("HealthProbe.classify")(
      test("a 200 naming the database reads as ready with that database") {
        assertTrue(
          HealthProbe.classify(200, """{"database":"localhost:5432/ccas"}""") ==
            Readiness.Ready(Some("localhost:5432/ccas"))
        )
      },
      test("a 503 naming the database reads as unready with that database") {
        assertTrue(
          HealthProbe.classify(503, """{"database":"ep-x.neon.tech/neondb"}""") ==
            Readiness.Unready(Some("ep-x.neon.tech/neondb"))
        )
      },
      test("an empty 200 body, as an older server sends, reads as ready with no database named") {
        assertTrue(HealthProbe.classify(200, "") == Readiness.Ready(None))
      },
      test("any other status reads as no answer") {
        assertTrue(
          HealthProbe.classify(500, """{"database":"localhost:5432/ccas"}""") == Readiness.NoAnswer,
          HealthProbe.classify(404, "") == Readiness.NoAnswer
        )
      }
    ),
    suite("Detach.startedMessage")(
      test("the started line names the database the server reports") {
        val pidPath = Path.of("/state/ccas-8080.pid")
        val logFile = Path.of("/state/server-8080.log")
        val started = Detach.startedMessage(
          pidOption = Some(4821L),
          databaseOption = Some("localhost:5432/ccas"),
          pidPath = pidPath,
          logFile = logFile
        )
        val pidUnread = Detach.startedMessage(
          pidOption = None,
          databaseOption = Some("localhost:5432/ccas"),
          pidPath = pidPath,
          logFile = logFile
        )
        val unnamed =
          Detach.startedMessage(pidOption = Some(4821L), databaseOption = None, pidPath = pidPath, logFile = logFile)
        assertTrue(
          started == "ccas server started, pid=4821, db localhost:5432/ccas (logs: /state/server-8080.log)",
          pidUnread.contains("db localhost:5432/ccas"),
          unnamed.contains("database not reported")
        )
      }
    )
  )
}
