package ccas.cli.serve

import java.nio.file.{Files, Path}

import zio.{durationInt, ExitCode, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

/** Deterministic, DB-free unit tests for the `ccas serve --detach` / `ccas stop` lifecycle. The pure decision logic
  * ([[Detach.reconstruct]], [[Detach.resolveDeadline]], [[Detach.timeoutMessage]], [[Detach.diedMessage]],
  * [[PidFile.parse]], [[PidFile.alreadyRunning]]) is tested directly; the effectful pid-file
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
    suite("PidFile.read")(
      test("reads a written pid and returns None for an absent file") {
        withTempDir { dir =>
          val present = dir.resolve("ccas.pid")
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
    suite("Detach failure messages")(
      // The regression this guards: an empty tail used to render as "Last log lines:" followed by nothing, which
      // promised evidence the CLI did not have and left the operator with no next move.
      test("an empty log names the file instead of an empty 'last log lines' section") {
        val msg = Detach.timeoutMessage(30.seconds, Some(true), Path.of("/state/server.log"), "")
        assertTrue(
          msg.contains("Nothing has been written to /state/server.log yet."),
          !msg.contains("Last log lines")
        )
      },
      test("a non-empty log is quoted and attributed to the file") {
        val msg = Detach.timeoutMessage(30.seconds, Some(false), Path.of("/state/server.log"), "boom")
        assertTrue(msg.contains("Last log lines from /state/server.log:\nboom"))
      },
      test("a still-running server is told the deadline is raisable, with a concrete larger value") {
        val msg = Detach.timeoutMessage(45.seconds, Some(true), Path.of("/state/server.log"), "")
        assertTrue(
          msg.contains("did not become ready within 45s"),
          msg.contains("still running"),
          msg.contains("--ready-timeout-seconds 90"),
          msg.contains("ready_timeout_seconds"),
          msg.contains("in the foreground to watch it boot")
        )
      },
      test("a failed liveness read says so rather than claiming the server was up") {
        val msg = Detach.timeoutMessage(30.seconds, None, Path.of("/state/server.log"), "")
        assertTrue(
          msg.contains("state could not be read"),
          !msg.contains("was still running"),
          msg.contains("--ready-timeout-seconds 60")
        )
      },
      test("a dead server is told a longer deadline will not help") {
        val msg = Detach.timeoutMessage(30.seconds, Some(false), Path.of("/state/server.log"), "")
        assertTrue(
          msg.contains("already gone"),
          msg.contains("longer deadline will not help"),
          !msg.contains("--ready-timeout-seconds 60")
        )
      },
      test("an exit during startup with no log suggests the foreground run") {
        val msg = Detach.diedMessage(Path.of("/state/server.log"), "")
        assertTrue(msg.contains("Nothing has been written"), msg.contains("in the foreground"))
      },
      test("an exit during startup with a log leaves the log to speak") {
        val msg = Detach.diedMessage(Path.of("/state/server.log"), "Caused by: ConnectException")
        assertTrue(msg.contains("Caused by: ConnectException"), !msg.contains("in the foreground"))
      }
    ),
    suite("Stop.run")(
      // The `liveServerPort` seam is injected (ZIO.succeed) so these exercise the pid-file branches without touching
      // the network — no dependence on whether anything happens to be listening on the resolved port.
      test("no pid file, no live server -> non-zero, clear message, no foreground hint") {
        withTempDir { dir =>
          for {
            code <- Stop.run(dir.resolve("absent.pid"), ZIO.succeed(Option.empty[Int]))
            err  <- TestConsole.outputErr
          } yield assertTrue(
            code == ExitCode(1),
            err.exists(_.contains("no detached server running")),
            !err.exists(_.contains("still responding"))
          )
        }
      },
      test("stale pid file, no live server -> non-zero, message, and the file is removed") {
        withTempDir { dir =>
          val stale = dir.resolve("stale.pid")
          for {
            _       <- ZIO.attempt(Files.writeString(stale, deadPid().toString + "\n"))
            code    <- Stop.run(stale, ZIO.succeed(Option.empty[Int]))
            err     <- TestConsole.outputErr
            removed <- ZIO.attempt(!Files.exists(stale))
          } yield assertTrue(code == ExitCode(1), err.exists(_.contains("stale pid file")), removed)
        }
      },
      test("no detached pid but a server is still up -> appends the foreground hint naming its port") {
        withTempDir { dir =>
          for {
            code <- Stop.run(dir.resolve("absent.pid"), ZIO.succeed(Option(9137)))
            err  <- TestConsole.outputErr
          } yield assertTrue(code == ExitCode(1), err.exists(_.contains("still responding on 127.0.0.1:9137")))
        }
      },
      test("foregroundHint names the probed port and both ways to stop a foreground server") {
        val hint = Stop.foregroundHint(9137)
        assertTrue(
          hint.contains("127.0.0.1:9137"),
          hint.contains("Ctrl-C"),
          hint.contains("lsof -ti tcp:9137")
        )
      }
    ),
    suite("Status.describe")(
      test("ready with a live pid -> success, shows pid + db ok") {
        val (msg, code) = Status.describe(ready = true, up = true, livePid = Some(48213L), stalePid = false, port = 8080)
        assertTrue(code == ExitCode.success, msg == "running (ready)  pid 48213  127.0.0.1:8080  db ok")
      },
      test("ready with no pid file (foreground server) -> success, no pid shown") {
        val (msg, code) = Status.describe(ready = true, up = true, livePid = None, stalePid = false, port = 9000)
        assertTrue(code == ExitCode.success, msg == "running (ready)  127.0.0.1:9000  db ok")
      },
      test("up but db unavailable -> non-zero") {
        val (msg, code) = Status.describe(ready = false, up = true, livePid = Some(7L), stalePid = false, port = 8080)
        assertTrue(code == ExitCode(1), msg == "running (db unavailable)  pid 7  127.0.0.1:8080")
      },
      test("not responding but pid alive -> starting/unhealthy, non-zero") {
        val (msg, code) = Status.describe(ready = false, up = false, livePid = Some(7L), stalePid = false, port = 8080)
        assertTrue(code == ExitCode(1), msg == "starting or unhealthy (pid 7, no response on :8080)")
      },
      test("not responding, no pid -> not running, non-zero") {
        val (msg, code) = Status.describe(ready = false, up = false, livePid = None, stalePid = false, port = 8080)
        assertTrue(code == ExitCode(1), msg == "not running")
      },
      test("not responding, stale pid file -> not running (stale pid file), non-zero") {
        val (msg, code) = Status.describe(ready = false, up = false, livePid = None, stalePid = true, port = 8080)
        assertTrue(code == ExitCode(1), msg == "not running (stale pid file)")
      }
    )
  )
}
