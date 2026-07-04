package ccas.cli.serve

import java.nio.file.{Files, Path}

import zio.{ExitCode, ZIO}
import zio.test.{assertTrue, Spec, TestConsole, ZIOSpecDefault}

/** Deterministic, DB-free unit tests for the `ccas serve --detach` / `ccas stop` lifecycle. The pure decision logic
  * ([[Detach.reconstruct]], [[PidFile.parse]], [[PidFile.alreadyRunning]]) is tested directly; the effectful pid-file
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
    suite("Stop.run")(
      test("no pid file -> non-zero with a clear message") {
        withTempDir { dir =>
          for {
            code <- Stop.run(dir.resolve("absent.pid"))
            err  <- TestConsole.outputErr
          } yield assertTrue(code == ExitCode(1), err.exists(_.contains("no detached server running")))
        }
      },
      test("stale pid file -> non-zero, message, and the file is removed") {
        withTempDir { dir =>
          val stale = dir.resolve("stale.pid")
          for {
            _       <- ZIO.attempt(Files.writeString(stale, deadPid().toString + "\n"))
            code    <- Stop.run(stale)
            err     <- TestConsole.outputErr
            removed <- ZIO.attempt(!Files.exists(stale))
          } yield assertTrue(code == ExitCode(1), err.exists(_.contains("stale pid file")), removed)
        }
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
