package ccas.server

import java.nio.file.Files

import zio.ZIO
import zio.test.{assertTrue, Spec, ZIOSpecDefault, ZTestLogger}

import ccas.utils.sql.{DatabaseAddress, PostgresClient}

/** What `CcasServer` does at boot: the pid file a detached server owns, and what it says and asks about its database
  * (#320). The boot line's client never connects: the line has to come out before the first connection, so a boot
  * that fails to connect still names where it tried. The terminal conversation itself is not reachable from a test;
  * [[BootGuard]]'s decisions are.
  */
object TestServerBoot extends ZIOSpecDefault {

  private val local  = Some(DatabaseAddress("localhost:5432", "/ccas"))
  private val remote = Some(DatabaseAddress("ep-x.neon.tech", "/neondb"))

  override def spec: Spec[Any, Throwable] = suite("TestServerBoot")(
    test("a detached server writes its own pid to the pid file, and deletes the file on shutdown") {
      for {
        dir    <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-pid"))
        path    = dir.resolve("state").resolve("ccas.pid")
        during <- ZIO.scoped(CcasServer.pidFileManaged(Some(path)) *> ZIO.attemptBlocking(Files.readString(path)))
        after  <- ZIO.attemptBlocking(Files.exists(path))
      } yield assertTrue(during == s"${ProcessHandle.current().pid()}\n", !after)
    },
    test("a boot names its database before it connects, so a boot that can't connect still says where it tried") {
      for {
        exit <- ZIO.scoped(PostgresClient.live(prefix = "databaseUnreachable", onInit = CcasServer.poolInit).build).exit
        logs <- ZTestLogger.logOutput
        messages = logs.map(_.message())
      } yield assertTrue(
        exit.isFailure,
        messages.contains("Database: 127.0.0.1:1/nowhere"),
        !messages.exists(_.contains("alice")),
        !messages.exists(_.contains("hunter2"))
      )
    }.provide(ZTestLogger.default),
    suite("BootGuard")(
      test("a loopback database boots without asking") {
        assertTrue(BootGuard.question(resolved = Right(local), detachedChild = false).isEmpty)
      },
      test("a remote database asks before booting, naming it") {
        assertTrue(
          BootGuard
            .question(resolved = Right(remote), detachedChild = false)
            .contains("Boot against ep-x.neon.tech/neondb? [y/N] ")
        )
      },
      test("a database whose address can't be read asks before booting") {
        assertTrue(
          BootGuard
            .question(resolved = Right(None), detachedChild = false)
            .contains("Boot against a database whose address can't be read from the configuration? [y/N] ")
        )
      },
      test("a detached child never asks, since its parent did") {
        assertTrue(BootGuard.question(resolved = Right(remote), detachedChild = true).isEmpty)
      },
      test("a config that doesn't resolve boots without asking, so the pool fails with its own message") {
        val unresolved = Left(new IllegalArgumentException("database.url is blank"))
        assertTrue(BootGuard.question(resolved = unresolved, detachedChild = false).isEmpty)
      },
      test("only y or yes confirms; a blank answer or EOF aborts") {
        assertTrue(
          BootGuard.accepts(Some("y")),
          BootGuard.accepts(Some(" YES ")),
          !BootGuard.accepts(Some("")),
          !BootGuard.accepts(Some("n")),
          !BootGuard.accepts(Some("yep")),
          !BootGuard.accepts(None)
        )
      }
    )
  )
}
