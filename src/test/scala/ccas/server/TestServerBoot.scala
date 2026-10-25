package ccas.server

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentLinkedQueue

import org.postgresql.ds.PGSimpleDataSource
import zio.{Exit, Fiber, Promise, Ref, RIO, Runtime, Scope, Task, UIO, URIO, ZIO, ZLayer, ZLogger}
import zio.http.{Client, Server}
import zio.http.netty.NettyConfig
import zio.stream.ZStream
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault, ZTestLogger}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.cli.CcasApiClient
import ccas.server.jobs.{JobEffect, JobKind, JobLogs, JobRun, JobRunner}
import ccas.server.routes.JobRoutes.JobStatusResponse
import ccas.server.scheduler.JobScheduler
import ccas.utils.client.{BodyStore, FsBodyStore, TestChessComClientSupport}
import ccas.utils.sql.{DatabaseAddress, PostgresClient}

/** What `CcasServer` does at boot: the pid file a detached server owns, what it says and asks about its database
  * (#320), the order it shuts down in and the logger its HTTP server inherits (#334). The boot line's client never
  * connects: the line has to come out before the first connection, so a boot that fails to connect still names where
  * it tried. The terminal conversation itself is not reachable from a test; [[BootGuard]]'s decisions are.
  */
object TestServerBoot extends ZIOSpecDefault {

  private val local  = Some(DatabaseAddress("localhost:5432", "/ccas"))
  private val remote = Some(DatabaseAddress("ep-x.neon.tech", "/neondb"))

  override def spec: Spec[Any, Throwable] = suite("TestServerBoot")(
    test("a detached server writes its own pid to the pid file, and deletes the file on shutdown") {
      for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-pid"))
        path = dir.resolve("state").resolve("ccas-8080.pid")
        during <- ZIO.scoped(CcasServer.pidFileManaged(Some(path)) *> ZIO.attemptBlocking(Files.readString(path)))
        after  <- ZIO.attemptBlocking(Files.exists(path))
      } yield assertTrue(during == s"${ProcessHandle.current().pid()}\n", !after)
    },
    test(
      "shutdown stops the loops, then the running jobs, then the HTTP server, before the job runner and the pool are " +
        "released"
    ) {
      for {
        (exit, events) <- serveThenInterrupt(pidFileOption = None)
      } yield assertTrue(
        exit.isInterruptedOnly,
        events.take(2).toSet == Set("scheduler loop interrupted", "retention sweep interrupted"),
        events.slice(2, 4) == Vector("jobs interrupted", "server stopped"),
        events.drop(4).toSet == Set("job runner released", "pool closed")
      )
    },
    test("a detached server's pid file outlives the services it owns") {
      for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-pid"))
        path = dir.resolve("ccas-8080.pid")
        (exit, events) <- serveThenInterrupt(pidFileOption = Some(path))
        after          <- ZIO.attemptBlocking(Files.exists(path))
      } yield assertTrue(exit.isInterruptedOnly, events.contains("pid file present at pool close"), !after)
    },
    test("a client following a running job doesn't hold shutdown for the HTTP server's drain") {
      for {
        booted    <- boot(pidFileOption = None)
        api       <- CcasApiClient.live(s"http://127.0.0.1:${booted.port}")
        following <- Promise.make[Nothing, Unit]
        follow    <- api.streamLines("/api/jobs/job-1/logs")(_ => following.succeed(()).unit).fork
        _         <- following.await.raceFirst(follow.join *> ZIO.never)
        // zio-http's drain gives up after 10 s, so a follow that held it would take at least that.
        (elapsed, exit) <- booted.fiber.interrupt.timed
        followed        <- follow.join.exit
      } yield assertTrue(exit.isInterruptedOnly, followed.isSuccess, elapsed.toMillis < 5000)
    }.provide(Client.default) @@ TestAspect.withLiveClock,
    // So the server's log keeps one format, and zio-http's warning for a read-idle reap meets ADR 0015's filter.
    test("the HTTP server logs through the logger the services install, as the loops and jobs do") {
      for {
        booted <- boot(pidFileOption = None)
        api    <- CcasApiClient.live(s"http://127.0.0.1:${booted.port}")
        _      <- api.getJson[JobStatusResponse]("/api/jobs/job-1").exit
        _      <- booted.fiber.interrupt
      } yield assertTrue(booted.serviceLog.contains("status of job-1 requested"))
    }.provide(Client.default) @@ TestAspect.withLiveClock,
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
      }
    )
  )

  /** A `CcasServer.serveWith` running in the background over stub services, with both loops started and the HTTP
    * server listening on `port`. `events` is what the stubs have recorded, in order, and `serviceLog` every message
    * logged through the logger the stub services install, as `ProgressDisplay.live` installs its own.
    */
  private final case class Booted(
    fiber: Fiber[Throwable, Unit],
    port: Int,
    events: Ref[Vector[String]],
    serviceLog: ConcurrentLinkedQueue[String]
  )

  private def boot(pidFileOption: Option[Path]): Task[Booted] =
    for {
      events           <- Ref.make(Vector.empty[String])
      serviceLog       <- ZIO.succeed(new ConcurrentLinkedQueue[String]())
      schedulerStarted <- Promise.make[Nothing, Unit]
      sweepStarted     <- Promise.make[Nothing, Unit]
      jobsInterrupted  <- Promise.make[Nothing, Unit]
      built            <- Promise.make[Nothing, Server]
      services = stubServices(events, serviceLog, schedulerStarted, sweepStarted, jobsInterrupted, pidFileOption)
      fiber <- ZIO.scoped(CcasServer.serveWith(pidFileOption, services, markedServer(events, built))).fork
      ready = schedulerStarted.await *> sweepStarted.await *> built.await.flatMap(_.port)
      // Racing the join turns a boot that fails before it is ready into a failure rather than a hang.
      port <- ready.raceFirst(fiber.join *> ZIO.never)
    } yield Booted(fiber, port, events, serviceLog)

  /** Boots, then interrupts the server as a SIGTERM would. Returns its exit and the events the stubs recorded. */
  private def serveThenInterrupt(
    pidFileOption: Option[Path]
  ): ZIO[Any, Throwable, (Exit[Throwable, Unit], Vector[String])] =
    for {
      booted   <- boot(pidFileOption)
      exit     <- booted.fiber.interrupt
      recorded <- booted.events.get
    } yield (exit, recorded)

  private def stubServices(
    events: Ref[Vector[String]],
    serviceLog: ConcurrentLinkedQueue[String],
    schedulerStarted: Promise[Nothing, Unit],
    sweepStarted: Promise[Nothing, Unit],
    jobsInterrupted: Promise[Nothing, Unit],
    pidFileOption: Option[Path]
  ): ZLayer[Any, Throwable, CcasServer.Served] = {
    def record(event: String): UIO[Unit] = events.update(_ :+ event)
    val pidFileAtPoolClose = ZIO.foreachDiscard(pidFileOption) { path =>
      ZIO.whenZIODiscard(ZIO.attemptBlocking(Files.exists(path)).orDie)(record("pid file present at pool close"))
    }
    Runtime.addLogger(ZLogger.simple[String, Any](serviceLog.add)) ++ ZLayer.make[CcasServer.Served](
      ZLayer.succeed[JobScheduler](new LoopingScheduler(schedulerStarted, record("scheduler loop interrupted"))),
      ZLayer.scoped(
        ZIO.acquireRelease(ZIO.succeed[JobRunner](new StubJobRunner(record, sweepStarted, jobsInterrupted))) { _ =>
          record("job runner released")
        }
      ),
      // A data source that is never asked for a connection, so the pool's release is all the test sees of it.
      ZLayer.scoped(
        ZIO.acquireRelease(ZIO.succeed(PostgresClient.fromDataSource(new PGSimpleDataSource()))) { _ =>
          pidFileAtPoolClose *> record("pool closed")
        }
      ),
      ZLayer.succeed[BodyStore](new FsBodyStore(Paths.get("target/test-bodies"))),
      TestChessComClientSupport.dummyLayer
    )
  }

  // The marker is built first, so its finalizer runs once the server's own release has finished. Netty's fast shutdown
  // skips the default 2 s quiet period, so a healthy stop takes milliseconds and the follow test's bound has room.
  private def markedServer(
    events: Ref[Vector[String]],
    built: Promise[Nothing, Server]
  ): ZLayer[Any, Throwable, Server] = {
    val config = ZLayer.succeed(Server.Config.default.binding("127.0.0.1", 0))
    ZLayer
      .scoped(ZIO.addFinalizer(events.update(_ :+ "server stopped")))
      .flatMap(_ => (config ++ ZLayer.succeed(NettyConfig.defaultWithFastShutdown)) >>> Server.customized)
      .tap(env => built.succeed(env.get[Server]))
  }

  private final class LoopingScheduler(started: Promise[Nothing, Unit], interrupted: UIO[Unit]) extends JobScheduler {
    override def start: URIO[Scope, Unit] = (started.succeed(()) *> ZIO.never).onInterrupt(interrupted).forkScoped.unit
  }

  /** A job runner with one job running, whose log stream stays open until `interruptAll`, as the real runner's does.
    * Its `sweepLogs`, the first half of the retention pass a boot starts at once, never finishes. Its `status` logs, as
    * a request handler might, then finds no job.
    */
  private final class StubJobRunner(
    record: String => UIO[Unit],
    sweepStarted: Promise[Nothing, Unit],
    jobsInterrupted: Promise[Nothing, Unit]
  ) extends JobRunner {
    private def unused: Nothing = throw new NotImplementedError("TestServerBoot drives a sweep, a follow and shutdown")

    override def sweepLogs: URIO[PostgresClient, Int] =
      (sweepStarted.succeed(()) *> ZIO.never).onInterrupt(record("retention sweep interrupted"))

    override def interruptAll: UIO[Unit] = record("jobs interrupted") *> jobsInterrupted.succeed(()).unit

    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs] =
      ZIO.succeed(JobLogs.Streaming(ZStream.succeed("started") ++ ZStream.fromZIO(jobsInterrupted.await).drain))

    override def submit(
      kind: JobKind,
      clubIdOption: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: JobEffect
    ): RIO[PostgresClient, JobRunId] = unused

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] =
      ZIO.logInfo(s"status of $id requested").as(None)

    override def cancel(id: JobRunId): UIO[Boolean]                        = unused
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = unused

    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = unused
  }
}
