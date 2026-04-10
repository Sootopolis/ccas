package ccas.server.scheduler

import java.time.temporal.ChronoUnit
import java.time.Instant

import com.typesafe.config.ConfigFactory
import zio.{durationLong, Duration, Scope, Task, UIO, URIO, URLayer, ZIO, ZLayer}

import ccas.analysis.apps.clubdata.ClubDataApp
import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubSlug, JobRunId}
import ccas.server.jobs.{JobKind, JobRunner}
import ccas.utils.CcasLogger
import ccas.utils.sql.PostgresClient

trait JobScheduler {
  def start: URIO[Scope, Unit]
}

object JobScheduler {

  val live: URLayer[CcasLogger & JobRunner & PostgresClient, JobScheduler] =
    ZLayer.fromFunction { (logger: CcasLogger, runner: JobRunner, pgClient: PostgresClient) =>
      val config = ConfigFactory.load()
      val pollMinutes =
        if config.hasPath("scheduler.pollIntervalMinutes")
        then config.getInt("scheduler.pollIntervalMinutes")
        else 5 // default poll interval in minutes
      val pollInterval = pollMinutes.toLong.minutes
      new JobSchedulerLive(logger, runner, pgClient, pollInterval)
    }

  private[scheduler] class JobSchedulerLive(logger: CcasLogger, runner: JobRunner, pgClient: PostgresClient, pollInterval: Duration)
      extends JobScheduler {

    private val pgClientEnv = zio.ZEnvironment(pgClient)
    private val loggerEnv     = zio.ZEnvironment(logger)

    override def start: URIO[Scope, Unit] =
      pollLoop
        .repeat(zio.Schedule.fixed(pollInterval))
        .forkScoped
        .unit

    private def pollLoop: UIO[Unit] =
      (for {
        schedules <- JobSchedule.selectEnabled.provideEnvironment(pgClientEnv)
        now = Instant.now()
        _ <- ZIO.foreachDiscard(schedules) { schedule =>
          val isDue = schedule.lastRunAt.forall(ts => ChronoUnit.HOURS.between(ts, now) >= schedule.intervalHours)
          ZIO.whenDiscard(isDue) {
            runSchedule(schedule, now).catchAll { e =>
              CcasLogger.error(s"[Scheduler] ${schedule.kind} (club ${schedule.clubId}): ${e.getMessage}")
                .provideEnvironment(loggerEnv)
            }
          }
        }
      } yield ())
        .catchAll(e => CcasLogger.error(s"[Scheduler] Poll error: ${e.getMessage}").provideEnvironment(loggerEnv))

    private def runSchedule(schedule: JobSchedule, now: Instant): Task[Unit] = {
      def requireClubSlug: Task[ClubSlug] =
        ZIO.fromOption(schedule.clubId)
          .orElseFail(new IllegalStateException(s"${schedule.kind} schedule missing clubId"))
          .flatMap { cid =>
            Club.selectId(cid).provideEnvironment(pgClientEnv)
              .someOrFail(new IllegalStateException(s"Club $cid not found in database"))
              .map(_.slug)
          }

      val effect = schedule.kind match {
        case JobKind.Recruitment =>
          (jobRunId: Option[JobRunId]) =>
            requireClubSlug.flatMap(name =>
              RecruitmentApp.recruit(
                name,
                "default",
                timeLimitMinutes = Some(30),
                trigger = RunTrigger.Scheduled,
                jobRunId = jobRunId
              ).unit
            )
        case JobKind.Membership =>
          (jobRunId: Option[JobRunId]) =>
            requireClubSlug.flatMap(name =>
              MembershipApp.reconcile(name, trigger = RunTrigger.Scheduled, jobRunId = jobRunId).unit
            )
        case JobKind.MatchRef =>
          (_: Option[JobRunId]) => RefApp.populate(forceSkipped = false, upgradeRefs = false).unit
        case JobKind.History =>
          (jobRunId: Option[JobRunId]) =>
            requireClubSlug.flatMap(name =>
              HistoryApp.discover(name, trigger = RunTrigger.Scheduled, jobRunId = jobRunId).unit
            )
        case JobKind.Stats =>
          (_: Option[JobRunId]) =>
            requireClubSlug.flatMap(name => StatsApp.memberStats(name).unit)
        case JobKind.ClubData =>
          (_: Option[JobRunId]) => ClubDataApp.refresh.unit
      }

      runner.submit(schedule.kind, schedule.clubId, schedule.params, RunTrigger.Scheduled, effect)
        .provideEnvironment(pgClientEnv) *>
        JobSchedule.updateLastRunAt(schedule.id, now).provideEnvironment(pgClientEnv).unit
    }
  }
}
