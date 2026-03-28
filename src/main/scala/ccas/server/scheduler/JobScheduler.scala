package ccas.server.scheduler

import java.time.temporal.ChronoUnit
import java.time.Instant

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import zio.{durationLong, Duration, Task, UIO, ZIO, ZLayer}

import ccas.analysis.apps.history.HistoryApp
import ccas.utils.CcasLogger
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.jobs.{JobKind, JobRunner}

trait JobScheduler {
  def start: UIO[Unit]
}

object JobScheduler {

  val live: ZLayer[CcasLogger & JobRunner & Transactor, Nothing, JobScheduler] =
    ZLayer.fromFunction { (logger: CcasLogger, runner: JobRunner, xa: Transactor) =>
      val config = ConfigFactory.load()
      val pollMinutes =
        if config.hasPath("scheduler.pollIntervalMinutes")
        then config.getInt("scheduler.pollIntervalMinutes")
        else 5 // default poll interval in minutes
      val pollInterval = pollMinutes.toLong.minutes
      new JobSchedulerLive(logger, runner, xa, pollInterval)
    }

  private class JobSchedulerLive(logger: CcasLogger, runner: JobRunner, xa: Transactor, pollInterval: Duration) extends JobScheduler {

    private val transactorEnv = zio.ZEnvironment(xa)
    private val loggerEnv = zio.ZEnvironment(logger)

    override def start: UIO[Unit] =
      pollLoop
        .repeat(zio.Schedule.fixed(pollInterval))
        .forkDaemon
        .unit

    private def pollLoop: UIO[Unit] =
      (for {
        schedules <- JobSchedule.selectEnabled.provideEnvironment(transactorEnv)
        now = Instant.now()
        _ <- ZIO.foreachDiscard(schedules) { schedule =>
          val isDue = schedule.lastRunAt.forall(ts => ChronoUnit.HOURS.between(ts, now) >= schedule.intervalHours)
          ZIO.whenDiscard(isDue)(runSchedule(schedule, now))
        }
      } yield ())
        .catchAll(e => CcasLogger.error(s"[Scheduler] Error: ${e.getMessage}").provideEnvironment(loggerEnv))

    private def runSchedule(schedule: JobSchedule, now: Instant): Task[Unit] = {
      def requireClubSlug: Task[ClubSlug] =
        ZIO.fromOption(schedule.clubSlug)
          .orElseFail(new IllegalStateException(s"${schedule.kind} schedule missing clubSlug"))

      // Job kind dispatch kept in sync with JobRunner.runJob follow-up logic
      val effect = schedule.kind match {
        case JobKind.Recruitment =>
          requireClubSlug.flatMap(name => RecruitmentApp.recruit(name, "default", timeLimitMinutes = Some(30), trigger = RunTrigger.Scheduled).unit)
        case JobKind.Membership =>
          requireClubSlug.flatMap(name => MembershipApp.reconcile(name, trigger = RunTrigger.Scheduled).unit)
        case JobKind.MatchRef =>
          RefApp.populate(RunTrigger.Scheduled)
        case JobKind.History =>
          requireClubSlug.flatMap(name => HistoryApp.discover(name, trigger = RunTrigger.Scheduled).unit)
      }

      runner.submit(schedule.kind, schedule.clubSlug, schedule.params, RunTrigger.Scheduled, effect)
        .provideEnvironment(transactorEnv) *>
        JobSchedule.updateLastRunAt(schedule.id, now).provideEnvironment(transactorEnv).unit
    }
  }
}
