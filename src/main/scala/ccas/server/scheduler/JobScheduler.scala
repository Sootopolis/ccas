package ccas.server.scheduler

import java.time.temporal.ChronoUnit
import java.time.Instant

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import zio.{durationLong, Duration, Task, UIO, ZIO, ZLayer}

import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.matchref.MatchRefApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.api.misc.subtypes.ClubUrlName
import ccas.server.jobs.{JobKind, JobRunner}

trait JobScheduler {
  def start: UIO[Unit]
}

object JobScheduler {

  val live: ZLayer[JobRunner & Transactor, Nothing, JobScheduler] =
    ZLayer.fromFunction { (runner: JobRunner, xa: Transactor) =>
      val config = ConfigFactory.load()
      val pollMinutes =
        if config.hasPath("scheduler.pollIntervalMinutes")
        then config.getInt("scheduler.pollIntervalMinutes")
        else 5
      val pollInterval = pollMinutes.toLong.minutes
      new JobSchedulerLive(runner, xa, pollInterval)
    }

  private class JobSchedulerLive(runner: JobRunner, xa: Transactor, pollInterval: Duration) extends JobScheduler {

    private val env = zio.ZEnvironment(xa)

    override def start: UIO[Unit] =
      pollLoop
        .repeat(zio.Schedule.fixed(pollInterval))
        .forkDaemon
        .unit

    private def pollLoop: UIO[Unit] =
      (for {
        schedules <- JobSchedule.selectEnabled.provideEnvironment(env)
        now = Instant.now()
        _ <- ZIO.foreachDiscard(schedules) { schedule =>
          val isDue = schedule.lastRunAt.forall(ts => ChronoUnit.HOURS.between(ts, now) >= schedule.intervalHours)
          ZIO.whenDiscard(isDue)(runSchedule(schedule, now))
        }
      } yield ())
        .catchAll(e => ZIO.logError(s"[Scheduler] Error: ${e.getMessage}"))

    private def runSchedule(schedule: JobSchedule, now: Instant): Task[Unit] = {
      def requireClubUrlName: Task[ClubUrlName] =
        ZIO.fromOption(schedule.clubUrlName)
          .orElseFail(new IllegalStateException(s"${schedule.kind} schedule missing clubUrlName"))

      val effect = schedule.kind match
        case JobKind.Recruitment =>
          requireClubUrlName.flatMap(name => RecruitmentApp.recruit(name, "default", timeLimitMinutes = Some(30)).unit)
        case JobKind.Membership =>
          requireClubUrlName.flatMap(name => MembershipApp.reconcile(name).unit)
        case JobKind.MatchRef =>
          MatchRefApp.populate
        case JobKind.History =>
          requireClubUrlName.flatMap(name => HistoryApp.discover(name).unit)

      runner.submit(schedule.kind, schedule.clubUrlName, schedule.params, effect)
        .provideEnvironment(env) *>
        JobSchedule.updateLastRunAt(schedule.id, now).provideEnvironment(env).unit
    }
  }
}
