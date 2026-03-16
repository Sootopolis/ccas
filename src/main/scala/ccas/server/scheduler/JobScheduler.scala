package ccas.server.scheduler

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import zio.{durationLong, Console, Duration, ZIO, ZLayer}

import ccas.analysis.apps.matchref.MatchRefApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.server.jobs.{JobKind, JobRunner}

trait JobScheduler {
  def start: ZIO[Any, Nothing, Unit]
}

object JobScheduler {

  val live: ZLayer[JobRunner & Transactor, Nothing, JobScheduler] =
    ZLayer.fromFunction { (runner: JobRunner, xa: Transactor) =>
      val config      = ConfigFactory.load()
      val pollMinutes = if config.hasPath("scheduler.pollIntervalMinutes")
                        then config.getInt("scheduler.pollIntervalMinutes")
                        else 5
      val pollInterval = pollMinutes.toLong.minutes
      new JobSchedulerLive(runner, xa, pollInterval)
    }

  private class JobSchedulerLive(
      runner: JobRunner,
      xa: Transactor,
      pollInterval: Duration
  ) extends JobScheduler {

    private val env = zio.ZEnvironment(xa)

    override def start: ZIO[Any, Nothing, Unit] =
      pollLoop
        .repeat(zio.Schedule.fixed(pollInterval))
        .forkDaemon
        .unit

    private def pollLoop: ZIO[Any, Nothing, Unit] =
      (for {
        schedules <- JobSchedule.selectEnabled.provideEnvironment(env)
        now        = Instant.now()
        _ <- ZIO.foreachDiscard(schedules) { schedule =>
          val isDue = schedule.lastRunAt match
            case None     => true
            case Some(ts) => ChronoUnit.HOURS.between(ts, now) >= schedule.intervalHours
          ZIO.when(isDue)(runSchedule(schedule, now))
        }
      } yield ())
        .catchAll(e => Console.printLine(s"[Scheduler] Error: ${e.getMessage}").orDie)

    private def runSchedule(schedule: JobSchedule, now: Instant): ZIO[Any, Throwable, Unit] = {
      val effect = schedule.kind match
        case JobKind.Recruitment =>
          val clubUrlName = schedule.clubUrlName.getOrElse(
            throw new IllegalStateException("Recruitment schedule missing clubUrlName")
          )
          RecruitmentApp.recruit(clubUrlName, "default", timeLimitMinutes = Some(30)).unit
        case JobKind.Membership =>
          val clubUrlName = schedule.clubUrlName.getOrElse(
            throw new IllegalStateException("Membership schedule missing clubUrlName")
          )
          MembershipApp.reconcile(clubUrlName).unit
        case JobKind.MatchRef =>
          MatchRefApp.populate

      runner.submit(schedule.kind, schedule.clubUrlName, schedule.params, effect)
        .provideEnvironment(env) *>
        JobSchedule.updateLastRunAt(schedule.id, now).provideEnvironment(env).unit
    }
  }
}
