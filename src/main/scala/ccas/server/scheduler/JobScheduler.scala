package ccas.server.scheduler

import java.time.Instant

import com.typesafe.config.ConfigFactory
import zio.{durationLong, Clock, Duration, Scope, Task, UIO, URIO, URLayer, ZIO, ZLayer}

import ccas.analysis.apps.clubdata.ClubDataApp
import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubSlug, JobRunId}
import ccas.server.jobs.{JobKind, JobRunner}
import ccas.utils.errors.ConflictException
import ccas.utils.sql.PostgresClient

trait JobScheduler {
  def start: URIO[Scope, Unit]
}

object JobScheduler {

  val live: URLayer[JobRunner & PostgresClient, JobScheduler] =
    ZLayer.fromFunction { (runner: JobRunner, pgClient: PostgresClient) =>
      val config = ConfigFactory.load()
      val pollMinutes =
        if config.hasPath("scheduler.pollIntervalMinutes")
        then config.getInt("scheduler.pollIntervalMinutes")
        else 15 // default poll interval in minutes
      val pollInterval = pollMinutes.toLong.minutes
      new JobSchedulerLive(runner, pgClient, pollInterval)
    }

  private[scheduler] class JobSchedulerLive(runner: JobRunner, pgClient: PostgresClient, pollInterval: Duration)
      extends JobScheduler {

    private val pgClientEnv = zio.ZEnvironment(pgClient)

    // Skip lateness ceiling for cron `Skip` schedules: a boundary may land just after a poll (observed up to one
    // pollInterval late) plus headroom for poll execution / DB latency. CatchUp ignores this; Interval is unaffected.
    private val grace: Duration = pollInterval.multipliedBy(2)

    override def start: URIO[Scope, Unit] =
      pollLoop
        .repeat(zio.Schedule.fixed(pollInterval))
        .forkScoped
        .unit

    private def pollLoop: UIO[Unit] =
      (for {
        schedules <- JobSchedule.selectEnabled.provideEnvironment(pgClientEnv)
        now       <- Clock.instant
        _ <- ZIO.foreachDiscard(schedules) { schedule =>
          // `isDue` decodes the trigger and can throw on a malformed (hand-edited) cron row; isolate that so one
          // bad row logs and is treated as not-due rather than aborting the whole poll iteration.
          ZIO
            .attempt(schedule.isDue(now, grace))
            .catchAll(e =>
              ZIO
                .logError(s"[Scheduler] ${schedule.kind} (club ${schedule.clubId}): invalid trigger: ${e.getMessage}")
                .as(false)
            )
            .flatMap { due =>
              ZIO.whenDiscard(due) {
                runSchedule(schedule, now).catchAll {
                  // A job of this kind/club is already running (e.g. a forked job outliving its own
                  // intervalHours). Expected and benign: last_run_at stays put, so the next tick after
                  // the job ends submits promptly. Debug-log instead of ERROR-spamming for the duration.
                  case _: ConflictException =>
                    ZIO.logDebug(s"[Scheduler] ${schedule.kind} (club ${schedule.clubId}): already running, skipping tick")
                  case e =>
                    ZIO.logError(s"[Scheduler] ${schedule.kind} (club ${schedule.clubId}): ${e.getMessage}")
                }
              }
            }
        }
      } yield ())
        .catchAll(e => ZIO.logError(s"[Scheduler] Poll error: ${e.getMessage}"))

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
              MembershipApp.reconcileAndReport(name, trustUsernames = true, RunTrigger.Scheduled, jobRunId).unit
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
            requireClubSlug.flatMap(name => StatsApp.memberStatsAndReport(name).unit)
        case JobKind.ClubData =>
          (_: Option[JobRunId]) => ClubDataApp.refresh(minAgeHours = None).unit
      }

      runner.submit(schedule.kind, schedule.clubId, schedule.params, RunTrigger.Scheduled, effect)
        .provideEnvironment(pgClientEnv) *>
        JobSchedule.updateLastRunAt(schedule.id, now).provideEnvironment(pgClientEnv).unit
    }
  }
}
