package ccas.server.scheduler

import java.time.Instant

import com.typesafe.config.ConfigFactory
import zio.{durationLong, Clock, Duration, RIO, Scope, Task, UIO, URIO, URLayer, ZIO, ZLayer}

import ccas.analysis.apps.clubdata.ClubDataApp
import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubSlug, JobRunId}
import ccas.server.jobs.{JobCaps, JobKind, JobRunner}
import ccas.utils.ProgressDisplay
import ccas.utils.client.ChessComClient
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
              ProgressDisplay
                .sourced("scheduler")(
                  ZIO.logError(s"${schedule.kind} (club ${schedule.clubId}): invalid trigger: ${e.getMessage}")
                )
                .as(false)
            )
            .flatMap { due =>
              ZIO.whenDiscard(due) {
                runSchedule(schedule, now).catchAll {
                  // A job of this kind/club is already running (e.g. a forked job outliving its own
                  // intervalHours). Expected and benign: last_run_at stays put, so the next tick after
                  // the job ends submits promptly. Debug-log instead of ERROR-spamming for the duration.
                  case _: ConflictException =>
                    ProgressDisplay.sourced("scheduler")(
                      ZIO.logDebug(s"${schedule.kind} (club ${schedule.clubId}): already running, skipping tick")
                    )
                  case e =>
                    ProgressDisplay.sourced("scheduler")(
                      ZIO.logError(s"${schedule.kind} (club ${schedule.clubId}): ${e.getMessage}")
                    )
                }
              }
            }
        }
      } yield ())
        .catchAll(e => ProgressDisplay.sourced("scheduler")(ZIO.logError(s"Poll error: ${e.getMessage}")))

    private def runSchedule(schedule: JobSchedule, now: Instant): Task[Unit] = {
      def requireClubSlug: Task[ClubSlug] =
        ZIO.fromOption(schedule.clubId)
          .orElseFail(new IllegalStateException(s"${schedule.kind} schedule missing clubId"))
          .flatMap { cid =>
            Club.selectId(cid).provideEnvironment(pgClientEnv)
              .someOrFail(new IllegalStateException(s"Club $cid not found in database"))
              .map(_.slug)
          }

      // Decode `schedule.params` into typed per-kind options and thread them into the app call. Decoding is
      // eager (before `submit`), so a malformed row fails here — caught by the poll loop's per-schedule guard
      // — without submitting or advancing `last_run_at`. Absent params decode to the kind's all-`None`
      // defaults, reproducing the previous hardcoded behaviour exactly.
      val buildEffect: Task[Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]] =
        schedule.kind match {
          case JobKind.Recruitment =>
            ScheduleParams.decode(schedule.params, ScheduleParams.RecruitmentOptions.Default).map { opts => (jobRunId: Option[JobRunId]) =>
              requireClubSlug.flatMap(name =>
                RecruitmentApp.recruit(
                  name,
                  opts.alias.getOrElse("default"),
                  target = opts.target.map(_ min JobCaps.MaxTarget),
                  cumulative = opts.cumulative.getOrElse(false),
                  sourceClubs = opts.sourceClubs.getOrElse(Nil),
                  timeLimitMinutes = opts.timeLimitMinutes.map(_ min JobCaps.MaxTimeLimitMinutes).orElse(Some(30)),
                  explore = opts.explore.getOrElse(true),
                  trigger = RunTrigger.Scheduled,
                  jobRunId = jobRunId
                ).unit
              )
            }
          case JobKind.Membership =>
            ScheduleParams.decode(schedule.params, ScheduleParams.MembershipOptions.Default).map { opts => (jobRunId: Option[JobRunId]) =>
              requireClubSlug.flatMap(name =>
                MembershipApp.reconcileAndReport(name, opts.trustUsernames.getOrElse(true), RunTrigger.Scheduled, jobRunId).unit
              )
            }
          case JobKind.MatchRef =>
            ScheduleParams.decode(schedule.params, ScheduleParams.MatchRefOptions.Default).map { opts => (_: Option[JobRunId]) =>
              RefApp.populate(opts.forceSkipped.getOrElse(false), opts.upgradeRefs.getOrElse(false)).unit
            }
          case JobKind.History =>
            ScheduleParams.decode(schedule.params, ScheduleParams.HistoryOptions.Default).map { opts =>
              val refresh = ScheduleParams.HistoryOptions.effectiveRefresh(opts)
              (jobRunId: Option[JobRunId]) =>
                requireClubSlug.flatMap(name =>
                  HistoryApp.discover(
                    name,
                    opts.full.getOrElse(false),
                    opts.includeFinished.getOrElse(false),
                    refresh,
                    RunTrigger.Scheduled,
                    jobRunId = jobRunId
                  ).unit
                )
            }
          case JobKind.Stats =>
            ScheduleParams.decode(schedule.params, ScheduleParams.StatsOptions.Default).flatMap { opts =>
              ScheduleParams.statsPeriod(opts).map { periodOpt => (_: Option[JobRunId]) =>
                requireClubSlug.flatMap(name =>
                  periodOpt match {
                    case Some((since, until)) =>
                      StatsApp.playerOfPeriodAndReport(name, since, until, opts.minGames.getOrElse(1)).unit
                    case None =>
                      StatsApp.memberStatsAndReport(name).unit
                  }
                )
              }
            }
          case JobKind.ClubData =>
            ScheduleParams.decode(schedule.params, ScheduleParams.ClubDataOptions.Default).map { opts => (_: Option[JobRunId]) =>
              ClubDataApp.refresh(opts.minAgeHours).unit
            }
        }

      for {
        effect <- buildEffect
        _ <- runner.submit(schedule.kind, schedule.clubId, schedule.params, RunTrigger.Scheduled, effect)
               .provideEnvironment(pgClientEnv)
        _ <- JobSchedule.updateLastRunAt(schedule.id, now).provideEnvironment(pgClientEnv).unit
      } yield ()
    }
  }
}
