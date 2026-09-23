package ccas.server.scheduler

import java.time.Instant

import com.typesafe.config.ConfigFactory
import zio.{durationLong, Clock, Duration, Schedule, Scope, Task, UIO, URIO, URLayer, ZEnvironment, ZIO, ZLayer}

import ccas.analysis.apps.{ClubQuery, ClubResolution}
import ccas.analysis.apps.clubdata.ClubDataApp
import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.RunTrigger
import ccas.server.jobs.{ClubJobEffect, JobCaps, JobEffect, JobKind, JobRunner}
import ccas.server.scheduler.ScheduleParams.{
  ClubDataOptions,
  HistoryOptions,
  MatchRefOptions,
  MembershipOptions,
  RecruitmentOptions,
  StatsOptions
}
import ccas.utils.ProgressDisplay
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

    private val pgClientEnv = ZEnvironment(pgClient)

    // Skip lateness ceiling for cron `Skip` schedules: a boundary may land just after a poll (observed up to one
    // pollInterval late) plus headroom for poll execution / DB latency. CatchUp ignores this; Interval is unaffected.
    private val grace: Duration = pollInterval.multipliedBy(2)

    override def start: URIO[Scope, Unit] =
      pollLoop
        .repeat(Schedule.fixed(pollInterval))
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
                  ZIO.logError(s"${schedule.kind} (club ${schedule.clubIdOption}): invalid trigger: ${e.getMessage}")
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
                      ZIO.logDebug(s"${schedule.kind} (club ${schedule.clubIdOption}): already running, skipping tick")
                    )
                  case e =>
                    ProgressDisplay.sourced("scheduler")(
                      ZIO.logError(s"${schedule.kind} (club ${schedule.clubIdOption}): ${e.getMessage}")
                    )
                }
              }
            }
        }
      } yield ())
        .catchAll(e => ProgressDisplay.sourced("scheduler")(ZIO.logError(s"Poll error: ${e.getMessage}")))

    private def runSchedule(schedule: JobSchedule, now: Instant): Task[Unit] =
      (for {
        effect <- jobEffect(schedule)
        _      <- runner.submit(schedule.kind, schedule.clubIdOption, schedule.params, RunTrigger.Scheduled, effect)
        _      <- JobSchedule.updateLastRunAt(schedule.id, now)
      } yield ()).provideEnvironment(pgClientEnv)

    // Decode `schedule.params` into typed per-kind options and thread them into the app call. Decoding is eager (before
    // `submit`), so a malformed row fails here — caught by the poll loop's per-schedule guard — without submitting or
    // advancing `last_run_at`. Absent params decode to the kind's all-`None` defaults.
    private def jobEffect(schedule: JobSchedule): Task[JobEffect] =
      schedule.kind match {
        case JobKind.Recruitment =>
          ScheduleParams.decode(schedule.params, RecruitmentOptions.Default).map { opts =>
            clubJob(schedule) { (club, jobRunIdOption) =>
              RecruitmentApp.recruit(
                clubSlug = club.slug,
                expectedClubIdOption = Some(club.clubId),
                alias = opts.alias.getOrElse("default"),
                target = opts.target.map(_ min JobCaps.MaxTarget),
                cumulative = opts.cumulative.getOrElse(false),
                sourceClubs = opts.sourceClubs.getOrElse(Nil),
                timeLimitMinutes = opts.timeLimitMinutes.map(_ min JobCaps.MaxTimeLimitMinutes).orElse(Some(30)),
                explore = opts.explore.getOrElse(true),
                trigger = RunTrigger.Scheduled,
                jobRunIdOption = jobRunIdOption
              )
            }
          }
        case JobKind.Membership =>
          ScheduleParams.decode(schedule.params, MembershipOptions.Default).map { opts =>
            clubJob(schedule) { (club, jobRunIdOption) =>
              MembershipApp.reconcileAndReport(
                clubSlug = club.slug,
                expectedClubIdOption = Some(club.clubId),
                trustUsernames = opts.trustUsernames.getOrElse(true),
                trigger = RunTrigger.Scheduled,
                jobRunIdOption = jobRunIdOption
              )
            }
          }
        case JobKind.MatchRef =>
          ScheduleParams.decode(schedule.params, MatchRefOptions.Default).map { opts => _ =>
            RefApp.populate(opts.forceSkipped.getOrElse(false), opts.upgradeRefs.getOrElse(false))
          }
        case JobKind.History =>
          ScheduleParams.decode(schedule.params, HistoryOptions.Default).map { opts =>
            clubJob(schedule) { (club, jobRunIdOption) =>
              HistoryApp.discover(
                clubSlug = club.slug,
                expectedClubIdOption = Some(club.clubId),
                full = opts.full.getOrElse(false),
                includeFinished = opts.includeFinished.getOrElse(false),
                refreshMinHours = HistoryOptions.effectiveRefresh(opts),
                trigger = RunTrigger.Scheduled,
                jobRunIdOption = jobRunIdOption
              )
            }
          }
        case JobKind.Stats =>
          for {
            opts         <- ScheduleParams.decode(schedule.params, StatsOptions.Default)
            periodOption <- ScheduleParams.statsPeriod(opts)
          } yield clubJob(schedule) { (club, _) =>
            periodOption match {
              case Some((since, until)) =>
                StatsApp.playerOfPeriodAndReport(club.clubId, since, until, opts.minGames.getOrElse(1))
              case None => StatsApp.memberStatsAndReport(club.clubId)
            }
          }
        case JobKind.ClubData =>
          ScheduleParams.decode(schedule.params, ClubDataOptions.Default).map { opts => _ =>
            ClubDataApp.refresh(opts.minAgeHours)
          }
      }

    // The schedule's club is resolved when the job starts, not when it is decoded, and by id — so the job runs
    // against the club that was scheduled whatever it is called now (ADR 0016).
    private def clubJob(schedule: JobSchedule)(effect: ClubJobEffect): JobEffect =
      jobRunIdOption =>
        for {
          clubId     <- ZIO.fromOption(schedule.clubIdOption)
                          .orElseFail(IllegalStateException(s"${schedule.kind} schedule missing clubId"))
          resolution <- ClubResolution.resolve(ClubQuery.ById(clubId))
          club       <- ZIO.fromEither(resolution.runnable).mapError(IllegalStateException(_))
          result     <- effect(club, jobRunIdOption)
        } yield result
  }
}
