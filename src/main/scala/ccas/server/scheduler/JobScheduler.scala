package ccas.server.scheduler

import java.time.Instant

import com.typesafe.config.ConfigFactory
import zio.{
  durationInt,
  durationLong,
  Clock,
  Duration,
  Ref,
  RIO,
  Schedule,
  Scope,
  Task,
  UIO,
  URIO,
  URLayer,
  ZEnvironment,
  ZIO,
  ZLayer
}

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
import ccas.utils.errors.{safeMessage, ConflictException}
import ccas.utils.sql.PostgresClient

trait JobScheduler {
  def start: URIO[Scope, Unit]

  /** Read the schedules again at the next tick: call after writing `job_schedule`, since between reads the scheduler
    * goes by what it read last (ADR 0028).
    */
  def recheck: UIO[Unit]
}

object JobScheduler {

  /** The longest the scheduler goes without reading the schedules, so a write it was not told of is seen in time. */
  private val MaxReadGap: Duration = 6.hours

  val live: URLayer[JobRunner & PostgresClient, JobScheduler] =
    ZLayer {
      val config = ConfigFactory.load()
      val pollMinutes =
        if config.hasPath("scheduler.pollIntervalMinutes")
        then config.getInt("scheduler.pollIntervalMinutes")
        else 15 // default poll interval in minutes
      for {
        runner   <- ZIO.service[JobRunner]
        pgClient <- ZIO.service[PostgresClient]
        scheduler <- JobSchedulerLive.make(
          runner,
          pgClient,
          pollInterval = pollMinutes.toLong.minutes,
          maxReadGap = MaxReadGap,
          readSchedules = JobSchedule.selectEnabled
        )
      } yield scheduler
    }

  val recheck: URIO[JobScheduler, Unit] = ZIO.serviceWithZIO[JobScheduler](_.recheck)

  private[scheduler] object JobSchedulerLive {
    def make(
      runner: JobRunner,
      pgClient: PostgresClient,
      pollInterval: Duration,
      maxReadGap: Duration,
      readSchedules: RIO[PostgresClient, List[JobSchedule]]
    ): UIO[JobSchedulerLive] =
      for {
        nextRead     <- Ref.make(Option.empty[Instant])
        recheckAsked <- Ref.make(false)
      } yield new JobSchedulerLive(runner, pgClient, pollInterval, maxReadGap, readSchedules, nextRead, recheckAsked)
  }

  /** Ticks every `pollInterval` but reads the enabled schedules only once `nextRead` has passed or a recheck was asked
    * for; `nextRead` is empty until the first read and after a failed one.
    */
  private[scheduler] class JobSchedulerLive(
    runner: JobRunner,
    pgClient: PostgresClient,
    pollInterval: Duration,
    maxReadGap: Duration,
    readSchedules: RIO[PostgresClient, List[JobSchedule]],
    private[scheduler] val nextRead: Ref[Option[Instant]],
    recheckAsked: Ref[Boolean]
  ) extends JobScheduler {

    private val pgClientEnv = ZEnvironment(pgClient)

    // Skip lateness ceiling for cron `Skip` schedules: a boundary may land just after a poll (observed up to one
    // pollInterval late) plus headroom for poll execution / DB latency. CatchUp ignores this; Interval is unaffected.
    private val grace: Duration = pollInterval.multipliedBy(2)

    override def start: URIO[Scope, Unit] =
      tick
        .repeat(Schedule.fixed(pollInterval))
        .forkScoped
        .unit

    override def recheck: UIO[Unit] = recheckAsked.set(true)

    /** The flag is taken before the read, so a recheck asked for while one runs is kept for the next tick. */
    private def tick: UIO[Unit] =
      for {
        now            <- Clock.instant
        asked          <- recheckAsked.getAndSet(false)
        nextReadOption <- nextRead.get
        _              <- ZIO.whenDiscard(asked || nextReadOption.forall(at => !now.isBefore(at)))(readAndFire)
      } yield ()

    /** `now` is taken after the read, which may have waited for a suspended compute to start. */
    private def readAndFire: UIO[Unit] =
      (for {
        schedules <- readSchedules.provideEnvironment(pgClientEnv)
        now       <- Clock.instant
        fired     <- ZIO.foreach(schedules)(fireIfDue(_, now))
        after     <- ZIO.attempt(nextReadAfter(fired.flatten, now))
        _         <- nextRead.set(Some(after))
      } yield ())
        .catchAll { e =>
          nextRead.set(None) *>
            ProgressDisplay.sourced("scheduler")(ZIO.logError(s"Poll error: ${e.safeMessage}"))
        }

    /** A due schedule that didn't fire, as when its job is still running, holds the next read at the next tick. */
    private def nextReadAfter(schedules: List[JobSchedule], now: Instant): Instant =
      (now.plus(maxReadGap) :: schedules.flatMap(_.nextDue(now, grace))).min

    /** Submits `schedule` if it is due. Returns it as it stands afterwards, with `lastRunAt` at `now` if submitted, or
      * `None` for a row that can't fire until it is changed: one whose trigger or params won't decode. Such a row is
      * logged at every read rather than holding the next one early.
      */
    private def fireIfDue(schedule: JobSchedule, now: Instant): UIO[Option[JobSchedule]] =
      ZIO
        .attempt(schedule.isDue(now, grace))
        .foldZIO(
          e => logUndecodable(schedule, "trigger", e).as(None),
          due =>
            if (due) { fire(schedule, now) }
            else { ZIO.some(schedule) }
        )

    private def fire(schedule: JobSchedule, now: Instant): UIO[Option[JobSchedule]] =
      jobEffect(schedule).foldZIO(
        e => logUndecodable(schedule, "params", e).as(None),
        effect =>
          submit(schedule, effect, now).foldZIO(
            e => logUnsubmitted(schedule, e).as(Some(schedule)),
            _ => ZIO.some(schedule.copy(lastRunAt = Some(now)))
          )
      )

    private def logUndecodable(schedule: JobSchedule, field: String, e: Throwable): UIO[Unit] =
      ProgressDisplay.sourced("scheduler")(
        ZIO.logError(s"${schedule.kind} (club ${schedule.clubIdOption}): invalid $field: ${e.safeMessage}")
      )

    private def logUnsubmitted(schedule: JobSchedule, e: Throwable): UIO[Unit] =
      ProgressDisplay.sourced("scheduler") {
        e match {
          // A job of this kind/club is already running (e.g. a forked job outliving its own
          // intervalHours). Expected and benign: last_run_at stays put, so the next tick after
          // the job ends submits promptly. Debug-log instead of ERROR-spamming for the duration.
          case _: ConflictException =>
            ZIO.logDebug(s"${schedule.kind} (club ${schedule.clubIdOption}): already running, skipping tick")
          case _ =>
            ZIO.logError(s"${schedule.kind} (club ${schedule.clubIdOption}): ${e.safeMessage}")
        }
      }

    private def submit(schedule: JobSchedule, effect: JobEffect, now: Instant): Task[Unit] =
      (for {
        _ <- runner.submit(schedule.kind, schedule.clubIdOption, schedule.params, RunTrigger.Scheduled, effect)
        _ <- JobSchedule.updateLastRunAt(schedule.id, now)
      } yield ()).provideEnvironment(pgClientEnv)

    /** Decode `schedule.params` into typed per-kind options and thread them into the app call. Decoding is eager
      * (before `submit`), so a malformed row fails here without submitting or advancing `last_run_at`. Absent params
      * decode to the kind's all-`None` defaults.
      */
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
                refreshMinHours = opts.effectiveRefresh,
                trigger = RunTrigger.Scheduled,
                jobRunIdOption = jobRunIdOption
              )
            }
          }
        case JobKind.Stats =>
          for {
            opts         <- ScheduleParams.decode(schedule.params, StatsOptions.Default)
            periodOption <- opts.period
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
          clubId <- ZIO.fromOption(schedule.clubIdOption)
            .orElseFail(IllegalStateException(s"${schedule.kind} schedule missing clubId"))
          resolution <- ClubResolution.resolve(ClubQuery.ById(clubId))
          club       <- ZIO.fromEither(resolution.runnable).mapError(IllegalStateException(_))
          result     <- effect(club, jobRunIdOption)
        } yield result
  }
}
