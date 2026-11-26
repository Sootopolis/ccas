package ccas.server.scheduler

import zio.{Task, ZIO}
import zio.json.JsonCodec

import ccas.analysis.tables.RunTrigger
import ccas.server.jobs.{ClubJobEffect, JobEffect, JobKind}
import ccas.server.jobs.JobOptions.{
  ClubDataOptions,
  HistoryOptions,
  MatchRefOptions,
  MembershipOptions,
  RecruitmentOptions,
  StatsOptions
}

/** Reads a schedule's free-text `params` column as its kind's options (`JobOptions`), and builds the job they
  * describe.
  */
object ScheduleParams {

  enum ScheduledJob {
    case ForClub(effect: ClubJobEffect)
    case Clubless(effect: JobEffect)
  }

  /** The job a schedule of `kind` runs with these `params`. It fails when the scheduler could never build a run from
    * them, so the API refuses a write that fails it (#547), and the scheduler builds each run from it.
    */
  def job(kind: JobKind, params: Option[String]): Task[ScheduledJob] =
    kind match {
      case JobKind.Recruitment =>
        for {
          opts   <- decode(params, RecruitmentOptions.Default)
          effect <- opts.effect(RunTrigger.Scheduled, autoConfirm = true, confirmedCriteriaIdOption = None)
        } yield ScheduledJob.ForClub(effect)
      case JobKind.Membership =>
        decode(params, MembershipOptions.Default).map(opts => ScheduledJob.ForClub(opts.effect(RunTrigger.Scheduled)))
      case JobKind.MatchRef =>
        decode(params, MatchRefOptions.Default).map(opts => ScheduledJob.Clubless(opts.effect))
      case JobKind.History =>
        for {
          opts   <- decode(params, HistoryOptions.Default)
          effect <- opts.effect(RunTrigger.Scheduled)
        } yield ScheduledJob.ForClub(effect)
      case JobKind.Stats =>
        for {
          opts   <- decode(params, StatsOptions.Default)
          effect <- opts.effect
        } yield ScheduledJob.ForClub(effect)
      case JobKind.ClubData =>
        for {
          opts   <- decode(params, ClubDataOptions.Default)
          effect <- opts.effect
        } yield ScheduledJob.Clubless(effect)
    }

  /** Decode `params` into `A`, falling back to `default` when absent or blank. */
  private[scheduler] def decode[A](params: Option[String], default: A)(using codec: JsonCodec[A]): Task[A] =
    params match {
      case None                      => ZIO.succeed(default)
      case Some(s) if s.trim.isEmpty => ZIO.succeed(default)
      case Some(s) => ZIO.fromEither(codec.decoder.decodeJson(s)).mapError(IllegalArgumentException(_))
    }
}
