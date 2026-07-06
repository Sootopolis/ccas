package ccas.server.scheduler

import java.time.Instant

import zio.{Task, ZIO}
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.TimeParser

/** Typed per-`JobKind` options decoded from a schedule's free-text `params` column so scheduled runs can
  * carry the same tunable knobs an ad-hoc HTTP submission (`JobRoutes`) can. Each DTO mirrors the
  * corresponding `*Request` type minus the club *target* (a schedule's club comes from `schedule.clubId`,
  * not the params JSON). Every field is optional; an absent (or blank) `params` decodes to the kind's
  * all-`None` `Default`, which threads into the app call as today's hardcoded defaults — so existing
  * param-less schedules are unchanged.
  */
object ScheduleParams {

  final case class ClubDataOptions(minAgeHours: Option[Int])
  object ClubDataOptions {
    given JsonCodec[ClubDataOptions] = DeriveJsonCodec.gen
    val Default: ClubDataOptions     = ClubDataOptions(None)
  }

  final case class RecruitmentOptions(
    alias: Option[String],
    target: Option[Int],
    cumulative: Option[Boolean],
    sourceClubs: Option[List[ClubSlug]],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean]
  )
  object RecruitmentOptions {
    given JsonCodec[RecruitmentOptions] = DeriveJsonCodec.gen
    val Default: RecruitmentOptions     = RecruitmentOptions(None, None, None, None, None, None)
  }

  final case class MembershipOptions(trustUsernames: Option[Boolean])
  object MembershipOptions {
    given JsonCodec[MembershipOptions] = DeriveJsonCodec.gen
    val Default: MembershipOptions     = MembershipOptions(None)
  }

  final case class HistoryOptions(
    full: Option[Boolean],
    includeFinished: Option[Boolean],
    refresh: Option[Boolean],
    refreshMinHours: Option[Int]
  )
  object HistoryOptions {
    given JsonCodec[HistoryOptions] = DeriveJsonCodec.gen
    val Default: HistoryOptions     = HistoryOptions(None, None, None, None)

    /** Coalesce the `refresh` flag and `refreshMinHours` into `HistoryApp.discover`'s `refreshMinHours`
      * argument, mirroring `JobRoutes`: an explicit hours value wins, else a bare `refresh: true` means
      * "always refresh" (0 hours), else `None`.
      */
    def effectiveRefresh(opts: HistoryOptions): Option[Int] =
      opts.refreshMinHours.orElse(opts.refresh.filter(identity).map(_ => 0))
  }

  final case class StatsOptions(since: Option[String], until: Option[String], minGames: Option[Int])
  object StatsOptions {
    given JsonCodec[StatsOptions] = DeriveJsonCodec.gen
    val Default: StatsOptions     = StatsOptions(None, None, None)
  }

  final case class MatchRefOptions(forceSkipped: Option[Boolean], upgradeRefs: Option[Boolean])
  object MatchRefOptions {
    given JsonCodec[MatchRefOptions] = DeriveJsonCodec.gen
    val Default: MatchRefOptions     = MatchRefOptions(None, None)
  }

  /** Decode `params` into `A`, falling back to `default` when absent or blank. A malformed JSON body fails
    * with a descriptive error, which the scheduler's per-schedule guard isolates (the bad row logs and the
    * poll loop continues) — and because decode runs before `submit`, `last_run_at` is not advanced.
    */
  def decode[A](params: Option[String], default: A)(using codec: JsonCodec[A]): Task[A] =
    params match {
      case None                          => ZIO.succeed(default)
      case Some(s) if s.trim.isEmpty     => ZIO.succeed(default)
      case Some(s) =>
        ZIO
          .fromEither(codec.decoder.decodeJson(s))
          .mapError(err => new IllegalArgumentException(s"invalid params JSON: $err"))
    }

  /** Resolve the optional stats period, enforcing the same both-or-neither rule as `JobRoutes`. `None` →
    * all-time member stats; `Some((since, until))` → player-of-period; only one supplied → error.
    */
  def statsPeriod(opts: StatsOptions): Task[Option[(Instant, Instant)]] =
    (opts.since, opts.until) match {
      case (Some(sinceStr), Some(untilStr)) =>
        for {
          since <- TimeParser.parseInstantZIO(sinceStr).mapError(e => new IllegalArgumentException(s"invalid 'since': $e"))
          until <- TimeParser.parseInstantZIO(untilStr).mapError(e => new IllegalArgumentException(s"invalid 'until': $e"))
        } yield Some((since, until))
      case (None, None) => ZIO.none
      case _            => ZIO.fail(new IllegalArgumentException("both 'since' and 'until' are required for period stats"))
    }
}
