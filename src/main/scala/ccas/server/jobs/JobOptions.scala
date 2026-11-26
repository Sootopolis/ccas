package ccas.server.jobs

import java.time.Instant

import zio.{IO, ZIO}
import zio.json.{jsonNoExtraFields, DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.clubdata.ClubDataApp
import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.{RecruitmentAlias, RunTrigger}
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.TimeParser
import ccas.utils.errors.BadRequestException
import ccas.utils.errors.BadRequestException.requireAtLeast

/** Each job kind's tunable options, and the one place they become the job's effect. An HTTP submission carries them
  * under `options` (`JobRoutes`) and a schedule in its `params` column (`JobScheduler`), as the same JSON. Every field
  * is optional, so `{}` and each kind's `Default` mean the app's usual run; an unknown key is refused, not dropped.
  */
object JobOptions {

  @jsonNoExtraFields
  final case class RecruitmentOptions(
    alias: Option[String],
    target: Option[Int],
    cumulative: Option[Boolean],
    sourceClubs: Option[List[ClubSlug]],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean]
  ) {

    def cappedTarget: Option[Int] = target.map(_ min JobCaps.MaxTarget)

    /** An absent limit is the cap, so no recruitment job runs unbounded (#265). */
    def cappedTimeLimitMinutes: Int =
      timeLimitMinutes.fold(JobCaps.MaxTimeLimitMinutes)(_ min JobCaps.MaxTimeLimitMinutes)

    /** `autoConfirm` and `confirmedCriteriaIdOption` are the interactive CLI's, so they are not options a schedule
      * could set: a scheduled run auto-confirms, with no criteria read at a terminal. A target or time limit below 1 is
      * refused here, before a job is submitted: either would end the run at once as if it had finished (#548).
      */
    def effect(
      trigger: RunTrigger,
      autoConfirm: Boolean,
      confirmedCriteriaIdOption: Option[Long]
    ): IO[BadRequestException, ClubJobEffect] =
      for {
        _ <- requireAtLeast("target", 1, target)
        _ <- requireAtLeast("timeLimitMinutes", 1, timeLimitMinutes)
      } yield (club, jobRunIdOption) =>
        RecruitmentApp.recruit(
          clubSlug = club.slug,
          expectedClubIdOption = Some(club.clubId),
          alias = alias.getOrElse(RecruitmentAlias.Default),
          target = cappedTarget,
          cumulative = cumulative.getOrElse(false),
          sourceClubs = sourceClubs.getOrElse(Nil),
          timeLimitMinutes = Some(cappedTimeLimitMinutes),
          explore = explore.getOrElse(true),
          trigger = trigger,
          autoConfirm = autoConfirm,
          jobRunIdOption = jobRunIdOption,
          confirmedCriteriaIdOption = confirmedCriteriaIdOption
        )
  }
  object RecruitmentOptions {
    given JsonCodec[RecruitmentOptions] = DeriveJsonCodec.gen
    val Default: RecruitmentOptions     = RecruitmentOptions(None, None, None, None, None, None)
  }

  @jsonNoExtraFields
  final case class MembershipOptions(trustUsernames: Option[Boolean]) {

    def effect(trigger: RunTrigger): ClubJobEffect =
      (club, jobRunIdOption) =>
        MembershipApp.reconcileAndReport(
          clubSlug = club.slug,
          expectedClubIdOption = Some(club.clubId),
          trustUsernames = trustUsernames.getOrElse(true),
          trigger = trigger,
          jobRunIdOption = jobRunIdOption
        )
  }
  object MembershipOptions {
    given JsonCodec[MembershipOptions] = DeriveJsonCodec.gen
    val Default: MembershipOptions     = MembershipOptions(None)
  }

  @jsonNoExtraFields
  final case class HistoryOptions(
    full: Option[Boolean],
    includeFinished: Option[Boolean],
    refresh: Option[Boolean],
    refreshMinHours: Option[Int]
  ) {

    /** `HistoryApp.discover`'s `refreshMinHours`: an explicit hours value wins, else a bare `refresh: true` means
      * "always refresh" (0 hours), else `None`.
      */
    def effectiveRefresh: Option[Int] = refreshMinHours.orElse(refresh.filter(identity).map(_ => 0))

    /** A negative `refreshMinHours` is refused: it would refresh exactly as 0 does (#555). */
    def effect(trigger: RunTrigger): IO[BadRequestException, ClubJobEffect] =
      requireAtLeast("refreshMinHours", 0, refreshMinHours).as { (club, jobRunIdOption) =>
        HistoryApp.discover(
          clubSlug = club.slug,
          expectedClubIdOption = Some(club.clubId),
          full = full.getOrElse(false),
          includeFinished = includeFinished.getOrElse(false),
          refreshMinHours = effectiveRefresh,
          trigger = trigger,
          jobRunIdOption = jobRunIdOption
        )
      }
  }
  object HistoryOptions {
    given JsonCodec[HistoryOptions] = DeriveJsonCodec.gen
    val Default: HistoryOptions     = HistoryOptions(None, None, None, None)
  }

  @jsonNoExtraFields
  final case class StatsOptions(since: Option[String], until: Option[String], minGames: Option[Int]) {

    /** `None` is all-time member stats, `Some` a player-of-period; one of `since` and `until` without the other, or a
      * period `StatsApp.checkPeriod` refuses at the current time, is refused.
      */
    def period: IO[BadRequestException, Option[(Instant, Instant)]] =
      (since, until) match {
        case (Some(sinceString), Some(untilString)) =>
          for {
            start <- TimeParser.parseInstantZIO(sinceString).mapError(e => BadRequestException(s"Invalid 'since': $e"))
            end   <- TimeParser.parseInstantZIO(untilString).mapError(e => BadRequestException(s"Invalid 'until': $e"))
            _     <- StatsApp.checkPeriod(start, end)
          } yield Some((start, end))
        case (None, None) => ZIO.none
        case _            => ZIO.fail(BadRequestException("Both 'since' and 'until' are required for period stats"))
      }

    /** Checks the period and `minGames` now, so a bad one is refused before a job is submitted. A negative `minGames`
      * would list exactly what 0 does: everyone with a board in the period, those with no counted game included (#555).
      */
    def effect: IO[BadRequestException, ClubJobEffect] =
      for {
        _            <- requireAtLeast("minGames", 0, minGames)
        periodOption <- period
      } yield (club, _) =>
        periodOption match {
          case Some((start, end)) => StatsApp.playerOfPeriodAndReport(club.clubId, start, end, minGames.getOrElse(1))
          case None               => StatsApp.memberStatsAndReport(club.clubId)
        }
  }
  object StatsOptions {
    given JsonCodec[StatsOptions] = DeriveJsonCodec.gen
    val Default: StatsOptions     = StatsOptions(None, None, None)
  }

  @jsonNoExtraFields
  final case class MatchRefOptions(forceSkipped: Option[Boolean], upgradeRefs: Option[Boolean]) {

    def effect: JobEffect =
      _ => RefApp.populate(forceSkipped.getOrElse(false), upgradeRefs.getOrElse(false))
  }
  object MatchRefOptions {
    given JsonCodec[MatchRefOptions] = DeriveJsonCodec.gen
    val Default: MatchRefOptions     = MatchRefOptions(None, None)
  }

  @jsonNoExtraFields
  final case class ClubDataOptions(minAgeHours: Option[Int]) {

    /** A negative `minAgeHours` is refused: like 0, it would skip no club (#555). */
    def effect: IO[BadRequestException, JobEffect] =
      requireAtLeast("minAgeHours", 0, minAgeHours).as(_ => ClubDataApp.refresh(minAgeHours))
  }
  object ClubDataOptions {
    given JsonCodec[ClubDataOptions] = DeriveJsonCodec.gen
    val Default: ClubDataOptions     = ClubDataOptions(None)
  }
}
