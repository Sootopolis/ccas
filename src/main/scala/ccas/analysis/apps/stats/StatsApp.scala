package ccas.analysis.apps.stats

import ccas.analysis.apps.stats.StatsUtils.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.{OutputFile, ProgressDisplay, TimeParser}
import zio.{RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.time.{Duration, Instant}

/** Generates club member contribution stats from stored match history.
  *
  * ==Modes==
  *   - '''Member stats (default):''' All-time per-member contribution table with raw and fairplay-adjusted views.
  *   - '''Player of period:''' Same stats filtered by a date range, ranked by raw points.
  *
  * ==CLI==
  * {{{
  * StatsApp <club-slug>
  * StatsApp <club-slug> --since <date-or-instant> --until <date-or-instant> [--min-games N]
  * }}}
  *
  * ==API==
  * `POST /api/jobs/stats` with `{"clubSlug": "...", "since": "...", "until": "..."}`
  */
object StatsApp extends ZIOAppDefault {
  private val help = "Usage: StatsApp <club-slug> [--since <date-or-instant>] [--until <date-or-instant>] [--min-games N]"

  final case class StatsResult(
    contributions: List[MemberContribution],
    boardCount: Int,
    matchCount: Long
  )

  private[stats] final case class StatsAppArgs(
    slug: ClubSlug,
    since: Option[Instant],
    until: Option[Instant],
    minGames: Option[Int]
  )

  private[stats] def parseArgs(args: zio.Chunk[String]): Task[StatsAppArgs] = {
    val positional = args.filterNot(_.startsWith("--"))
    for {
      slug <- ZIO.fromOption(positional.headOption.map(ClubSlug.wrap)).orElseFail(BadRequestException(help))
      since <- ZIO.foreach(flagValue(args, "--since"))(s => TimeParser.parseInstantZIO(s).mapError(BadRequestException(_)))
      until <- ZIO.foreach(flagValue(args, "--until"))(s => TimeParser.parseInstantZIO(s).mapError(BadRequestException(_)))
      minGames <- ZIO.foreach(flagValue(args, "--min-games")) { s =>
        ZIO.fromOption(s.toIntOption)
          .orElseFail(BadRequestException(s"--min-games requires an integer (got '$s')"))
      }
    } yield StatsAppArgs(slug, since, until, minGames)
  }

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    for {
      _ <- ProgressDisplay.live(showProgress = false).build
      _ <- (for {
        args   <- ZIOAppArgs.getArgs
        parsed <- parseArgs(args)
        _ <- (parsed.since, parsed.until) match {
          case (Some(s), Some(u)) =>
            val mg = parsed.minGames.getOrElse(1)
            for {
              result <- playerOfPeriodAndReport(parsed.slug, s, u, mg)
              content = StatsReport.formatPlayerOfPeriod(result.contributions, mg)
              _ <- OutputFile.writeAndLog("stats", parsed.slug, content, ext = "csv")
            } yield ()
          case (None, None) =>
            for {
              result <- memberStatsAndReport(parsed.slug)
              content = StatsReport.formatContribution(result.contributions)
              _ <- OutputFile.writeAndLog("stats", parsed.slug, content, ext = "csv")
            } yield ()
          case _ => ZIO.fail(BadRequestException("Both --since and --until are required for period stats"))
        }
      } yield ()).provideSomeAuto(PostgresClient.live(onInit = Tables.ensureTables))
    } yield ()

  /** All-time member contribution summary for a club. */
  def memberStats(clubSlug: ClubSlug): RIO[PostgresClient, StatsResult] =
    for {
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club '$clubSlug' not found"))
      clubId = club.clubId
      (rows, matchCount) <- ClubBoard.selectClubBoards(clubId) <&> ClubMatch.countForClub(clubId)
      playerIds = rows.map(_.playerId).distinct
      usernameMap <- Player.resolveUsernames(playerIds)
      contributions = aggregate(rows, usernameMap)
    } yield StatsResult(contributions, rows.size, matchCount)

  /** Per-member stats for a date range. */
  def playerOfPeriod(
    clubSlug: ClubSlug,
    since: Instant,
    until: Instant
  ): RIO[PostgresClient, StatsResult] =
    for {
      now = Instant.now()
      _ <- ZIO.whenDiscard(!since.isBefore(until))(
        ZIO.fail(BadRequestException(s"--since must be before --until (got $since .. $until)"))
      )
      _ <- ZIO.whenDiscard(until.isAfter(now.minus(Duration.ofHours(12))))(
        ZIO.fail(BadRequestException(s"--until must be at least 12 hours before now to account for API data lag (earliest allowed: ${now.minus(Duration.ofHours(12))})"))
      )
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club '$clubSlug' not found"))
      clubId = club.clubId
      (rows, matchCount) <- ClubBoard.selectClubBoardsInPeriod(clubId, since, until) <&> ClubMatch.countForClubInPeriod(clubId, since, until)
      playerIds = rows.map(_.playerId).distinct
      usernameMap <- Player.resolveUsernames(playerIds)
      contributions = aggregate(rows, usernameMap)
    } yield StatsResult(contributions, rows.size, matchCount)

  // The core stats fns are pure DB reads with no logging. These wrappers append the one-line summary so HTTP- and
  // scheduler-submitted stats jobs surface it in their per-job log (the CLI additionally writes the out/ CSV) — the
  // shared-reporting split membership #130 established. Period eligibility is a presentation threshold (minGames), so
  // it stays a wrapper arg rather than polluting playerOfPeriod's signature.
  def memberStatsAndReport(clubSlug: ClubSlug): RIO[PostgresClient, StatsResult] =
    memberStats(clubSlug).tap(r =>
      ZIO.logInfo(s"Players: ${r.contributions.size}, Boards: ${r.boardCount}, Matches: ${r.matchCount}")
    )

  def playerOfPeriodAndReport(
    clubSlug: ClubSlug,
    since: Instant,
    until: Instant,
    minGames: Int
  ): RIO[PostgresClient, StatsResult] =
    playerOfPeriod(clubSlug, since, until).tap { r =>
      val eligible = r.contributions.count(_.raw.games >= minGames)
      ZIO.logInfo(s"Players: ${r.contributions.size}, Eligible (>=$minGames games): $eligible")
    }

  private def flagValue(args: zio.Chunk[String], flag: String): Option[String] = {
    val idx = args.indexOf(flag)
    if (idx >= 0 && idx + 1 < args.size) Some(args(idx + 1))
    else None
  }
}
