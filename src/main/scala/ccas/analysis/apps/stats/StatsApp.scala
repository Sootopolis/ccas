package ccas.analysis.apps.stats

import java.time.{Duration, Instant}

import zio.{Chunk, Clock, IO, RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.{ClubQuery, ClubResolution, OutputFile, ReportRoot}
import ccas.analysis.apps.stats.StatsUtils.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.{ProgressDisplay, TimeParser}
import ccas.utils.config.Settings
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient

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
  * `POST /api/jobs/stats`, whose options are `JobOptions.StatsOptions`.
  */
object StatsApp extends ZIOAppDefault {
  private val help =
    "Usage: StatsApp <club-slug> [--since <date-or-instant>] [--until <date-or-instant>] [--min-games N]"

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

  private[stats] def parseArgs(args: Chunk[String]): Task[StatsAppArgs] = {
    val positional = args.filterNot(_.startsWith("--"))
    for {
      slug <- ZIO.fromOption(positional.headOption.map(ClubSlug.wrap)).orElseFail(BadRequestException(help))
      since <- ZIO.foreach(flagValue(args, "--since")) { s =>
        TimeParser.parseInstantZIO(s).mapError(BadRequestException(_))
      }
      until <- ZIO.foreach(flagValue(args, "--until")) { s =>
        TimeParser.parseInstantZIO(s).mapError(BadRequestException(_))
      }
      minGames <- ZIO.foreach(flagValue(args, "--min-games")) { s =>
        ZIO.fromOption(s.toIntOption)
          .orElseFail(BadRequestException(s"--min-games requires an integer (got '$s')"))
      }
    } yield StatsAppArgs(slug, since, until, minGames)
  }

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    ProgressDisplay.installed(showProgress = false) {
      (for {
        args   <- ZIOAppArgs.getArgs
        parsed <- parseArgs(args)
        clubId <- resolveClub(parsed.slug)
        _ <- (parsed.since, parsed.until) match {
          case (Some(s), Some(u)) =>
            val mg = parsed.minGames.getOrElse(1)
            for {
              result <- playerOfPeriodAndReport(clubId, s, u, mg)
              content = StatsReport.formatPlayerOfPeriod(result.contributions, mg)
              _ <- OutputFile.writeAndLog(appName = "stats", clubSlug = parsed.slug, content = content, ext = "csv")
            } yield ()
          case (None, None) =>
            for {
              result <- memberStatsAndReport(clubId)
              content = StatsReport.formatContribution(result.contributions)
              _ <- OutputFile.writeAndLog(appName = "stats", clubSlug = parsed.slug, content = content, ext = "csv")
            } yield ()
          case _ => ZIO.fail(BadRequestException("Both --since and --until are required for period stats"))
        }
      } yield ()).provideSomeAuto(
        PostgresClient.live(onInit = Tables.ensureTablesOnInit),
        Settings.live,
        ReportRoot.live
      )
    }

  /** Resolves a slug the way job submission does, so a club addressed by a former name still resolves. */
  private[stats] def resolveClub(clubSlug: ClubSlug): RIO[PostgresClient, ClubId] =
    ClubResolution.resolve(ClubQuery.BySlug(clubSlug)).flatMap { resolution =>
      ZIO.fromEither(resolution.runnable).mapBoth(NotFoundException(_), _.clubId)
    }

  /** All-time member contribution summary for a club. */
  def memberStats(clubId: ClubId): RIO[PostgresClient, StatsResult] =
    for {
      (rows, matchCount) <- ClubBoard.selectClubBoards(clubId) <&> ClubMatch.countForClub(clubId)
      playerIds = rows.map(_.playerId).distinct
      displayNames <- Player.selectDisplayNames(playerIds)
      contributions = aggregate(rows, displayNames)
    } yield StatsResult(contributions, rows.size, matchCount)

  /** Refuses a period that does not run forwards, or that ends within 12 hours of now, before Chess.com's data has
    * settled. Asked at submit and again at run time: a scheduled period that passes once only grows older.
    */
  def checkPeriod(since: Instant, until: Instant): IO[BadRequestException, Unit] =
    Clock.instant.flatMap { now =>
      val latestUntil = now.minus(Duration.ofHours(12))
      if (!since.isBefore(until)) {
        ZIO.fail(BadRequestException(s"'since' must be before 'until' (got $since .. $until)"))
      } else if (until.isAfter(latestUntil)) {
        ZIO.fail(
          BadRequestException(
            s"'until' must be at least 12 hours before now to account for API data lag (latest allowed: $latestUntil)"
          )
        )
      } else { ZIO.unit }
    }

  /** Per-member stats for a date range. */
  def playerOfPeriod(
    clubId: ClubId,
    since: Instant,
    until: Instant
  ): RIO[PostgresClient, StatsResult] =
    for {
      _ <- checkPeriod(since, until)
      (rows, matchCount) <- ClubBoard.selectClubBoardsInPeriod(clubId, since, until) <&> ClubMatch.countForClubInPeriod(
        clubId = clubId,
        since = since,
        until = until
      )
      playerIds = rows.map(_.playerId).distinct
      displayNames <- Player.selectDisplayNames(playerIds)
      contributions = aggregate(rows, displayNames)
    } yield StatsResult(contributions, rows.size, matchCount)

  // The core stats fns are pure DB reads with no logging. These wrappers append the one-line summary so HTTP- and
  // scheduler-submitted stats jobs surface it in their per-job log (the CLI additionally writes the report CSV) — the
  // shared-reporting split membership #130 established. Period eligibility is a presentation threshold (minGames), so
  // it stays a wrapper arg rather than polluting playerOfPeriod's signature.
  def memberStatsAndReport(clubId: ClubId): RIO[PostgresClient, StatsResult] =
    memberStats(clubId).tap { r =>
      ZIO.logInfo(s"Players: ${r.contributions.size}, Boards: ${r.boardCount}, Matches: ${r.matchCount}")
    }

  def playerOfPeriodAndReport(
    clubId: ClubId,
    since: Instant,
    until: Instant,
    minGames: Int
  ): RIO[PostgresClient, StatsResult] =
    playerOfPeriod(clubId, since, until).tap { r =>
      val eligible = r.contributions.count(_.raw.games >= minGames)
      ZIO.logInfo(s"Players: ${r.contributions.size}, Eligible (>=$minGames games): $eligible")
    }

  private def flagValue(args: Chunk[String], flag: String): Option[String] = {
    val idx = args.indexOf(flag)
    if (idx >= 0 && idx + 1 < args.size) Some(args(idx + 1))
    else None
  }
}
