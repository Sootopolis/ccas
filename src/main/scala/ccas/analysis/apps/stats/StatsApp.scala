package ccas.analysis.apps.stats

import ccas.analysis.apps.stats.StatsUtils.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.{CcasLogger, OutputFile}
import zio.{RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.time.Instant

/** Generates club member contribution stats from stored match history.
  *
  * ==Modes==
  *   - '''Member stats (default):''' All-time per-member contribution table with raw and fairplay-adjusted views.
  *   - '''Player of period:''' Same stats filtered by a date range, ranked by raw points.
  *
  * ==CLI==
  * {{{
  * StatsApp <club-slug>
  * StatsApp <club-slug> --since <iso-instant> --until <iso-instant> [--min-games N]
  * }}}
  *
  * ==API==
  * `POST /api/jobs/stats` with `{"clubSlug": "...", "since": "...", "until": "...", "minGames": N}`
  */
object StatsApp extends ZIOAppDefault {
  private val help = "Usage: StatsApp <club-slug> [--since <iso-instant>] [--until <iso-instant>] [--min-games N]"

  case class StatsResult(
    contributions: List[MemberContribution],
    boardCount: Int,
    matchCount: Long
  )

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      positional = args.filterNot(_.startsWith("--"))
      slug <- positional.headOption match {
        case Some(s) => ZIO.succeed(ClubSlug.wrap(s))
        case None    => ZIO.fail(BadRequestException(help))
      }
      since <- ZIO.foreach(flagValue(args, "--since"))(s => ZIO.attempt(Instant.parse(s)))
      until <- ZIO.foreach(flagValue(args, "--until"))(s => ZIO.attempt(Instant.parse(s)))
      minGames <- ZIO.foreach(flagValue(args, "--min-games"))(s => ZIO.attempt(s.toInt))
      _ <- (since, until) match {
        case (Some(s), Some(u)) =>
          for {
            result  <- playerOfPeriod(slug, s, u)
            mg       = minGames.getOrElse(4)
            content  = StatsReport.formatPlayerOfPeriod(result.contributions, mg)
            eligible = result.contributions.count(_.raw.games >= mg)
            _ <- CcasLogger.info(s"Players: ${result.contributions.size}, Eligible (>=$mg games): $eligible")
            _ <- OutputFile.writeAndLog("stats", slug, content, ext = "csv")
          } yield ()
        case (None, None) =>
          for {
            result  <- memberStats(slug)
            content  = StatsReport.formatContribution(result.contributions)
            _ <- CcasLogger.info(s"Players: ${result.contributions.size}, Boards: ${result.boardCount}, Matches: ${result.matchCount}")
            _ <- OutputFile.writeAndLog("stats", slug, content, ext = "csv")
          } yield ()
        case _ => ZIO.fail(BadRequestException("Both --since and --until are required for period stats"))
      }
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = false),
      PostgresClient.live(onInit = Tables.ensureTables)
    )

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
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club '$clubSlug' not found"))
      clubId = club.clubId
      rows <- ClubBoard.selectClubBoardsInPeriod(clubId, since, until)
      playerIds = rows.map(_.playerId).distinct
      usernameMap <- Player.resolveUsernames(playerIds)
      contributions = aggregate(rows, usernameMap)
    } yield StatsResult(contributions, rows.size, 0L)

  private def flagValue(args: zio.Chunk[String], flag: String): Option[String] = {
    val idx = args.indexOf(flag)
    if (idx >= 0 && idx + 1 < args.size) Some(args(idx + 1))
    else None
  }
}
