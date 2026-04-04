package ccas.analysis.apps.stats

import ccas.analysis.apps.stats.StatsUtils.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubSlug, JobRunId}
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
        case (Some(s), Some(u)) => playerOfPeriod(slug, s, u, minGames.getOrElse(4))
        case (None, None)       => memberStats(slug)
        case _                  => ZIO.fail(BadRequestException("Both --since and --until are required for period stats"))
      }
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = false),
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  /** All-time member contribution summary for a club. */
  // trigger/jobRunId accepted for consistency with other app entry points but not persisted (no run table)
  @annotation.nowarn("msg=unused explicit parameter")
  def memberStats(
    clubSlug: ClubSlug,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunId: Option[JobRunId] = None
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      _    <- CcasLogger.info(s"=== StatsApp: $clubSlug ===")
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club '$clubSlug' not found"))
      clubId = club.clubId
      (rows, matchCount) <- ClubBoard.selectClubBoards(clubId) <&> ClubMatch.countForClub(clubId)
      playerIds = rows.map(_.playerId).distinct
      usernameMap <- Player.resolveUsernames(playerIds)
      contributions = aggregate(rows, usernameMap)
      content = StatsReport.formatContribution(contributions)
      _ <- CcasLogger.info(s"Players: ${contributions.size}, Boards: ${rows.size}, Matches: $matchCount")
      _ <- OutputFile.writeAndLog("stats", clubSlug, content, ext = "csv")
    } yield ()

  /** Per-member stats for a date range, ranked by raw points. */
  @annotation.nowarn("msg=unused explicit parameter")
  def playerOfPeriod(
    clubSlug: ClubSlug,
    since: Instant,
    until: Instant,
    minGames: Int,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunId: Option[JobRunId] = None
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      _    <- CcasLogger.info(s"=== StatsApp (period): $clubSlug ===")
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club '$clubSlug' not found"))
      clubId = club.clubId
      rows <- ClubBoard.selectClubBoardsInPeriod(clubId, since, until)
      playerIds = rows.map(_.playerId).distinct
      usernameMap <- Player.resolveUsernames(playerIds)
      contributions = aggregate(rows, usernameMap)
      content = StatsReport.formatPlayerOfPeriod(contributions, minGames)
      eligible = contributions.count(_.raw.games >= minGames)
      _ <- CcasLogger.info(s"Players: ${contributions.size}, Eligible (>=$minGames games): $eligible")
      _ <- OutputFile.writeAndLog("stats", clubSlug, content, ext = "csv")
    } yield ()

  private def flagValue(args: zio.Chunk[String], flag: String): Option[String] = {
    val idx = args.indexOf(flag)
    if (idx >= 0 && idx + 1 < args.size) Some(args(idx + 1))
    else None
  }
}
