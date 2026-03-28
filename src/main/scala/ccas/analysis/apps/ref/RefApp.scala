package ccas.analysis.apps.ref

import java.time.{Duration as JDuration, Instant}
import java.time.temporal.ChronoUnit
import scala.annotation.nowarn

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Task, UIO, ZIO, ZIOAppDefault}
import zio.http.{Client, URL}

import ccas.analysis.tables.{ClubMatch, ClubMatchBoard, ClubMatchRef, ClubRefSkip, MatchKey, PlayerMatchRef, PlayerRefSkip, PlayerTournamentRef, RefSkipReason, RunTrigger, Tables}
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.TeamMatchTeams
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, TournamentSlug, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerMatches, ApiPlayerTournaments}
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch
import ccas.api.tournament.ApiTournamentRound
import ccas.utils.{CcasLogger, OutputFile, display}
import ccas.utils.client.{ChessComClient, HttpStatusException}
import ccas.utils.errors.safeMessage
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO
import ccas.analysis.apps.ref.RefHelpers.parseMatchUrl

object RefApp extends ZIOAppDefault {
  override def run: RIO[Scope, Unit] =
    populate().provideSome[Scope](
      CcasLogger.live(showProgress = true),
      ChessComClient.live,
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  private final case class UnresolvedPlayer(playerId: PlayerId, username: Username)
  private final case class UnresolvedClub(clubId: ClubId, slug: ClubSlug)

  private enum ResolveResult {
    case Resolved
    case NotFound       // had candidates, tried them, none worked
    case NoData         // API returned empty candidate list
    case SkipPlayer     // player ID mismatch — don't try more matches/tournaments
  }

  private object RetryWindows {
    val NoData: Long            = 14 // days
    val NotFound: Long          = 30
    val IdMismatch: Long        = 90
    val ResolutionFailed: Long  = 7
    val ApiError: Long          = 3

    def cutoff(reason: RefSkipReason, now: Instant): Instant = {
      val days = reason match {
        case RefSkipReason.NoData           => NoData
        case RefSkipReason.NotFound         => NotFound
        case RefSkipReason.IdMismatch       => IdMismatch
        case RefSkipReason.ResolutionFailed => ResolutionFailed
        case RefSkipReason.ApiError         => ApiError
      }
      now.minus(days, ChronoUnit.DAYS)
    }

    case class Cutoffs(
      noData: Instant,
      notFound: Instant,
      idMismatch: Instant,
      resolutionFailed: Instant,
      apiError: Instant
    )

    def allCutoffs(now: Instant): Cutoffs = Cutoffs(
      noData           = cutoff(RefSkipReason.NoData, now),
      notFound         = cutoff(RefSkipReason.NotFound, now),
      idMismatch       = cutoff(RefSkipReason.IdMismatch, now),
      resolutionFailed = cutoff(RefSkipReason.ResolutionFailed, now),
      apiError         = cutoff(RefSkipReason.ApiError, now)
    )

  }

  // --- Context ---

  private class RefContext(
    val client: ChessComClient,
    val cache: Ref[Map[MatchKey, Promise[Throwable, TeamMatchTeams]]],
    val failedUrls: Ref[Map[String, String]],
    val failedUrlSource: Ref[Map[String, String]],
    val clubsResolvedDb: Ref[Int],
    val clubsResolvedApi: Ref[Int],
    val playersResolvedDb: Ref[Int],
    val playersResolvedApi: Ref[Int],
    val skippedPlayers: Ref[List[(PlayerId, Username)]],
    val playersSkippedNew: Ref[Int],
    val clubsSkippedNew: Ref[Int]
  )

  private object RefContext {
    def make(client: ChessComClient): UIO[RefContext] =
      for {
        cache              <- Ref.make(Map.empty[MatchKey, Promise[Throwable, TeamMatchTeams]])
        failedUrls         <- Ref.make(Map.empty[String, String])
        failedUrlSource    <- Ref.make(Map.empty[String, String])
        clubsResolvedDb    <- Ref.make(0)
        clubsResolvedApi   <- Ref.make(0)
        playersResolvedDb  <- Ref.make(0)
        playersResolvedApi <- Ref.make(0)
        skippedPlayers     <- Ref.make(List.empty[(PlayerId, Username)])
        playersSkippedNew  <- Ref.make(0)
        clubsSkippedNew    <- Ref.make(0)
      } yield new RefContext(
        client, cache, failedUrls, failedUrlSource,
        clubsResolvedDb, clubsResolvedApi,
        playersResolvedDb, playersResolvedApi,
        skippedPlayers, playersSkippedNew, clubsSkippedNew
      )
  }


  // --- Entry point ---

  // trigger accepted for consistency with other app entry points but not persisted (no run table)
  @nowarn("msg=unused")
  def populate(trigger: RunTrigger = RunTrigger.Cli, outputDir: Option[String] = Some("_ccas")): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      startedAt <- ZIO.succeed(Instant.now())
      client    <- ZIO.service[ChessComClient]
      ctx       <- RefContext.make(client)
      // Clubs
      clubs <- selectUnresolvedClubs
      _     <- CcasLogger.info(s"Clubs without match ref: ${clubs.size}")
      clubProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          clubBar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(clubs) { club =>
            resolveClub(ctx, club)
              *> clubProcessed.updateAndGet(_ + 1).flatMap(n =>
                clubBar.print(n, clubs.size, s"  Resolving clubs: $n/${clubs.size}")
              )
          }
        } yield ()
      }
      clubsDb         <- ctx.clubsResolvedDb.get
      clubsApi        <- ctx.clubsResolvedApi.get
      clubsSkippedNew <- ctx.clubsSkippedNew.get
      _ <- CcasLogger.info(s"Clubs resolved: $clubsDb (DB) + $clubsApi (API) = ${clubsDb + clubsApi} / ${clubs.size}, skipped: $clubsSkippedNew new")
      // Players
      players <- selectUnresolvedPlayers
      _       <- CcasLogger.info(s"Players without match ref: ${players.size}")
      playerProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          playerBar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(players) { player =>
            resolvePlayer(ctx, player)
              *> playerProcessed.updateAndGet(_ + 1).flatMap(n =>
                playerBar.print(n, players.size, s"  Resolving players: $n/${players.size}")
              )
          }
        } yield ()
      }
      playersDb         <- ctx.playersResolvedDb.get
      playersApi        <- ctx.playersResolvedApi.get
      playersSkippedNew <- ctx.playersSkippedNew.get
      skipped           <- ctx.skippedPlayers.get
      _ <- CcasLogger.info(
        s"Players resolved: $playersDb (DB) + $playersApi (API) = ${playersDb + playersApi} / ${players.size}, skipped: $playersSkippedNew new"
      )
      _ <- ZIO.whenDiscard(skipped.nonEmpty)(
        CcasLogger.warn(s"Players skipped (ID mismatch): ${skipped.size}")
      )
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      _ <- CcasLogger.info(s"Duration: ${duration.display}")
      // Output report
      failed              <- ctx.failedUrls.get
      failedSrc           <- ctx.failedUrlSource.get
      playerSkipsByReason <- PlayerRefSkip.countByReason
      clubSkipsByReason   <- ClubRefSkip.countByReason
      report = formatReport(ReportData(
        clubsTotal = clubs.size, clubsResolvedDb = clubsDb, clubsResolvedApi = clubsApi,
        clubsSkippedNew = clubsSkippedNew,
        playersTotal = players.size, playersResolvedDb = playersDb, playersResolvedApi = playersApi,
        playersSkippedNew = playersSkippedNew,
        skippedPlayers = skipped,
        playerSkipsByReason = playerSkipsByReason,
        clubSkipsByReason = clubSkipsByReason,
        startedAt = startedAt, completedAt = completedAt,
        failedQueries = failed, failedUrlSources = failedSrc
      ))
      _ <- ZIO.whenCaseDiscard(outputDir) { case Some(dir) => OutputFile.writeAndLogGlobal("ref", report, dir) }
    } yield ()

  // --- Report ---

  private case class ReportData(
    clubsTotal: Int,
    clubsResolvedDb: Int,
    clubsResolvedApi: Int,
    clubsSkippedNew: Int,
    playersTotal: Int,
    playersResolvedDb: Int,
    playersResolvedApi: Int,
    playersSkippedNew: Int,
    skippedPlayers: List[(PlayerId, Username)],
    playerSkipsByReason: List[(RefSkipReason, Long)],
    clubSkipsByReason: List[(RefSkipReason, Long)],
    startedAt: Instant,
    completedAt: Instant,
    failedQueries: Map[String, String],
    failedUrlSources: Map[String, String]
  )

  private def formatReport(d: ReportData): String = {
    val duration = JDuration.between(d.startedAt, d.completedAt)
    val sb       = new StringBuilder

    sb.append("=== Ref Resolution Report ===\n\n")
    sb.append(s"Started:   ${d.startedAt}\n")
    sb.append(s"Completed: ${d.completedAt}\n")
    sb.append(s"Duration:  ${duration.display}\n\n")

    sb.append("--- Clubs ---\n")
    sb.append(s"Total:          ${d.clubsTotal}\n")
    sb.append(s"Resolved (DB):  ${d.clubsResolvedDb}\n")
    sb.append(s"Resolved (API): ${d.clubsResolvedApi}\n")
    sb.append(s"Skipped (new):  ${d.clubsSkippedNew}\n")
    sb.append(s"Unresolved:     ${d.clubsTotal - d.clubsResolvedDb - d.clubsResolvedApi - d.clubsSkippedNew}\n\n")

    sb.append("--- Players ---\n")
    sb.append(s"Total:          ${d.playersTotal}\n")
    sb.append(s"Resolved (DB):  ${d.playersResolvedDb}\n")
    sb.append(s"Resolved (API): ${d.playersResolvedApi}\n")
    sb.append(s"Skipped (new):  ${d.playersSkippedNew}\n")
    sb.append(s"Unresolved:     ${d.playersTotal - d.playersResolvedDb - d.playersResolvedApi - d.playersSkippedNew}\n\n")

    if (d.skippedPlayers.nonEmpty) {
      sb.append(s"--- Skipped Players — ID Mismatch (${d.skippedPlayers.size}) ---\n")
      d.skippedPlayers.sortBy(_._2.toString).foreach { case (pid, username) =>
        sb.append(s"  $username (player_id=$pid)\n")
      }
      sb.append("\n")
    }

    if (d.playerSkipsByReason.nonEmpty || d.clubSkipsByReason.nonEmpty) {
      sb.append("--- Skip Totals ---\n")
      if (d.playerSkipsByReason.nonEmpty) {
        sb.append("  Players:\n")
        d.playerSkipsByReason.sortBy(_._1.toString).foreach { case (reason, count) =>
          sb.append(s"    $reason: $count\n")
        }
      }
      if (d.clubSkipsByReason.nonEmpty) {
        sb.append("  Clubs:\n")
        d.clubSkipsByReason.sortBy(_._1.toString).foreach { case (reason, count) =>
          sb.append(s"    $reason: $count\n")
        }
      }
      sb.append("\n")
    }

    val clubFailed   = d.failedQueries.filter { case (url, _) => d.failedUrlSources.get(url).contains("club") }
    val playerFailed = d.failedQueries.filter { case (url, _) => d.failedUrlSources.get(url).contains("player") }

    if (clubFailed.nonEmpty) {
      sb.append(s"--- Failed Club Queries (${clubFailed.size}) ---\n")
      clubFailed.toList.sortBy(_._1).foreach { case (url, error) =>
        sb.append(s"  $url: $error\n")
      }
      sb.append("\n")
    }

    if (playerFailed.nonEmpty) {
      sb.append(s"--- Failed Player Queries (${playerFailed.size}) ---\n")
      playerFailed.toList.sortBy(_._1).foreach { case (url, error) =>
        sb.append(s"  $url: $error\n")
      }
      sb.append("\n")
    }

    sb.toString
  }

  // --- Queries ---

  private def selectUnresolvedPlayers: RIO[Transactor, List[UnresolvedPlayer]] = {
    val c = RetryWindows.allCutoffs(Instant.now())
    connectZIO {
      sql"""SELECT p.player_id, ps.username
            FROM player p
            INNER JOIN (
              SELECT player_id, username, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY since DESC) AS rn
              FROM player_snapshot
            ) ps ON p.player_id = ps.player_id AND ps.rn = 1
            LEFT JOIN player_match_ref pmr ON p.player_id = pmr.player_id
            LEFT JOIN player_tournament_ref ptr ON p.player_id = ptr.player_id
            LEFT JOIN player_ref_skip prs ON p.player_id = prs.player_id
              AND ((prs.reason = 'NoData'           AND prs.last_attempted > ${c.noData})
                OR (prs.reason = 'NotFound'         AND prs.last_attempted > ${c.notFound})
                OR (prs.reason = 'IdMismatch'       AND prs.last_attempted > ${c.idMismatch})
                OR (prs.reason = 'ResolutionFailed' AND prs.last_attempted > ${c.resolutionFailed})
                OR (prs.reason = 'ApiError'         AND prs.last_attempted > ${c.apiError}))
            WHERE pmr.player_id IS NULL AND ptr.player_id IS NULL AND prs.player_id IS NULL""".query[UnresolvedPlayer].run().toList
    }
  }

  private def selectUnresolvedClubs: RIO[Transactor, List[UnresolvedClub]] = {
    val c = RetryWindows.allCutoffs(Instant.now())
    connectZIO {
      sql"""SELECT c.club_id, c.slug
            FROM club c
            LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
            LEFT JOIN club_ref_skip crs ON c.club_id = crs.club_id
              AND ((crs.reason = 'NoData'           AND crs.last_attempted > ${c.noData})
                OR (crs.reason = 'NotFound'         AND crs.last_attempted > ${c.notFound})
                OR (crs.reason = 'IdMismatch'       AND crs.last_attempted > ${c.idMismatch})
                OR (crs.reason = 'ResolutionFailed' AND crs.last_attempted > ${c.resolutionFailed})
                OR (crs.reason = 'ApiError'         AND crs.last_attempted > ${c.apiError}))
            WHERE cmr.club_id IS NULL AND crs.club_id IS NULL""".query[UnresolvedClub].run().toList
    }
  }

  // --- Player resolution ---

  private def resolvePlayer(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, Boolean] =
    (for {
      dbRef <- ClubMatchBoard.selectPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) =>
          PlayerMatchRef.insert(ref) *> PlayerRefSkip.deleteId(player.playerId) *>
            ctx.playersResolvedDb.update(_ + 1) *>
            CcasLogger.debug(s"  ${player.username}: resolved via DB").as(true)
        case None =>
          resolvePlayerViaMatch(ctx, player).flatMap {
            case ResolveResult.Resolved   => ZIO.succeed(true)
            case ResolveResult.SkipPlayer => skipPlayer(ctx, player, RefSkipReason.IdMismatch).as(false)
            case matchOutcome => // NotFound or NoData
              resolvePlayerViaTournament(ctx, player).flatMap {
                case ResolveResult.Resolved   => ZIO.succeed(true)
                case ResolveResult.SkipPlayer => skipPlayer(ctx, player, RefSkipReason.IdMismatch).as(false)
                case tournamentOutcome =>
                  val reason = (matchOutcome, tournamentOutcome) match {
                    case (ResolveResult.NoData, ResolveResult.NoData) => RefSkipReason.NoData
                    case _                                            => RefSkipReason.ResolutionFailed
                  }
                  skipPlayer(ctx, player, reason).as(false)
              }
          }
      }
    } yield resolved).catchAll {
      case e: HttpStatusException if e.statusCode == 404 =>
        CcasLogger.warn(s"  ${player.username}: 404 — ${e.safeMessage}") *>
          skipPlayer(ctx, player, RefSkipReason.NotFound, Some(e.safeMessage)).as(false)
      case error =>
        CcasLogger.warn(s"  ${player.username}: error — ${error.safeMessage}") *>
          skipPlayer(ctx, player, RefSkipReason.ApiError, Some(error.safeMessage)).as(false)
    }

  private def resolvePlayerViaMatch(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    for {
      playerMatches <- ctx.client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      candidates = playerMatches.finished.filter(_.board.isDefined)
      result <- if (candidates.isEmpty) {
        CcasLogger.debug(s"  ${player.username}: no finished match with board").as(ResolveResult.NoData)
      } else {
        tryMatches(ctx, player, candidates.toList)
      }
    } yield result

  private def tryMatches(
    ctx: RefContext,
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerMatch]
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    ZIO.foldLeft(candidates)(ResolveResult.NotFound: ResolveResult) { (status, m) =>
      status match {
        case ResolveResult.NotFound => tryOneMatch(ctx, player, m)
        case other                  => ZIO.succeed(other)
      }
    }

  private def tryOneMatch(
    ctx: RefContext,
    player: UnresolvedPlayer,
    m: ApiPlayerMatch
  ): RIO[CcasLogger & Transactor, ResolveResult] = {
    val parsed      = parseMatchUrl(m.`@id`)
    val boardIdxOpt = m.board.get.path.segments.lastOption.flatMap(_.toIntOption)
    boardIdxOpt match {
      case None =>
        CcasLogger.debug(s"  ${player.username}: malformed board URL ${m.board.get}").as(ResolveResult.NotFound)
      case Some(boardIdx) =>
        isFailedUrl(ctx, parsed.matchUrl).flatMap {
          case true => ZIO.succeed(ResolveResult.NotFound)
          case false =>
            fetchMatch(ctx, parsed.matchId, parsed.isLive).foldZIO(
              error => recordFailedUrl(ctx, parsed.matchUrl, error, "player").as(ResolveResult.NotFound),
              teams => RefHelpers.findPlayerIsTeam1(teams, player.username) match {
                case None => ZIO.succeed(ResolveResult.NotFound)
                case Some(isTeam1) =>
                  handleVerification(ctx, player) {
                    val ref = PlayerMatchRef(player.playerId, parsed.matchId, parsed.isLive, isTeam1, boardIdx)
                    PlayerMatchRef.insert(ref) *> PlayerRefSkip.deleteId(player.playerId) *>
                      ctx.playersResolvedApi.update(_ + 1).as(ResolveResult.Resolved)
                  }
              }
            )
        }
    }
  }

  private def resolvePlayerViaTournament(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    for {
      playerTournaments <- ctx.client.get[ApiPlayerTournaments](ApiPlayerTournaments.getUrl(player.username))
      eligible = playerTournaments.finished ++ playerTournaments.inProgress
      result <- if (eligible.isEmpty) {
        CcasLogger.debug(s"  ${player.username}: no eligible tournaments").as(ResolveResult.NoData)
      } else {
        tryTournaments(ctx, player, eligible.toList)
      }
    } yield result

  private def tryTournaments(
    ctx: RefContext,
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerTournaments.ApiPlayerTournament]
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    ZIO.foldLeft(candidates)(ResolveResult.NotFound: ResolveResult) { (status, t) =>
      status match {
        case ResolveResult.NotFound => tryOneTournament(ctx, player, t)
        case other                  => ZIO.succeed(other)
      }
    }

  private def tryOneTournament(
    ctx: RefContext,
    player: UnresolvedPlayer,
    t: ApiPlayerTournaments.ApiPlayerTournament
  ): RIO[CcasLogger & Transactor, ResolveResult] = {
    val slug     = TournamentSlug.fromUrl(t.`@id`)
    val roundUrl = ApiTournamentRound.getUrl(slug, 1)
    isFailedUrl(ctx, roundUrl).flatMap {
      case true => ZIO.succeed(ResolveResult.NotFound)
      case false =>
        ctx.client.get[ApiTournamentRound](roundUrl).foldZIO(
          error => recordFailedUrl(ctx, roundUrl, error, "player").as(ResolveResult.NotFound),
          round => {
            val playerIdx = round.players.indexWhere(rp => rp.username == player.username)
            if (playerIdx < 0) {
              ZIO.succeed(ResolveResult.NotFound)
            } else {
              handleVerification(ctx, player) {
                val ref = PlayerTournamentRef(player.playerId, slug, playerIdx)
                PlayerTournamentRef.insert(ref) *> PlayerRefSkip.deleteId(player.playerId) *>
                  ctx.playersResolvedApi.update(_ + 1).as(ResolveResult.Resolved)
              }
            }
          }
        )
    }
  }

  // --- Club resolution ---

  private def resolveClub(
    ctx: RefContext,
    club: UnresolvedClub
  ): RIO[CcasLogger & Transactor, Boolean] =
    (for {
      dbRef <- ClubMatch.selectClubMatchRef(club.clubId)
      resolved <- dbRef match {
        case Some(ref) =>
          ClubMatchRef.insert(ref) *> ClubRefSkip.deleteId(club.clubId) *>
            ctx.clubsResolvedDb.update(_ + 1) *>
            CcasLogger.debug(s"  ${club.slug}: resolved via DB").as(true)
        case None =>
          for {
            clubMatches <- ctx.client.get[ApiClubMatches](ApiClubMatches.getUrl(club.slug))
            result <- if (clubMatches.finished.isEmpty) {
              skipClub(ctx, club, RefSkipReason.NoData).as(false)
            } else {
              tryClubMatches(ctx, club, clubMatches.finished.toList).flatMap {
                case true  => ZIO.succeed(true)
                case false => skipClub(ctx, club, RefSkipReason.ResolutionFailed).as(false)
              }
            }
          } yield result
      }
    } yield resolved).catchAll {
      case e: HttpStatusException if e.statusCode == 404 =>
        CcasLogger.warn(s"  ${club.slug}: 404 — ${e.safeMessage}") *>
          skipClub(ctx, club, RefSkipReason.NotFound, Some(e.safeMessage)).as(false)
      case error =>
        CcasLogger.warn(s"  ${club.slug}: error — ${error.safeMessage}") *>
          skipClub(ctx, club, RefSkipReason.ApiError, Some(error.safeMessage)).as(false)
    }

  private def tryClubMatches(
    ctx: RefContext,
    club: UnresolvedClub,
    candidates: List[ApiClubMatches.ApiClubMatchFinished]
  ): RIO[CcasLogger & Transactor, Boolean] =
    ZIO.foldLeft(candidates)(false) { (resolved, m) =>
      if (resolved) { ZIO.succeed(true) }
      else { tryOneClubMatch(ctx, club, m) }
    }

  private def tryOneClubMatch(
    ctx: RefContext,
    club: UnresolvedClub,
    m: ApiClubMatches.ApiClubMatchFinished
  ): RIO[Transactor, Boolean] = {
    val parsed = parseMatchUrl(m.`@id`)
    isFailedUrl(ctx, parsed.matchUrl).flatMap {
      case true => ZIO.succeed(false)
      case false =>
        fetchMatch(ctx, parsed.matchId, parsed.isLive).foldZIO(
          error => recordFailedUrl(ctx, parsed.matchUrl, error, "club").as(false),
          teams => RefHelpers.findClubIsTeam1(teams, club.slug) match {
            case None => ZIO.succeed(false)
            case Some(isTeam1) =>
              val ref = ClubMatchRef(club.clubId, parsed.matchId, parsed.isLive, isTeam1)
              ClubMatchRef.insert(ref) *> ClubRefSkip.deleteId(club.clubId) *>
                ctx.clubsResolvedApi.update(_ + 1).as(true)
          }
        )
    }
  }

  // --- Match fetching ---

  private def fetchMatch(
    ctx: RefContext,
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    for {
      promise <- Promise.make[Throwable, TeamMatchTeams]
      key = MatchKey(matchId, isLive)
      action <- ctx.cache.modify { m =>
        m.get(key) match {
          case Some(existing) => (existing.await, m)
          case None           => (fetchAndComplete(ctx.client, promise, matchId, isLive), m + (key -> promise))
        }
      }
      result <- action
    } yield result

  private def fetchAndComplete(
    client: ChessComClient,
    promise: Promise[Throwable, TeamMatchTeams],
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    RefHelpers.fetchTeamMatchTeams(client, matchId, isLive).tapBoth(promise.fail, promise.succeed)

  // --- Shared helpers ---

  private def verifyPlayerId(
    client: ChessComClient,
    username: Username,
    expectedPlayerId: PlayerId
  ): Task[Either[PlayerId, Unit]] =
    client.get[ApiPlayer](ApiPlayer.getUrl(username)).map { apiPlayer =>
      if (apiPlayer.playerId == expectedPlayerId) { Right(()) }
      else { Left(apiPlayer.playerId) }
    }

  private def handleVerification(
    ctx: RefContext,
    player: UnresolvedPlayer
  )(onVerified: => RIO[Transactor, ResolveResult]): RIO[CcasLogger & Transactor, ResolveResult] =
    verifyPlayerId(ctx.client, player.username, player.playerId).foldZIO(
      error =>
        CcasLogger.warn(s"  ${player.username}: verification error — ${error.safeMessage}").as(ResolveResult.NotFound),
      {
        case Left(actualId) =>
          CcasLogger.warn(
            s"  ${player.username}: player_id mismatch (expected ${player.playerId}, actual $actualId), skipping"
          ) *> ctx.skippedPlayers.update(_ :+ (player.playerId, player.username)).as(ResolveResult.SkipPlayer)
        case Right(()) => onVerified
      }
    )

  private def isFailedUrl(ctx: RefContext, url: URL): UIO[Boolean] =
    ctx.failedUrls.get.map(_.contains(url.encode))

  private def recordFailedUrl(ctx: RefContext, url: URL, error: Throwable, source: String): UIO[Unit] =
    ctx.failedUrls.update(_ + (url.encode -> error.safeMessage)) *>
      ctx.failedUrlSource.update(_ + (url.encode -> source))

  private def skipPlayer(
    ctx: RefContext,
    player: UnresolvedPlayer,
    reason: RefSkipReason,
    detail: Option[String] = None
  ): RIO[Transactor, Unit] =
    PlayerRefSkip.upsert(PlayerRefSkip(player.playerId, reason, detail, Instant.now())) *>
      ctx.playersSkippedNew.update(_ + 1)

  private def skipClub(
    ctx: RefContext,
    club: UnresolvedClub,
    reason: RefSkipReason,
    detail: Option[String] = None
  ): RIO[Transactor, Unit] =
    ClubRefSkip.upsert(ClubRefSkip(club.clubId, reason, detail, Instant.now())) *>
      ctx.clubsSkippedNew.update(_ + 1)
}
