package ccas.analysis.apps.ref

import java.time.{Duration as JDuration, Instant}
import scala.annotation.nowarn

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Task, UIO, ZIO, ZIOAppDefault}
import zio.http.{Client, URL}

import ccas.analysis.tables.{ClubMatch, ClubMatchBoard, ClubMatchRef, MatchKey, PlayerMatchRef, PlayerTournamentRef, RunTrigger, Tables}
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, TeamMatchTeams}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, TournamentSlug, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerMatches, ApiPlayerTournaments}
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch
import ccas.api.tournament.ApiTournamentRound
import ccas.utils.{CcasLogger, OutputFile}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.safeMessage
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.SqlZioTypes.connectZIO

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
    case NotFound
    case SkipPlayer // player ID mismatch — don't try more matches/tournaments
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
    val skippedPlayers: Ref[List[(PlayerId, Username)]]
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
      } yield new RefContext(
        client, cache, failedUrls, failedUrlSource,
        clubsResolvedDb, clubsResolvedApi,
        playersResolvedDb, playersResolvedApi,
        skippedPlayers
      )
  }

  // --- Match URL parsing ---

  private case class ParsedMatch(matchId: ClubMatchId, isLive: Boolean, matchUrl: URL)

  private def parseMatchUrl(atId: URL): ParsedMatch = {
    val matchId  = ClubMatchId.fromUrl(atId)
    val isLive   = atId.path.segments.contains("live")
    val matchUrl = if (isLive) { ApiLiveMatch.getUrl(matchId) } else { ApiDailyMatch.getUrl(matchId) }
    ParsedMatch(matchId, isLive, matchUrl)
  }

  // --- Entry point ---

  // trigger accepted for consistency with other app entry points but not persisted (no run table)
  @nowarn("msg=unused")
  def populate(trigger: RunTrigger = RunTrigger.Cli, outputDir: String = "_ccas"): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
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
      clubsDb  <- ctx.clubsResolvedDb.get
      clubsApi <- ctx.clubsResolvedApi.get
      _ <- CcasLogger.info(s"Clubs resolved: $clubsDb (DB) + $clubsApi (API) = ${clubsDb + clubsApi} / ${clubs.size}")
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
      playersDb  <- ctx.playersResolvedDb.get
      playersApi <- ctx.playersResolvedApi.get
      skipped    <- ctx.skippedPlayers.get
      _ <- CcasLogger.info(
        s"Players resolved: $playersDb (DB) + $playersApi (API) = ${playersDb + playersApi} / ${players.size}"
      )
      _ <- ZIO.whenDiscard(skipped.nonEmpty)(
        CcasLogger.warn(s"Players skipped (ID mismatch): ${skipped.size}")
      )
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      _ <- CcasLogger.info(s"Duration: ${duration.toMinutes}m ${duration.toSecondsPart}s")
      // Output report
      failed    <- ctx.failedUrls.get
      failedSrc <- ctx.failedUrlSource.get
      report = formatReport(ReportData(
        clubsTotal = clubs.size, clubsResolvedDb = clubsDb, clubsResolvedApi = clubsApi,
        playersTotal = players.size, playersResolvedDb = playersDb, playersResolvedApi = playersApi,
        skippedPlayers = skipped,
        startedAt = startedAt, completedAt = completedAt,
        failedQueries = failed, failedUrlSources = failedSrc
      ))
      _ <- OutputFile.writeAndLogGlobal("ref", report, outputDir)
    } yield ()

  // --- Report ---

  private case class ReportData(
    clubsTotal: Int,
    clubsResolvedDb: Int,
    clubsResolvedApi: Int,
    playersTotal: Int,
    playersResolvedDb: Int,
    playersResolvedApi: Int,
    skippedPlayers: List[(PlayerId, Username)],
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
    sb.append(s"Duration:  ${duration.toMinutes}m ${duration.toSecondsPart}s\n\n")

    sb.append("--- Clubs ---\n")
    sb.append(s"Total:          ${d.clubsTotal}\n")
    sb.append(s"Resolved (DB):  ${d.clubsResolvedDb}\n")
    sb.append(s"Resolved (API): ${d.clubsResolvedApi}\n")
    sb.append(s"Unresolved:     ${d.clubsTotal - d.clubsResolvedDb - d.clubsResolvedApi}\n\n")

    sb.append("--- Players ---\n")
    sb.append(s"Total:          ${d.playersTotal}\n")
    sb.append(s"Resolved (DB):  ${d.playersResolvedDb}\n")
    sb.append(s"Resolved (API): ${d.playersResolvedApi}\n")
    if (d.skippedPlayers.nonEmpty) {
      sb.append(s"Skipped (ID mismatch): ${d.skippedPlayers.size}\n")
    }
    sb.append(s"Unresolved:     ${d.playersTotal - d.playersResolvedDb - d.playersResolvedApi - d.skippedPlayers.size}\n\n")

    if (d.skippedPlayers.nonEmpty) {
      sb.append(s"--- Skipped Players (${d.skippedPlayers.size}) ---\n")
      d.skippedPlayers.sortBy(_._2.toString).foreach { case (pid, username) =>
        sb.append(s"  $username (player_id=$pid)\n")
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

  private def selectUnresolvedPlayers: RIO[Transactor, List[UnresolvedPlayer]] =
    connectZIO {
      sql"""SELECT p.player_id, ps.username
            FROM player p
            INNER JOIN (
              SELECT player_id, username, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY since DESC) AS rn
              FROM player_snapshot
            ) ps ON p.player_id = ps.player_id AND ps.rn = 1
            LEFT JOIN player_match_ref pmr ON p.player_id = pmr.player_id
            LEFT JOIN player_tournament_ref ptr ON p.player_id = ptr.player_id
            WHERE pmr.player_id IS NULL AND ptr.player_id IS NULL""".query[UnresolvedPlayer].run().toList
    }

  private def selectUnresolvedClubs: RIO[Transactor, List[UnresolvedClub]] =
    connectZIO {
      sql"""SELECT c.club_id, c.slug
            FROM club c
            LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
            WHERE cmr.club_id IS NULL""".query[UnresolvedClub].run().toList
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
          PlayerMatchRef.insert(ref) *>
            ctx.playersResolvedDb.update(_ + 1) *>
            CcasLogger.debug(s"  ${player.username}: resolved via DB").as(true)
        case None =>
          resolvePlayerViaMatch(ctx, player).flatMap {
            case ResolveResult.Resolved   => ZIO.succeed(true)
            case ResolveResult.SkipPlayer => ZIO.succeed(false)
            case ResolveResult.NotFound   =>
              resolvePlayerViaTournament(ctx, player).map(_ == ResolveResult.Resolved)
          }
      }
    } yield resolved).catchAll(error =>
      CcasLogger.warn(s"  ${player.username}: error — ${error.safeMessage}").as(false)
    )

  private def resolvePlayerViaMatch(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    for {
      playerMatches <- ctx.client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      candidates = playerMatches.finished.filter(_.board.isDefined)
      result <- if (candidates.isEmpty) {
        CcasLogger.debug(s"  ${player.username}: no finished match with board").as(ResolveResult.NotFound)
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
              teams => findIsTeam1(teams, player.username) match {
                case None => ZIO.succeed(ResolveResult.NotFound)
                case Some(isTeam1) =>
                  handleVerification(ctx, player) {
                    val ref = PlayerMatchRef(player.playerId, parsed.matchId, parsed.isLive, isTeam1, boardIdx)
                    PlayerMatchRef.insert(ref) *> ctx.playersResolvedApi.update(_ + 1).as(ResolveResult.Resolved)
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
        CcasLogger.debug(s"  ${player.username}: no eligible tournaments").as(ResolveResult.NotFound)
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
                PlayerTournamentRef.insert(ref) *> ctx.playersResolvedApi.update(_ + 1).as(ResolveResult.Resolved)
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
          ClubMatchRef.insert(ref) *>
            ctx.clubsResolvedDb.update(_ + 1) *>
            CcasLogger.debug(s"  ${club.slug}: resolved via DB").as(true)
        case None =>
          for {
            clubMatches <- ctx.client.get[ApiClubMatches](ApiClubMatches.getUrl(club.slug))
            result <- if (clubMatches.finished.isEmpty) {
              CcasLogger.debug(s"  ${club.slug}: no finished match").as(false)
            } else {
              tryClubMatches(ctx, club, clubMatches.finished.toList)
            }
          } yield result
      }
    } yield resolved).catchAll(error =>
      CcasLogger.warn(s"  ${club.slug}: error — ${error.safeMessage}").as(false)
    )

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
          teams => findClubIsTeam1(teams, club.slug) match {
            case None => ZIO.succeed(false)
            case Some(isTeam1) =>
              val ref = ClubMatchRef(club.clubId, parsed.matchId, parsed.isLive, isTeam1)
              ClubMatchRef.insert(ref) *> ctx.clubsResolvedApi.update(_ + 1).as(true)
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
  ): Task[TeamMatchTeams] = {
    val fetch: Task[TeamMatchTeams] =
      if (isLive) { client.get[ApiLiveMatch](ApiLiveMatch.getUrl(matchId)).map(_.teams) }
      else { client.get[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).map(_.teams) }
    fetch.tapBoth(promise.fail, promise.succeed)
  }

  // --- Shared helpers ---

  private def findIsTeam1(teams: TeamMatchTeams, username: Username): Option[Boolean] = {
    if (teams.team1.players.exists(_.username == username)) { Some(true) }
    else if (teams.team2.players.exists(_.username == username)) { Some(false) }
    else { None }
  }

  private def findClubIsTeam1(teams: TeamMatchTeams, slug: ClubSlug): Option[Boolean] = {
    if (teams.team1.`@id`.path.segments.lastOption.map(ClubSlug.wrap).contains(slug)) { Some(true) }
    else if (teams.team2.`@id`.path.segments.lastOption.map(ClubSlug.wrap).contains(slug)) { Some(false) }
    else { None }
  }

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
}
