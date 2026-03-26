package ccas.analysis.apps.ref

import java.time.{Duration as JDuration, Instant}
import scala.annotation.nowarn

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Task, UIO, ZIO, ZIOAppDefault}
import zio.http.{Client, URL}

import ccas.analysis.tables.{ClubMatch, ClubMatchBoard, ClubMatchRef, PlayerMatchRef, PlayerTournamentRef, RunTrigger, Tables}
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

  private final case class UnresolvedPlayer(playerId: PlayerId, username: Username)
  private final case class UnresolvedClub(clubId: ClubId, slug: ClubSlug)

  private enum ResolveResult {
    case Resolved
    case NotFound
    case SkipPlayer // player ID mismatch — don't try more matches/tournaments
  }

  override def run: RIO[Scope, Unit] =
    populate().provideSome[Scope](
      CcasLogger.live(showProgress = true),
      ChessComClient.live(),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  // trigger accepted for consistency with other app entry points but not persisted (no run table)
  @nowarn("msg=unused")
  def populate(trigger: RunTrigger = RunTrigger.Cli, outputDir: String = "_ccas"): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      startedAt  <- ZIO.succeed(Instant.now())
      client     <- ZIO.service[ChessComClient]
      cache      <- Ref.make(Map.empty[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]])
      failedUrls <- Ref.make(Map.empty[String, String])
      // Clubs
      clubs       <- selectUnresolvedClubs
      _           <- CcasLogger.info(s"Clubs without match ref: ${clubs.size}")
      clubCounter   <- Ref.make(0)
      clubProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          clubBar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(clubs) { club =>
            resolveClub(client, cache, failedUrls, club).tap(r => clubCounter.update(_ + 1).when(r))
              *> clubProcessed.updateAndGet(_ + 1).flatMap(n =>
                clubBar.print(n, clubs.size, s"  Resolving clubs: $n/${clubs.size}")
              )
          }
        } yield ()
      }
      resolvedClubs <- clubCounter.get
      _             <- CcasLogger.info(s"Resolved: $resolvedClubs / ${clubs.size}")
      // Players
      players       <- selectUnresolvedPlayers
      _             <- CcasLogger.info(s"Players without match ref: ${players.size}")
      playerCounter   <- Ref.make(0)
      playerProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          playerBar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(players) { player =>
            resolvePlayer(client, cache, failedUrls, player).tap(r => playerCounter.update(_ + 1).when(r))
              *> playerProcessed.updateAndGet(_ + 1).flatMap(n =>
                playerBar.print(n, players.size, s"  Resolving players: $n/${players.size}")
              )
          }
        } yield ()
      }
      resolvedPlayers <- playerCounter.get
      _               <- CcasLogger.info(s"Resolved: $resolvedPlayers / ${players.size}")
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      _ <- CcasLogger.info(s"Duration: ${duration.toMinutes}m ${duration.toSecondsPart}s")
      // Output report
      failed <- failedUrls.get
      report = formatReport(
        clubs.size, resolvedClubs, players.size, resolvedPlayers,
        startedAt, completedAt, failed
      )
      _ <- OutputFile.writeAndLogGlobal("ref", report, outputDir)
    } yield ()

  // --- Report ---

  private def formatReport(
    clubsTotal: Int,
    clubsResolved: Int,
    playersTotal: Int,
    playersResolved: Int,
    startedAt: Instant,
    completedAt: Instant,
    failedQueries: Map[String, String]
  ): String = {
    val duration = JDuration.between(startedAt, completedAt)
    val sb       = new StringBuilder

    sb.append("=== Ref Resolution Report ===\n\n")
    sb.append(s"Started:   $startedAt\n")
    sb.append(s"Completed: $completedAt\n")
    sb.append(s"Duration:  ${duration.toMinutes}m ${duration.toSecondsPart}s\n\n")

    sb.append("--- Clubs ---\n")
    sb.append(s"Total:    $clubsTotal\n")
    sb.append(s"Resolved: $clubsResolved\n\n")

    sb.append("--- Players ---\n")
    sb.append(s"Total:    $playersTotal\n")
    sb.append(s"Resolved: $playersResolved\n\n")

    if (failedQueries.nonEmpty) {
      sb.append(s"--- Failed Queries (${failedQueries.size}) ---\n")
      failedQueries.toList.sortBy(_._1).foreach { case (url, error) =>
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
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, Boolean] =
    (for {
      // Try DB first (from HistoryApp's club_match_board data)
      dbRef <- ClubMatchBoard.selectPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) => PlayerMatchRef.upsert(ref).as(true)
        case None =>
          resolvePlayerViaMatch(client, cache, failedUrls, player).flatMap {
            case ResolveResult.Resolved   => ZIO.succeed(true)
            case ResolveResult.SkipPlayer => ZIO.succeed(false)
            case ResolveResult.NotFound   =>
              resolvePlayerViaTournament(client, failedUrls, player).map(_ == ResolveResult.Resolved)
          }
      }
    } yield resolved).catchAll(error =>
      CcasLogger.warn(s"  ${player.username}: error — ${error.safeMessage}").as(false)
    )

  private def resolvePlayerViaMatch(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    for {
      playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      candidates = playerMatches.finished.filter(_.board.isDefined)
      result <- if (candidates.isEmpty) {
        CcasLogger.info(s"  ${player.username}: no finished match with board").as(ResolveResult.NotFound)
      } else {
        tryMatches(client, cache, failedUrls, player, candidates.toList)
      }
    } yield result

  private def tryMatches(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerMatch]
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    ZIO.foldLeft(candidates)(ResolveResult.NotFound: ResolveResult) { (status, m) =>
      status match {
        case ResolveResult.NotFound => tryOneMatch(client, cache, failedUrls, player, m)
        case other                  => ZIO.succeed(other)
      }
    }

  private def tryOneMatch(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer,
    m: ApiPlayerMatch
  ): RIO[CcasLogger & Transactor, ResolveResult] = {
    val matchId  = ClubMatchId.fromUrl(m.`@id`)
    val isLive   = m.`@id`.path.segments.contains("live")
    val boardIdx = m.board.get.path.segments.last.toInt
    val matchUrl = if (isLive) { ApiLiveMatch.getUrl(matchId) } else { ApiDailyMatch.getUrl(matchId) }
    isFailedUrl(failedUrls, matchUrl).flatMap {
      case true => ZIO.succeed(ResolveResult.NotFound)
      case false =>
        fetchMatch(client, cache, matchId, isLive).foldZIO(
          error => recordFailedUrl(failedUrls, matchUrl, error).as(ResolveResult.NotFound),
          teams => findIsTeam1(teams, player.username) match {
            case None => ZIO.succeed(ResolveResult.NotFound)
            case Some(isTeam1) =>
              verifyPlayerId(client, player.username, player.playerId).flatMap {
                case false =>
                  CcasLogger.warn(s"  ${player.username}: player_id mismatch, skipping").as(ResolveResult.SkipPlayer)
                case true =>
                  val ref = PlayerMatchRef(player.playerId, matchId, isLive, isTeam1, boardIdx)
                  PlayerMatchRef.upsert(ref).as(ResolveResult.Resolved)
              }
          }
        )
    }
  }

  private def resolvePlayerViaTournament(
    client: ChessComClient,
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    for {
      playerTournaments <- client.get[ApiPlayerTournaments](ApiPlayerTournaments.getUrl(player.username))
      eligible = playerTournaments.finished ++ playerTournaments.inProgress
      result <- if (eligible.isEmpty) {
        CcasLogger.info(s"  ${player.username}: no eligible tournaments").as(ResolveResult.NotFound)
      } else {
        tryTournaments(client, failedUrls, player, eligible.toList)
      }
    } yield result

  private def tryTournaments(
    client: ChessComClient,
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerTournaments.ApiPlayerTournament]
  ): RIO[CcasLogger & Transactor, ResolveResult] =
    ZIO.foldLeft(candidates)(ResolveResult.NotFound: ResolveResult) { (status, t) =>
      status match {
        case ResolveResult.NotFound => tryOneTournament(client, failedUrls, player, t)
        case other                  => ZIO.succeed(other)
      }
    }

  private def tryOneTournament(
    client: ChessComClient,
    failedUrls: Ref[Map[String, String]],
    player: UnresolvedPlayer,
    t: ApiPlayerTournaments.ApiPlayerTournament
  ): RIO[CcasLogger & Transactor, ResolveResult] = {
    val slug     = TournamentSlug.fromUrl(t.`@id`)
    val roundUrl = ApiTournamentRound.getUrl(slug, 1)
    isFailedUrl(failedUrls, roundUrl).flatMap {
      case true => ZIO.succeed(ResolveResult.NotFound)
      case false =>
        client.get[ApiTournamentRound](roundUrl).foldZIO(
          error => recordFailedUrl(failedUrls, roundUrl, error).as(ResolveResult.NotFound),
          round => {
            val playerIdx = round.players.indexWhere(rp =>
              rp.username == player.username
            )
            if (playerIdx < 0) {
              ZIO.succeed(ResolveResult.NotFound)
            } else {
              verifyPlayerId(client, player.username, player.playerId).flatMap {
                case false =>
                  CcasLogger.warn(s"  ${player.username}: player_id mismatch, skipping").as(ResolveResult.SkipPlayer)
                case true =>
                  val ref = PlayerTournamentRef(player.playerId, slug, playerIdx)
                  PlayerTournamentRef.upsert(ref).as(ResolveResult.Resolved)
              }
            }
          }
        )
    }
  }

  // --- Club resolution ---

  private def resolveClub(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    club: UnresolvedClub
  ): RIO[CcasLogger & Transactor, Boolean] =
    (for {
      // Try DB first (from HistoryApp's club_match data)
      dbRef <- ClubMatch.selectClubMatchRef(club.clubId)
      resolved <- dbRef match {
        case Some(ref) => ClubMatchRef.upsert(ref).as(true)
        case None =>
          // Fall back to API — iterate finished team matches (daily or live)
          for {
            clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(club.slug))
            result <- if (clubMatches.finished.isEmpty) {
              CcasLogger.info(s"  ${club.slug}: no finished match").as(false)
            } else {
              tryClubMatches(client, cache, failedUrls, club, clubMatches.finished.toList)
            }
          } yield result
      }
    } yield resolved).catchAll(error =>
      CcasLogger.warn(s"  ${club.slug}: error — ${error.safeMessage}").as(false)
    )

  private def tryClubMatches(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    club: UnresolvedClub,
    candidates: List[ApiClubMatches.ApiClubMatchFinished]
  ): RIO[CcasLogger & Transactor, Boolean] =
    ZIO.foldLeft(candidates)(false) { (resolved, m) =>
      if (resolved) { ZIO.succeed(true) }
      else { tryOneClubMatch(client, cache, failedUrls, club, m) }
    }

  private def tryOneClubMatch(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    failedUrls: Ref[Map[String, String]],
    club: UnresolvedClub,
    m: ApiClubMatches.ApiClubMatchFinished
  ): RIO[Transactor, Boolean] = {
    val matchId  = ClubMatchId.fromUrl(m.`@id`)
    val isLive   = m.`@id`.path.segments.contains("live")
    val matchUrl = if (isLive) { ApiLiveMatch.getUrl(matchId) } else { ApiDailyMatch.getUrl(matchId) }
    isFailedUrl(failedUrls, matchUrl).flatMap {
      case true => ZIO.succeed(false)
      case false =>
        fetchMatch(client, cache, matchId, isLive).foldZIO(
          error => recordFailedUrl(failedUrls, matchUrl, error).as(false),
          teams => findClubIsTeam1(teams, club.slug) match {
            case None => ZIO.succeed(false)
            case Some(isTeam1) =>
              val ref = ClubMatchRef(club.clubId, matchId, isLive, isTeam1)
              ClubMatchRef.upsert(ref).as(true)
          }
        )
    }
  }

  // --- Match fetching ---

  private def fetchMatch(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    for {
      promise <- Promise.make[Throwable, TeamMatchTeams]
      key = (matchId, isLive)
      action <- cache.modify { m =>
        m.get(key) match {
          case Some(existing) => (existing.await, m)
          case None           => (fetchAndComplete(client, promise, matchId, isLive), m + (key -> promise))
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

  private def verifyPlayerId(client: ChessComClient, username: Username, expectedPlayerId: PlayerId): Task[Boolean] =
    client.get[ApiPlayer](ApiPlayer.getUrl(username))
      .map(_.playerId == expectedPlayerId)
      .catchAll(_ => ZIO.succeed(false))

  private def isFailedUrl(failedUrls: Ref[Map[String, String]], url: URL): UIO[Boolean] =
    failedUrls.get.map(_.contains(url.encode))

  private def recordFailedUrl(failedUrls: Ref[Map[String, String]], url: URL, error: Throwable): UIO[Unit] =
    failedUrls.update(_ + (url.encode -> error.safeMessage))
}
