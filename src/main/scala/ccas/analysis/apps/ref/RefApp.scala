package ccas.analysis.apps.ref

import java.time.{Duration as JDuration, Instant}
import scala.annotation.nowarn

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Task, ZIO, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.{ClubMatch, ClubMatchBoard, ClubMatchRef, PlayerMatchRef, PlayerTournamentRef, RunTrigger, Tables}
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, TeamMatchTeams}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, TournamentSlug, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerMatches, ApiPlayerTournaments}
import ccas.api.tournament.ApiTournament
import ccas.utils.ProgressBar
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.SqlZioTypes.connectZIO

object RefApp extends ZIOAppDefault {

  private final case class UnresolvedPlayer(playerId: PlayerId, username: Username)
  private final case class UnresolvedClub(clubId: ClubId, slug: ClubSlug)

  override def run: RIO[Scope, Unit] =
    populate().provide(
      ChessComClient.live(),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  // trigger accepted for consistency with other app entry points but not persisted (no run table)
  @nowarn("msg=unused")
  def populate(trigger: RunTrigger = RunTrigger.Cli): RIO[ChessComClient & Transactor, Unit] =
    for {
      startedAt <- ZIO.succeed(Instant.now())
      client    <- ZIO.service[ChessComClient]
      cache     <- Ref.make(Map.empty[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]])
      // Clubs
      clubs       <- selectUnresolvedClubs
      _           <- ZIO.logInfo(s"Clubs without match ref: ${clubs.size}")
      clubCounter   <- Ref.make(0)
      clubProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          clubBar <- ProgressBar.scoped
          _ <- ZIO.foreachParDiscard(clubs) { club =>
            resolveClub(client, cache, club, clubBar).tap(r => clubCounter.update(_ + 1).when(r.isDefined))
              *> clubProcessed.updateAndGet(_ + 1).flatMap(n =>
                clubBar.print(n, clubs.size, s"  Resolving clubs: $n/${clubs.size}")
              )
          }
        } yield ()
      }
      resolvedClubs <- clubCounter.get
      _             <- ZIO.logInfo(s"Resolved: $resolvedClubs / ${clubs.size}")
      // Players
      players       <- selectUnresolvedPlayers
      _             <- ZIO.logInfo(s"Players without match ref: ${players.size}")
      playerCounter   <- Ref.make(0)
      playerProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          playerBar <- ProgressBar.scoped
          _ <- ZIO.foreachParDiscard(players) { player =>
            resolvePlayer(client, cache, player, playerBar).tap(r => playerCounter.update(_ + 1).when(r))
              *> playerProcessed.updateAndGet(_ + 1).flatMap(n =>
                playerBar.print(n, players.size, s"  Resolving players: $n/${players.size}")
              )
          }
        } yield ()
      }
      resolvedPlayers <- playerCounter.get
      _               <- ZIO.logInfo(s"Resolved: $resolvedPlayers / ${players.size}")
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      _ <- ZIO.logInfo(s"Duration: ${duration.toMinutes}m ${duration.toSecondsPart}s")
    } yield ()

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

  private def resolvePlayer(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    player: UnresolvedPlayer,
    bar: ProgressBar
  ): RIO[Transactor, Boolean] =
    (for {
      // Try DB first (from HistoryApp's club_match_board data)
      dbRef <- ClubMatchBoard.selectPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) => PlayerMatchRef.upsert(ref).as(true)
        case None =>
          resolvePlayerViaMatch(client, cache, player, bar).flatMap {
            case true  => ZIO.succeed(true)
            case false => resolvePlayerViaTournament(client, player, bar)
          }
      }
    } yield resolved).catchAll(error => bar.logWarning(s"  ${player.username}: error — ${error.getMessage}").as(false))

  private def resolvePlayerViaMatch(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    player: UnresolvedPlayer,
    bar: ProgressBar
  ): RIO[Transactor, Boolean] =
    for {
      playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      result <- playerMatches.finished.find(_.board.isDefined) match {
        case None => bar.logInfo(s"  ${player.username}: no finished match with board").as(false)
        case Some(m) =>
          val matchId  = ClubMatchId.fromUrl(m.`@id`)
          val isLive   = m.`@id`.path.segments.contains("live")
          val boardIdx = m.board.get.path.segments.last.toInt
          for {
            teams <- fetchMatch(client, cache, matchId, isLive)
            r <- findIsTeam1(teams, player.username) match {
              case None => bar.logInfo(s"  ${player.username}: not found in match $matchId teams").as(false)
              case Some(isTeam1) =>
                verifyPlayerId(client, player.username, player.playerId).flatMap {
                  case false =>
                    bar.logWarning(s"  ${player.username}: player_id mismatch, skipping").as(false)
                  case true =>
                    val ref = PlayerMatchRef(player.playerId, matchId, isLive, isTeam1, boardIdx)
                    PlayerMatchRef.upsert(ref).as(true)
                }
            }
          } yield r
      }
    } yield result

  private def resolvePlayerViaTournament(
    client: ChessComClient,
    player: UnresolvedPlayer,
    bar: ProgressBar
  ): RIO[Transactor, Boolean] =
    for {
      playerTournaments <- client.get[ApiPlayerTournaments](ApiPlayerTournaments.getUrl(player.username))
      eligible = playerTournaments.finished ++ playerTournaments.inProgress
      result <- eligible.headOption match {
        case None => bar.logInfo(s"  ${player.username}: no eligible tournaments").as(false)
        case Some(t) =>
          val slug = TournamentSlug.fromUrl(t.`@id`)
          for {
            tournament <- client.get[ApiTournament](ApiTournament.getUrl(slug))
            playerIdx = tournament.players.indexWhere(tp =>
              Username.unwrap(tp.username).equalsIgnoreCase(Username.unwrap(player.username))
            )
            r <- if (playerIdx < 0) {
              bar.logInfo(s"  ${player.username}: not found in tournament $slug players").as(false)
            } else {
              verifyPlayerId(client, player.username, player.playerId).flatMap {
                case false =>
                  bar.logWarning(s"  ${player.username}: player_id mismatch, skipping").as(false)
                case true =>
                  val ref = PlayerTournamentRef(player.playerId, slug, playerIdx)
                  PlayerTournamentRef.upsert(ref).as(true)
              }
            }
          } yield r
      }
    } yield result

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
    val u = Username.unwrap(username)
    if (teams.team1.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u))) { Some(true) }
    else if (teams.team2.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u))) { Some(false) }
    else { None }
  }

  private def findClubIsTeam1(teams: TeamMatchTeams, slug: ClubSlug): Option[Boolean] = {
    val name = ClubSlug.unwrap(slug)
    if (teams.team1.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(true) }
    else if (teams.team2.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(false) }
    else { None }
  }

  private def verifyPlayerId(client: ChessComClient, username: Username, expectedPlayerId: PlayerId): Task[Boolean] =
    client.get[ApiPlayer](ApiPlayer.getUrl(username))
      .map(_.playerId == expectedPlayerId)
      .catchAll(_ => ZIO.succeed(false))

  // --- Club resolution ---

  private def selectUnresolvedClubs: RIO[Transactor, List[UnresolvedClub]] =
    connectZIO {
      sql"""SELECT c.club_id, c.slug
            FROM club c
            LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
            WHERE cmr.club_id IS NULL""".query[UnresolvedClub].run().toList
    }

  private def resolveClub(
    client: ChessComClient,
    cache: Ref[Map[(ClubMatchId, Boolean), Promise[Throwable, TeamMatchTeams]]],
    club: UnresolvedClub,
    bar: ProgressBar
  ): RIO[Transactor, Option[ClubMatchRef]] =
    (for {
      // Try DB first (from HistoryApp's club_match data)
      dbRef <- ClubMatch.selectClubMatchRef(club.clubId)
      ref <- dbRef match {
        case Some(ref) => ClubMatchRef.upsert(ref).as(Some(ref))
        case None =>
          // Fall back to API — any finished team match works (daily or live)
          for {
            clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(club.slug))
            result <- clubMatches.finished.headOption match {
              case None => bar.logInfo(s"  ${club.slug}: no finished match").as(None)
              case Some(m) =>
                val matchId = ClubMatchId.fromUrl(m.`@id`)
                val isLive  = m.`@id`.path.segments.contains("live")
                for {
                  teams <- fetchMatch(client, cache, matchId, isLive)
                  r <- findClubIsTeam1(teams, club.slug) match {
                    case None => bar.logInfo(s"  ${club.slug}: not found in match $matchId teams").as(None)
                    case Some(isTeam1) =>
                      val ref = ClubMatchRef(club.clubId, matchId, isLive, isTeam1)
                      ClubMatchRef.upsert(ref).as(Some(ref))
                  }
                } yield r
            }
          } yield result
      }
    } yield ref).catchAll(error => bar.logWarning(s"  ${club.slug}: error — ${error.getMessage}").as(None))
}
