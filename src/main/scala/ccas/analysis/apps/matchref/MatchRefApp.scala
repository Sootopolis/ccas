package ccas.analysis.apps.matchref

import java.time.{Duration as JDuration, Instant}
import scala.annotation.nowarn

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Task, ZIO, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.{ClubMatch, ClubMatchBoard, ClubMatchRef, PlayerMatchRef, RunTrigger, Tables}
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubUrlName, PlayerId, Username}
import ccas.api.player.ApiPlayerMatches
import ccas.utils.ProgressBar
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.SqlZioTypes.connectZIO

object MatchRefApp extends ZIOAppDefault {

  private final case class UnresolvedPlayer(playerId: PlayerId, username: Username)
  private final case class UnresolvedClub(clubId: ClubId, urlName: ClubUrlName)

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
      cache     <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiDailyMatch]])
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
            resolvePlayer(client, cache, player, playerBar).tap(r => playerCounter.update(_ + 1).when(r.isDefined))
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
            WHERE pmr.player_id IS NULL""".query[UnresolvedPlayer].run().toList
    }

  private def resolvePlayer(
    client: ChessComClient,
    cache: Ref[Map[ClubMatchId, Promise[Throwable, ApiDailyMatch]]],
    player: UnresolvedPlayer,
    bar: ProgressBar
  ): RIO[Transactor, Option[PlayerMatchRef]] =
    (for {
      // Try DB first (from HistoryApp's club_match_board data)
      dbRef <- ClubMatchBoard.selectPlayerMatchRef(player.playerId)
      ref <- dbRef match {
        case Some(ref) => PlayerMatchRef.upsert(ref).as(Some(ref))
        case None =>
          // Fall back to API
          for {
            playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
            result <- playerMatches.finished.find(_.board.isDefined) match {
              case None => bar.logInfo(s"  ${player.username}: no finished match with board").as(None)
              case Some(m) =>
                val matchId  = ClubMatchId.wrap(m.`@id`.path.segments.last.toLong)
                val boardIdx = m.board.get.path.segments.last.toInt
                for {
                  dailyMatch <- fetchMatch(client, cache, matchId)
                  r <- findIsTeam1(dailyMatch, player.username) match {
                    case None => bar.logInfo(s"  ${player.username}: not found in match $matchId teams").as(None)
                    case Some(isTeam1) =>
                      val ref = PlayerMatchRef(player.playerId, matchId, isTeam1, boardIdx)
                      PlayerMatchRef.upsert(ref).as(Some(ref))
                  }
                } yield r
            }
          } yield result
      }
    } yield ref).catchAll(error => bar.logWarning(s"  ${player.username}: error — ${error.getMessage}").as(None))

  private def fetchMatch(
    client: ChessComClient,
    cache: Ref[Map[ClubMatchId, Promise[Throwable, ApiDailyMatch]]],
    matchId: ClubMatchId
  ): Task[ApiDailyMatch] =
    for {
      promise <- Promise.make[Throwable, ApiDailyMatch]
      action <- cache.modify { m =>
        m.get(matchId) match {
          case Some(existing) => (existing.await, m)
          case None           => (fetchAndComplete(client, promise, matchId), m + (matchId -> promise))
        }
      }
      result <- action
    } yield result

  private def fetchAndComplete(
    client: ChessComClient,
    promise: Promise[Throwable, ApiDailyMatch],
    matchId: ClubMatchId
  ): Task[ApiDailyMatch] =
    client.get[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).tapBoth(promise.fail, promise.succeed)

  private def findIsTeam1(dailyMatch: ApiDailyMatch, username: Username): Option[Boolean] = {
    val teams = dailyMatch.teams
    val u     = Username.unwrap(username)
    if (teams.team1.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u))) { Some(true) }
    else if (teams.team2.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u))) { Some(false) }
    else { None }
  }

  // --- Club resolution ---

  private def selectUnresolvedClubs: RIO[Transactor, List[UnresolvedClub]] =
    connectZIO {
      sql"""SELECT c.club_id, c.url_name
            FROM club c
            LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
            WHERE cmr.club_id IS NULL""".query[UnresolvedClub].run().toList
    }

  private def resolveClub(
    client: ChessComClient,
    cache: Ref[Map[ClubMatchId, Promise[Throwable, ApiDailyMatch]]],
    club: UnresolvedClub,
    bar: ProgressBar
  ): RIO[Transactor, Option[ClubMatchRef]] =
    (for {
      // Try DB first (from HistoryApp's club_match data)
      dbRef <- ClubMatch.selectClubMatchRef(club.clubId)
      ref <- dbRef match {
        case Some(ref) => ClubMatchRef.upsert(ref).as(Some(ref))
        case None =>
          // Fall back to API
          for {
            clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(club.urlName))
            result <- clubMatches.finished.headOption match {
              case None => bar.logInfo(s"  ${club.urlName}: no finished match").as(None)
              case Some(m) =>
                val matchId = ClubMatchId.wrap(m.`@id`.path.segments.last.toLong)
                for {
                  dailyMatch <- fetchMatch(client, cache, matchId)
                  r <- findClubIsTeam1(dailyMatch, club.urlName) match {
                    case None => bar.logInfo(s"  ${club.urlName}: not found in match $matchId teams").as(None)
                    case Some(isTeam1) =>
                      val ref = ClubMatchRef(club.clubId, matchId, isTeam1)
                      ClubMatchRef.upsert(ref).as(Some(ref))
                  }
                } yield r
            }
          } yield result
      }
    } yield ref).catchAll(error => bar.logWarning(s"  ${club.urlName}: error — ${error.getMessage}").as(None))

  private def findClubIsTeam1(dailyMatch: ApiDailyMatch, urlName: ClubUrlName): Option[Boolean] = {
    val teams = dailyMatch.teams
    val name  = ClubUrlName.unwrap(urlName)
    if (teams.team1.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(true) }
    else if (teams.team2.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(false) }
    else { None }
  }
}
