package ccas.analysis.apps.matchref

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Promise, RIO, Ref, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.{ClubMatchRef, PlayerMatchRef, Tables}
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubUrlName, PlayerId, Username}
import ccas.api.player.ApiPlayerMatches
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.SqlZioTypes.connectZIO

object MatchRefApp extends ZIOAppDefault {

  private final case class UnresolvedPlayer(playerId: PlayerId, username: Username)
  private final case class UnresolvedClub(clubId: ClubId, urlName: ClubUrlName)

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    populate.provide(
      ChessComClient.live(),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  def populate: RIO[ChessComClient & Transactor, Unit] =
    for {
      client <- ZIO.service[ChessComClient]
      cache  <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiDailyMatch]])
      // Clubs
      clubs       <- selectUnresolvedClubs
      _           <- ZIO.logInfo(s"Clubs without match ref: ${clubs.size}")
      clubCounter <- Ref.make(0)
      _ <- ZIO.foreachParDiscard(clubs) { club =>
        resolveClub(client, cache, club).tap(r => clubCounter.update(_ + 1).when(r.isDefined))
      }
      resolvedClubs <- clubCounter.get
      _             <- ZIO.logInfo(s"Resolved: $resolvedClubs / ${clubs.size}")
      // Players
      players       <- selectUnresolvedPlayers
      _             <- ZIO.logInfo(s"Players without match ref: ${players.size}")
      playerCounter <- Ref.make(0)
      _ <- ZIO.foreachParDiscard(players) { player =>
        resolvePlayer(client, cache, player).tap(r => playerCounter.update(_ + 1).when(r.isDefined))
      }
      resolvedPlayers <- playerCounter.get
      _               <- ZIO.logInfo(s"Resolved: $resolvedPlayers / ${players.size}")
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
    player: UnresolvedPlayer
  ): RIO[Transactor, Option[PlayerMatchRef]] =
    (for {
      playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      ref <- playerMatches.finished.find(_.board.isDefined) match {
        case None => ZIO.logInfo(s"  ${player.username}: no finished match with board").as(None)
        case Some(m) =>
          val matchId  = ClubMatchId.wrap(m.`@id`.path.segments.last.toLong)
          val boardIdx = m.board.get.path.segments.last.toInt
          for {
            dailyMatch <- fetchMatch(client, cache, matchId)
            result <- findTeamIdx(dailyMatch, player.username) match {
              case None => ZIO.logInfo(s"  ${player.username}: not found in match $matchId teams").as(None)
              case Some(idx) =>
                val ref = PlayerMatchRef(player.playerId, matchId, idx, boardIdx)
                PlayerMatchRef.upsert(ref).as(Some(ref))
            }
          } yield result
      }
    } yield ref).catchAll(error => ZIO.logWarning(s"  ${player.username}: error — ${error.getMessage}").as(None))

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

  private def findTeamIdx(dailyMatch: ApiDailyMatch, username: Username): Option[Int] = {
    val teams = dailyMatch.teams
    val u     = Username.unwrap(username)
    if (teams.team1.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u))) { Some(1) }
    else if (teams.team2.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u))) { Some(2) }
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
    club: UnresolvedClub
  ): RIO[Transactor, Option[ClubMatchRef]] =
    (for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(club.urlName))
      ref <- clubMatches.finished.headOption match {
        case None => ZIO.logInfo(s"  ${club.urlName}: no finished match").as(None)
        case Some(m) =>
          val matchId = ClubMatchId.wrap(m.`@id`.path.segments.last.toLong)
          for {
            dailyMatch <- fetchMatch(client, cache, matchId)
            result <- findClubTeamIdx(dailyMatch, club.urlName) match {
              case None => ZIO.logInfo(s"  ${club.urlName}: not found in match $matchId teams").as(None)
              case Some(idx) =>
                val ref = ClubMatchRef(club.clubId, matchId, idx)
                ClubMatchRef.upsert(ref).as(Some(ref))
            }
          } yield result
      }
    } yield ref).catchAll(error => ZIO.logWarning(s"  ${club.urlName}: error — ${error.getMessage}").as(None))

  private def findClubTeamIdx(dailyMatch: ApiDailyMatch, urlName: ClubUrlName): Option[Int] = {
    val teams = dailyMatch.teams
    val name  = ClubUrlName.unwrap(urlName)
    if (teams.team1.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(1) }
    else if (teams.team2.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(2) }
    else { None }
  }
}
