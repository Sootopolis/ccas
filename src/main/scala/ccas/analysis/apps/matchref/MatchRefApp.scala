package ccas.analysis.apps.matchref

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Console, Ref, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
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

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    populate.provide(
      ChessComClient.live(),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  def populate: ZIO[ChessComClient & Transactor, Throwable, Unit] =
    for {
      client <- ZIO.service[ChessComClient]
      cache  <- Ref.make(Map.empty[ClubMatchId, ApiDailyMatch])
      // Players
      players        <- selectUnresolvedPlayers
      _              <- Console.printLine(s"Players without match ref: ${players.size}").orDie
      resolvedPlayers <- ZIO.foldLeft(players)(0) { case (count, player) =>
        resolvePlayer(client, cache, player).map {
          case Some(_) => count + 1
          case None    => count
        }
      }
      _ <- Console.printLine(s"Resolved: $resolvedPlayers / ${players.size}").orDie
      // Clubs
      clubs        <- selectUnresolvedClubs
      _            <- Console.printLine(s"Clubs without match ref: ${clubs.size}").orDie
      resolvedClubs <- ZIO.foldLeft(clubs)(0) { case (count, club) =>
        resolveClub(client, cache, club).map {
          case Some(_) => count + 1
          case None    => count
        }
      }
      _ <- Console.printLine(s"Resolved: $resolvedClubs / ${clubs.size}").orDie
    } yield ()

  private def selectUnresolvedPlayers: ZIO[Transactor, Throwable, List[UnresolvedPlayer]] =
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
      cache: Ref[Map[ClubMatchId, ApiDailyMatch]],
      player: UnresolvedPlayer
    ): ZIO[Transactor, Throwable, Option[PlayerMatchRef]] =
    (for {
      playerMatches <- client.getWithPermit[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      matchOpt = playerMatches.finished.find(_.board.isDefined).headOption
      ref <- matchOpt match
        case None =>
          Console.printLine(s"  ${player.username}: no finished match with board").orDie.as(None)
        case Some(m) =>
          val matchId  = ClubMatchId.wrap(m.`@id`.path.segments.last.toLong)
          val boardIdx = m.board.get.path.segments.last.toInt
          for {
            dailyMatch <- fetchMatch(client, cache, matchId)
            teamIdx = findTeamIdx(dailyMatch, player.username)
            result <- teamIdx match
              case None =>
                Console.printLine(s"  ${player.username}: not found in match $matchId teams").orDie.as(None)
              case Some(idx) =>
                val ref = PlayerMatchRef(player.playerId, matchId, idx, boardIdx)
                PlayerMatchRef.upsert(ref).as(Some(ref))
          } yield result
    } yield ref).catchAll { error =>
      Console.printLine(s"  ${player.username}: error — ${error.getMessage}").orDie.as(None)
    }

  private def fetchMatch(
      client: ChessComClient,
      cache: Ref[Map[ClubMatchId, ApiDailyMatch]],
      matchId: ClubMatchId
    ): ZIO[Any, Throwable, ApiDailyMatch] =
    cache.get.flatMap(_.get(matchId) match
      case Some(m) => ZIO.succeed(m)
      case None =>
        client.getWithPermit[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).flatMap { m =>
          cache.update(_ + (matchId -> m)).as(m)
        }
    )

  private def findTeamIdx(dailyMatch: ApiDailyMatch, username: Username): Option[Int] = {
    val teams = dailyMatch.teams
    val u     = Username.unwrap(username)
    if teams.team1.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u)) then Some(1)
    else if teams.team2.players.exists(p => Username.unwrap(p.username).equalsIgnoreCase(u)) then Some(2)
    else None
  }

  // --- Club resolution ---

  private def selectUnresolvedClubs: ZIO[Transactor, Throwable, List[UnresolvedClub]] =
    connectZIO {
      sql"""SELECT c.club_id, c.url_name
            FROM club c
            LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
            WHERE cmr.club_id IS NULL""".query[UnresolvedClub].run().toList
    }

  private def resolveClub(
      client: ChessComClient,
      cache: Ref[Map[ClubMatchId, ApiDailyMatch]],
      club: UnresolvedClub
    ): ZIO[Transactor, Throwable, Option[ClubMatchRef]] =
    (for {
      clubMatches <- client.getWithPermit[ApiClubMatches](ApiClubMatches.getUrl(club.urlName))
      matchOpt = clubMatches.finished.headOption
      ref <- matchOpt match
        case None =>
          Console.printLine(s"  ${club.urlName}: no finished match").orDie.as(None)
        case Some(m) =>
          val matchId = ClubMatchId.wrap(m.`@id`.path.segments.last.toLong)
          for {
            dailyMatch <- fetchMatch(client, cache, matchId)
            teamIdx = findClubTeamIdx(dailyMatch, club.urlName)
            result <- teamIdx match
              case None =>
                Console.printLine(s"  ${club.urlName}: not found in match $matchId teams").orDie.as(None)
              case Some(idx) =>
                val ref = ClubMatchRef(club.clubId, matchId, idx)
                ClubMatchRef.upsert(ref).as(Some(ref))
          } yield result
    } yield ref).catchAll { error =>
      Console.printLine(s"  ${club.urlName}: error — ${error.getMessage}").orDie.as(None)
    }

  private def findClubTeamIdx(dailyMatch: ApiDailyMatch, urlName: ClubUrlName): Option[Int] = {
    val teams = dailyMatch.teams
    val name  = ClubUrlName.unwrap(urlName)
    if teams.team1.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name)) then Some(1)
    else if teams.team2.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name)) then Some(2)
    else None
  }

}
