package ccas.analysis.apps.recruitment

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.{Console, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.*
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubUrlName, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer

object BlacklistApp extends ZIOAppDefault {

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- (args.toList match
        case clubStr :: usernameStr :: rest =>
          addToBlacklist(
            ClubUrlName.wrap(clubStr),
            Username.wrap(usernameStr),
            reason = rest.headOption,
            expiresAt = rest.lift(1).map(Instant.parse)
          )
        case _ =>
          ZIO.fail(
            ExternalException(
              "Usage: BlacklistApp <club-url-name> <username> [reason] [expires-at]"
            )
          )
      ).provide(
        ChessComClient.live(),
        Client.default,
        DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
      )
    } yield ()

  def addToBlacklist(
      clubUrlName: ClubUrlName,
      username: Username,
      reason: Option[String],
      expiresAt: Option[Instant]
    ): ZIO[ChessComClient & Transactor, Throwable, Unit] =
    for {
      client    <- ZIO.service[ChessComClient]
      apiClub   <- ApiClub.get(client, clubUrlName)
      club       = Club(apiClub.clubId, Instant.ofEpochSecond(apiClub.created), clubUrlName)
      _         <- Club.upsert(club)
      apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
      now        = Instant.now()
      _         <- RecruitmentBlacklist.insert(
                     RecruitmentBlacklist(apiClub.clubId, apiPlayer.playerId, now, expiresAt, reason)
                   )
      _         <- Console.printLine(
                     s"Blacklisted ${username} (player_id=${apiPlayer.playerId}) for club ${clubUrlName}"
                   ).orDie
    } yield ()
}
