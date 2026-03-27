package ccas.analysis.apps.recruitment

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.{Console, RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.*
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.DataSourceLayer

object BlacklistApp extends ZIOAppDefault {
  private val help = "Usage: BlacklistApp <club-slug> <username> [reason] [expires-at]"

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      _ <- args.toList match {
        case clubStr :: usernameStr :: rest =>
          addToBlacklist(
            ClubSlug.wrap(clubStr),
            Username.wrap(usernameStr),
            reason = rest.headOption,
            expiresAt = rest.lift(1).map(Instant.parse)
          )
        case _ => ZIO.fail(BadRequestException(help))
      }
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = true),
      ChessComClient.live,
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  def addToBlacklist(
    clubSlug: ClubSlug,
    username: Username,
    reason: Option[String],
    expiresAt: Option[Instant]
  ): RIO[ChessComClient & Transactor, Unit] =
    for {
      client  <- ZIO.service[ChessComClient]
      apiClub <- ApiClub.get(client, clubSlug)
      club = Club(apiClub.clubId, Instant.ofEpochSecond(apiClub.created), clubSlug, apiClub.name)
      _         <- Club.upsert(club)
      apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
      now = Instant.now()
      _ <- RecruitmentBlacklist.insert(
        RecruitmentBlacklist(apiClub.clubId, apiPlayer.playerId, now, expiresAt, reason)
      )
      _ <- Console.printLine(
        s"Blacklisted $username (player_id=${apiPlayer.playerId}) for club $clubSlug"
      ).orDie
    } yield ()
}
