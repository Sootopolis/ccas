package ccas.analysis.apps.recruitment

import java.time.{Instant, ZoneOffset}

import zio.{Clock, RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.PlayerUpdater
import ccas.analysis.tables.*
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.ProgressDisplay
import ccas.utils.client.{ChessComClient, HttpClientLayer}
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

object BlacklistApp extends ZIOAppDefault {
  private val help =
    """Usage: BlacklistApp <command> [args]
      |
      |Commands:
      |  add <club-slug> <user1,user2,...> [reason] [months]   Add players to blacklist (indefinite if months omitted)
      |  list <club-slug>                                       List active blacklist entries
      |  remove <club-slug> <username>                          Remove player from blacklist""".stripMargin

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      _ <- args.toList match {
        case "add" :: clubStr :: usernamesStr :: rest =>
          val usernames = usernamesStr.split(',').map(s => Username.wrap(s.trim)).toList
          val months    = rest.lift(1).map(_.toInt)
          for {
            now <- Clock.instant
            expiresAt = months.map(m => now.atZone(ZoneOffset.UTC).plusMonths(m.toLong).toInstant)
            _ <- addToBlacklist(ClubSlug.wrap(clubStr), usernames, reason = rest.headOption, expiresAt)
          } yield ()
        case "list" :: clubStr :: _ =>
          listBlacklist(ClubSlug.wrap(clubStr))
        case "remove" :: clubStr :: usernameStr :: _ =>
          removeFromBlacklist(ClubSlug.wrap(clubStr), Username.wrap(usernameStr))
        case _ => ZIO.fail(BadRequestException(help))
      }
    } yield ()).provideSomeAuto(
      ProgressDisplay.live(showProgress = true),
      ChessComClient.live("blacklist"),
      HttpClientLayer.live,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  def addToBlacklist(
    clubSlug: ClubSlug,
    usernames: List[Username],
    reason: Option[String],
    expiresAt: Option[Instant]
  ): RIO[ChessComClient & PostgresClient, Unit] =
    for {
      client  <- ZIO.service[ChessComClient]
      apiClub <- ApiClub.get(client, clubSlug)
      club = Club.fromApi(apiClub, clubSlug)
      _ <- Club.upsertResolvingSlugConflict(club, client)
      _ <- ZIO.foreachDiscard(usernames) { username =>
        for {
          apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
          now       <- Clock.instant
          _ <- withTransaction {
            PlayerUpdater.reconcile(apiPlayer, client) *> RecruitmentBlacklist.upsert(
              RecruitmentBlacklist(apiClub.clubId, apiPlayer.playerId, now, expiresAt, reason)
            )
          }
          _ <- ZIO.logInfo(
            s"Blacklisted $username (player_id=${apiPlayer.playerId}) for club $clubSlug"
          )
        } yield ()
      }
    } yield ()

  private def listBlacklist(clubSlug: ClubSlug): RIO[PostgresClient, Unit] =
    for {
      club    <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      now     <- Clock.instant
      entries <- RecruitmentBlacklist.selectActiveByClub(club.clubId, now)
      _ <-
        if (entries.isEmpty) {
          ZIO.logInfo(s"No active blacklist entries for $clubSlug")
        } else {
          ZIO.foreachDiscard(entries) { e =>
            val name    = e.username.fold(s"player_id=${e.playerId}")(_.toString)
            val expires = e.expiresAt.fold("indefinite")(t => s"expires $t")
            val reason  = e.reason.fold("")(r => s" reason=$r")
            ZIO.logInfo(s"  $name  $expires$reason")
          }
        }
    } yield ()

  def removeFromBlacklist(clubSlug: ClubSlug, username: Username): RIO[PostgresClient, Unit] =
    for {
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      ps   <- Player.selectByUsername(username).someOrFail(NotFoundException(s"Player not found: $username"))
      rows <- RecruitmentBlacklist.delete(club.clubId, ps.playerId)
      _ <- ZIO.logInfo(
        if (rows > 0) s"Removed $username from blacklist for $clubSlug"
        else s"$username was not blacklisted for $clubSlug"
      )
    } yield ()
}
