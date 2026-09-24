package ccas.analysis.apps.recruitment

import java.time.{Instant, ZoneOffset}

import zio.{Clock, RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub, PlayerUpdater, UsernameRenameResolver}
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{BodyStore, ChessComClient, HttpClientLayer}
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
            club <- resolve(clubStr)
            now  <- Clock.instant
            expiresAt = months.map(m => now.atZone(ZoneOffset.UTC).plusMonths(m.toLong).toInstant)
            _ <- addToBlacklist(club = club, usernames = usernames, reason = rest.headOption, expiresAt = expiresAt)
          } yield ()
        case "list" :: clubStr :: _ =>
          resolve(clubStr).flatMap(listBlacklist)
        case "remove" :: clubStr :: usernameStr :: _ =>
          resolve(clubStr).flatMap(removeFromBlacklist(_, Username.wrap(usernameStr)))
        case _ => ZIO.fail(BadRequestException(help))
      }
    } yield ()).provideSomeAuto(
      ProgressDisplay.live(showProgress = true),
      ChessComClient.live("blacklist"),
      HttpClientLayer.live,
      BodyStore.live,
      PostgresClient.live(onInit = Tables.ensureTablesOnInit)
    )

  private def resolve(clubStr: String): RIO[ChessComClient & PostgresClient, NamedClub] =
    ClubResolution.resolveRunnable(ClubQuery.BySlug(ClubSlug.wrap(clubStr)))

  /** Blacklists each player for a club already resolved, answering with the usernames as blacklisted — a renamed
    * player under their current name.
    */
  def addToBlacklist(
    club: NamedClub,
    usernames: List[Username],
    reason: Option[String],
    expiresAt: Option[Instant]
  ): RIO[ChessComClient & PostgresClient, List[Username]] =
    for {
      client <- ZIO.service[ChessComClient]
      blacklisted <- ZIO.foreach(usernames) { username =>
        for {
          apiPlayer <- UsernameRenameResolver.fetchOrRecover(client, username)
          now       <- Clock.instant
          // Single transaction: reconcile (handles rename archival or fresh insert) + blacklist upsert. Resolver's
          // verification fetch already authenticated apiPlayer; we don't double-reconcile.
          _ <- withTransaction {
            PlayerUpdater.reconcile(apiPlayer, client) *> RecruitmentBlacklist.upsert(
              RecruitmentBlacklist(club.clubId, apiPlayer.playerId, now, expiresAt, reason)
            )
          }
          _ <- ZIO.whenDiscard(apiPlayer.username != username) {
            ZIO.logInfo(s"  Renamed: input '$username' resolved to '${apiPlayer.username}'")
          }
          _ <- ZIO.logInfo(
            s"Blacklisted ${apiPlayer.username} (player_id=${apiPlayer.playerId}) for club ${club.slug}"
          )
        } yield apiPlayer.username
      }
    } yield blacklisted

  private def listBlacklist(club: NamedClub): RIO[PostgresClient, Unit] =
    for {
      now     <- Clock.instant
      entries <- RecruitmentBlacklist.selectActiveByClub(club.clubId, now)
      _ <-
        if (entries.isEmpty) {
          ZIO.logInfo(s"No active blacklist entries for ${club.slug}")
        } else {
          ZIO.foreachDiscard(entries) { e =>
            val name    = e.username.fold(s"player_id=${e.playerId}")(_.toString)
            val expires = e.expiresAt.fold("indefinite")(t => s"expires $t")
            val reason  = e.reason.fold("")(r => s" reason=$r")
            ZIO.logInfo(s"  $name  $expires$reason")
          }
        }
    } yield ()

  /** Answers whether the player was blacklisted for the club. */
  def removeFromBlacklist(club: NamedClub, username: Username): RIO[PostgresClient, Boolean] =
    for {
      playerId <- PlayerName.selectCurrentHolder(username).someOrFail(NotFoundException(s"Player not found: $username"))
      rows     <- RecruitmentBlacklist.delete(club.clubId, playerId)
      _ <- ZIO.logInfo(
        if (rows > 0) s"Removed $username from blacklist for ${club.slug}"
        else s"$username was not blacklisted for ${club.slug}"
      )
    } yield rows > 0
}
