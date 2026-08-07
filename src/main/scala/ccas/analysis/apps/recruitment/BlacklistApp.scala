package ccas.analysis.apps.recruitment

import java.time.{Instant, ZoneOffset}

import zio.{Clock, RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.{PlayerUpdater, UsernameRenameResolver, withClubSlugRenameRecovery}
import ccas.analysis.tables.*
import ccas.api.club.ApiClub
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
      BodyStore.live,
      PostgresClient.live(onInit = Tables.ensureTablesOnInit)
    )

  def addToBlacklist(
    clubSlug: ClubSlug,
    usernames: List[Username],
    reason: Option[String],
    expiresAt: Option[Instant]
  ): RIO[ChessComClient & PostgresClient, Unit] =
    for {
      client <- ZIO.service[ChessComClient]
      // Recover the canonical slug if the user typed a stale handle. Tier B works only if a Club row already exists
      // under the stale slug (resolver derives clubIdHint via DB) — first-time blacklisting against a never-seen
      // stale slug still 404s, which is the correct behaviour: nothing in our DB knows what they meant.
      apiClub <- ApiClub.get(client, clubSlug)
        .withClubSlugRenameRecovery(client, clubSlug, clubIdHint = None)(fresh => ApiClub.get(client, fresh))
      effectiveSlug = ClubSlug.wrap(apiClub.`@id`.path.segments.last)
      club          = Club.fromApi(apiClub, effectiveSlug)
      // On the recovery path the resolver already upserted under the canonical slug; this is an idempotent
      // reaffirmation. On the no-recovery happy path, this is the source-of-truth write.
      _ <- Club.upsertResolvingSlugConflict(club, client)
      _ <- ZIO.foreachDiscard(usernames) { username =>
        for {
          apiPlayer <- UsernameRenameResolver.fetchOrRecover(client, username)
          now       <- Clock.instant
          // Single transaction: reconcile (handles rename archival or fresh insert) + blacklist upsert. Resolver's
          // verification fetch already authenticated apiPlayer; we don't double-reconcile.
          _ <- withTransaction {
            PlayerUpdater.reconcile(apiPlayer, client) *> RecruitmentBlacklist.upsert(
              RecruitmentBlacklist(apiClub.clubId, apiPlayer.playerId, now, expiresAt, reason)
            )
          }
          _ <- ZIO.whenDiscard(apiPlayer.username != username) {
            ZIO.logInfo(s"  Renamed: input '$username' resolved to '${apiPlayer.username}'")
          }
          _ <- ZIO.logInfo(
            s"Blacklisted ${apiPlayer.username} (player_id=${apiPlayer.playerId}) for club $clubSlug"
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
