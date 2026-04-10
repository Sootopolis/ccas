package ccas.analysis.apps.clubdata

import java.time.Instant

import zio.{RIO, ZIO}

import ccas.analysis.tables.{ClubAdmin, Player}
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

object ClubAdminResolver {

  /** Resolves admin usernames to player IDs (fetching `ApiPlayer` for any usernames not already known in the `player`
    * table and inserting them), then atomically replaces the `club_admin` rows for the club via
    * [[ClubAdmin.replaceForClub]] when the resolved set differs from `existingAdminIds`. Per-username resolution
    * failures are logged and dropped from the result. Returns the resolved set of admin player IDs that were persisted.
    */
  def resolveAndPersistAdmins(
    client: ChessComClient,
    clubId: ClubId,
    adminUsernames: Set[Username],
    existingAdminIds: Set[PlayerId]
  ): RIO[CcasLogger & PostgresClient, Set[PlayerId]] =
    if (adminUsernames.isEmpty) {
      ZIO.whenDiscard(existingAdminIds.nonEmpty)(ClubAdmin.deleteByClub(clubId))
        .as(Set.empty[PlayerId])
    } else {
      for {
        knownPlayers <- Player.selectByUsernames(adminUsernames)
        knownByUsername = knownPlayers.map(p => p.username -> p.playerId).toMap
        unknownUsernames = adminUsernames -- knownByUsername.keySet

        resolvedUnknowns <- ZIO.foreach(unknownUsernames.toList) { username =>
          (for {
            apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
            player = Player(
              apiPlayer.playerId,
              apiPlayer.joinedAt,
              apiPlayer.username,
              apiPlayer.status.category,
              apiPlayer.title,
              Instant.now()
            )
            _ <- Player.insertIfNew(player)
          } yield Some(apiPlayer.username -> apiPlayer.playerId)).catchAll { error =>
            CcasLogger.info(s"[ClubAdminResolver] Could not resolve admin '$username': ${error.getMessage}")
              .as(None)
          }
        }.map(_.flatten.toMap)

        allAdminIds = (knownByUsername ++ resolvedUnknowns).values.toSet
        _ <- ZIO.whenDiscard(allAdminIds != existingAdminIds)(ClubAdmin.replaceForClub(clubId, allAdminIds))
      } yield allAdminIds
    }
}
