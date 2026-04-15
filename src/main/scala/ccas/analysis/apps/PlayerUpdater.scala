package ccas.analysis.apps

import java.time.Instant

import ccas.utils.sql.PostgresClient
import zio.{RIO, ZIO}

import ccas.analysis.tables.{Player, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.Title
import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayer
import ccas.utils.client.ChessComClient

/** Shared helper for updating a player's current state with archive-to-snapshot and username conflict resolution. Must
  * be called within `withTransaction`.
  */
object PlayerUpdater {

  def archiveAndUpdate(
    existing: Player,
    newUsername: Username,
    newStatus: PlayerStatusCategory,
    newTitle: Option[Title],
    since: Instant,
    client: ChessComClient
  ): RIO[PostgresClient, Int] = {
    val resolveConflict = if (newUsername != existing.username) {
      Player.selectByUsernameForUpdate(newUsername).flatMap {
        case Some(conflicting) =>
          client.get[ApiPlayer](ApiPlayer.getUrl(conflicting.username)).flatMap { apiPlayer =>
            if (!conflicting.stateMatches(apiPlayer.username, apiPlayer.status.category, apiPlayer.title)) {
              archiveAndUpdate(
                conflicting,
                apiPlayer.username,
                apiPlayer.status.category,
                apiPlayer.title,
                Instant.now(),
                client
              )
            } else {
              ZIO.succeed(0)
            }
          }
        case None => ZIO.succeed(0)
      }
    } else { ZIO.succeed(0) }

    val updated = existing.copy(username = newUsername, status = newStatus, title = newTitle, since = since)
    resolveConflict *> PlayerSnapshot.insert(existing.toSnapshot) *> Player.updateCurrentState(updated)
  }

  /** Reconciles a freshly-fetched `ApiPlayer` against the `player` table by `player_id`. If an existing row is present
    * and has drifted (username/status/title), archives the prior state to `player_snapshot` and updates via
    * [[archiveAndUpdate]]. Otherwise inserts a new row with `since = now` for active accounts or `since = lastOnline`
    * for non-active ones. Must be called within `withTransaction`. Returns `true` only when a brand-new row was
    * inserted (never on update, no-op, or insert-on-conflict no-op).
    */
  def reconcile(
    apiPlayer: ApiPlayer,
    client: ChessComClient
  ): RIO[PostgresClient, Boolean] = {
    val now            = Instant.now()
    val statusCategory = apiPlayer.status.category
    Player.selectIdForUpdate(apiPlayer.playerId).flatMap {
      case Some(existing) =>
        ZIO.whenDiscard(!existing.stateMatches(apiPlayer.username, statusCategory, apiPlayer.title)) {
          archiveAndUpdate(existing, apiPlayer.username, statusCategory, apiPlayer.title, now, client)
        }.as(false)

      case None =>
        val since = if (statusCategory == PlayerStatusCategory.Active) { now } else { apiPlayer.lastOnlineAt }
        Player.insertIfNew(
          Player(apiPlayer.playerId, apiPlayer.joinedAt, apiPlayer.username, statusCategory, apiPlayer.title, since)
        ).map(_ > 0)
    }
  }
}
