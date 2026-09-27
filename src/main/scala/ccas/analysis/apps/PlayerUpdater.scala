package ccas.analysis.apps

import java.sql.SQLException
import java.time.Instant

import zio.ZIO

import ccas.analysis.tables.{Player, PlayerName, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.Title
import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayer
import ccas.utils.sql.PostgresClient

/** Shared helper for updating a player's current state, archiving the prior one to `player_snapshot`. Must be called
  * within `withTransaction`.
  */
object PlayerUpdater {

  // A pure rename archives a snapshot too: the update moves `since`, and the snapshot is what keeps when the prior
  // status began.
  private[apps] def archiveAndUpdate(
    existing: Player,
    newUsername: Username,
    newStatus: PlayerStatusCategory,
    newTitle: Option[Title],
    since: Instant
  ): ZIO[PostgresClient, SQLException, Int] = {
    val updated = existing.copy(username = newUsername, status = newStatus, title = newTitle, since = since)
    PlayerSnapshot.insert(existing.toSnapshot) *> Player.updateCurrentState(updated)
  }

  /** [[archiveAndUpdate]], when an observation differs from what we hold for `existing` ([[Player.stateMatches]]). */
  def updateIfDrifted(
    existing: Player,
    newUsername: Username,
    newStatus: PlayerStatusCategory,
    newTitle: Option[Title],
    since: Instant
  ): ZIO[PostgresClient, SQLException, Unit] =
    ZIO.whenZIODiscard(
      PlayerName.selectCurrentName(existing.playerId).map(existing.stateMatches(_, newUsername, newStatus, newTitle))
        .negate
    )(archiveAndUpdate(existing, newUsername, newStatus, newTitle, since))

  /** Reconciles a freshly-fetched `ApiPlayer` against the `player` table by `player_id`. An existing row goes through
    * [[updateIfDrifted]], archived to `player_snapshot` and updated only when the observation differs from what we
    * hold. Otherwise inserts a new row with `since = now` for active accounts or `since = lastOnline` for non-active
    * ones. Must be called within `withTransaction`. Returns `true` only when a brand-new row was inserted (never on
    * update, no-op, or insert-on-conflict no-op).
    */
  def reconcile(apiPlayer: ApiPlayer): ZIO[PostgresClient, SQLException, Boolean] = {
    val now            = Instant.now()
    val statusCategory = apiPlayer.status.category
    Player.selectIdForUpdate(apiPlayer.playerId).flatMap {
      case Some(existing) =>
        updateIfDrifted(existing, apiPlayer.username, statusCategory, apiPlayer.title, now).as(false)

      case None =>
        val since = if (statusCategory == PlayerStatusCategory.Active) { now } else { apiPlayer.lastOnlineAt }
        Player.insertIfNew(
          Player(apiPlayer.playerId, apiPlayer.joinedAt, apiPlayer.username, statusCategory, apiPlayer.title, since)
        ).map(_ > 0)
    }
  }
}
