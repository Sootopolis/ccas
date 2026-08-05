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
    val resolveConflict: RIO[PostgresClient, Unit] =
      ZIO.whenDiscard(newUsername != existing.username) {
        Player.selectByUsernameForUpdate(newUsername).flatMap {
          case None => ZIO.unit
          case Some(conflicting) =>
            for {
              apiPlayer <- client.getUncached[ApiPlayer](ApiPlayer.getUrl(conflicting.username))
              _ <-
                if (apiPlayer.playerId != conflicting.playerId) {
                  // Recycled handle: the API now serves the conflicting username for a different player. Our
                  // `conflicting` row is stale — the rightful holder renamed away. Tombstone the row to free the
                  // UNIQUE(username) slot. The deferred constraint allows this update plus the caller's update to
                  // satisfy the constraint at commit time. The renamed player will be rediscovered organically when
                  // their new name surfaces on a board / club roster / direct fetch.
                  tombstoneConflicting(conflicting, Instant.now()).unit
                } else if (!conflicting.stateMatches(apiPlayer.username, apiPlayer.status.category, apiPlayer.title)) {
                  // The recursive `since` is a fresh now(), not the caller's `since`: the conflicting player's
                  // state-change is a different event than the caller's, observed at the moment we discover it.
                  archiveAndUpdate(
                    conflicting,
                    apiPlayer.username,
                    apiPlayer.status.category,
                    apiPlayer.title,
                    Instant.now(),
                    client
                  ).unit
                } else {
                  ZIO.unit
                }
            } yield ()
        }
      }

    val updated = existing.copy(username = newUsername, status = newStatus, title = newTitle, since = since)
    resolveConflict *> PlayerSnapshot.insert(existing.toSnapshot) *> Player.updateCurrentState(updated)
  }

  /** Archives the conflicting Player's prior state and rewrites its `username` to a sentinel (`_stale_<playerId>`)
    * so the UNIQUE(username) slot is freed. Used when the conflicting username's API-served playerId no longer
    * matches our row — the rightful holder has been renamed away. The tombstone is replaced when the renamed
    * player is rediscovered under their new handle through any normal-path callsite (HistoryApp, MembershipApp,
    * RefApp, etc.).
    *
    * Defensive `since`: `Player.updateCurrentState` has an optimistic `AND since < newSince` guard. If `since`
    * coincides with `conflicting.since` (sub-microsecond clock under tests, very fast retry), the UPDATE no-ops and
    * the tombstone never lands — the caller's UPDATE then violates UNIQUE at commit. Bump by 1µs (Postgres
    * TIMESTAMPTZ resolution) when the proposed `since` isn't strictly later than the existing row.
    */
  private def tombstoneConflicting(conflicting: Player, since: Instant): RIO[PostgresClient, Int] = {
    val tombstone = UsernameRenameResolver.stalePlaceholder(conflicting.playerId)
    val effectiveSince =
      if (since.isAfter(conflicting.since)) { since }
      else { conflicting.since.plus(1L, java.time.temporal.ChronoUnit.MICROS) }
    PlayerSnapshot.insert(conflicting.toSnapshot) *>
      Player.updateCurrentState(conflicting.copy(username = tombstone, since = effectiveSince)).flatMap { rows =>
        if (rows == 1) { ZIO.succeed(rows) }
        else {
          ZIO.fail(new IllegalStateException(
            s"Tombstone update for player_id=${conflicting.playerId} affected $rows rows; expected 1. " +
              s"conflicting.since=${conflicting.since} since=$since effectiveSince=$effectiveSince"
          ))
        }
      }
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
