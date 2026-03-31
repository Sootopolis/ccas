package ccas.analysis.apps

import java.time.Instant

import com.augustnagro.magnum.Transactor
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
  ): RIO[Transactor, Int] = {
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
}
