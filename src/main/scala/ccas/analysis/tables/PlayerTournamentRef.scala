package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{PlayerId, TournamentSlug}
import ccas.utils.sql.SqlZioTypes.connectZIO

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class PlayerTournamentRef(
  @Id playerId: PlayerId,
  tournamentSlug: TournamentSlug,
  playerIdx: Int
) derives DbCodec

object PlayerTournamentRef {
  private val repo = ImmutableRepo[PlayerTournamentRef, PlayerId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_tournament_ref (
              player_id        BIGINT PRIMARY KEY REFERENCES player (player_id),
              tournament_slug  VARCHAR NOT NULL,
              player_idx       INT NOT NULL
            )""".update.run()
    }

  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, Option[PlayerTournamentRef]] =
    connectZIO(repo.findById(playerId))

  def upsert(ref: PlayerTournamentRef): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player_tournament_ref (player_id, tournament_slug, player_idx)
            VALUES (${ref.playerId}, ${ref.tournamentSlug}, ${ref.playerIdx})
            ON CONFLICT (player_id) DO UPDATE SET tournament_slug = EXCLUDED.tournament_slug, player_idx = EXCLUDED.player_idx""".update.run()
    }

  def deleteId(playerId: PlayerId): ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_tournament_ref WHERE player_id = $playerId".update.run())

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM player_tournament_ref".update.run())
}
