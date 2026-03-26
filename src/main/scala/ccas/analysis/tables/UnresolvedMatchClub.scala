package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubMatchId, ClubSlug}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

object UnresolvedMatchClub {
  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS unresolved_match_club (
              match_id   BIGINT NOT NULL,
              is_team1   BOOLEAN NOT NULL,
              slug       VARCHAR NOT NULL,
              first_seen TIMESTAMPTZ NOT NULL,
              PRIMARY KEY (match_id, is_team1)
            )""".update.run()
    }

  def insert(matchId: ClubMatchId, isTeam1: Boolean, slug: ClubSlug): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO unresolved_match_club (match_id, is_team1, slug, first_seen)
            VALUES ($matchId, $isTeam1, $slug, ${Instant.now()})
            ON CONFLICT (match_id, is_team1) DO NOTHING""".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[(ClubMatchId, Boolean, ClubSlug)]] =
    connectZIO {
      sql"SELECT match_id, is_team1, slug FROM unresolved_match_club"
        .query[(ClubMatchId, Boolean, ClubSlug)].run().toList
    }

  def delete(matchId: ClubMatchId, isTeam1: Boolean): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM unresolved_match_club WHERE match_id = $matchId AND is_team1 = $isTeam1"
        .update.run()
    }
}
