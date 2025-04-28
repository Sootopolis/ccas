package ccas.analysis.tables.general

import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{PlayerId, Username}
import io.getquill.*
import io.getquill.extras.InstantOps
import io.getquill.jdbczio.Quill
import zio.{Tag, ZIO}

import java.sql.SQLException
import java.time.Instant

case class PlayerSnapshot(
  playerId: PlayerId,
  since   : Instant,
  username: Username,
  status  : PlayerStatusCategory,
  title   : Option[Title]
)

object PlayerSnapshot {
  inline given InsertMeta[PlayerSnapshot] = insertMeta()

  inline given UpdateMeta[PlayerSnapshot] = updateMeta(_.playerId, _.since)

  inline def selectAll: ZIO[Quill.Postgres[SnakeCase], SQLException, List[PlayerSnapshot]] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run(query[PlayerSnapshot])
    }
  }

  inline def selectAllLatest: ZIO[Quill.Postgres[SnakeCase], SQLException, List[PlayerSnapshot]] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        query[PlayerSnapshot]
          .join { query[PlayerSnapshot].groupByMap(_.playerId)(row => row.playerId -> max(row.since)) }
          .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }
          .map(_._1)
      }
    }
  }

  inline def selectAllForId(playerId: PlayerId): ZIO[Quill.Postgres[SnakeCase], SQLException, List[PlayerSnapshot]] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        query[PlayerSnapshot]
          .filter(_.playerId == lift(playerId))
      }
    }
  }

  inline def selectLatestForId(playerId: PlayerId): ZIO[Quill.Postgres[SnakeCase], SQLException, Option[PlayerSnapshot]] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        query[PlayerSnapshot]
          .filter(_.playerId == lift(playerId))
          .sortBy(_.since)(Ord.desc)
          .value
      }
    }
  }

  inline def selectAllSince(since: Instant): ZIO[Quill.Postgres[SnakeCase], SQLException, List[PlayerSnapshot]] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        val justBefore = query[PlayerSnapshot]
          .join(
            query[PlayerSnapshot]
              .filter(_._2 < lift(since))
              .groupByMap(_.playerId)(row => row.playerId -> max(row.since))
          )
          .on { case (row, (playerId, since)) => row.playerId == playerId && row.since == since }
          .map(_._1)
        val after = query[PlayerSnapshot]
          .filter(_.since >= lift(since))
        justBefore.union(after)
      }
    }
  }

  inline def insert(item: PlayerSnapshot): ZIO[Quill.Postgres[SnakeCase], SQLException, PlayerSnapshot] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        query[PlayerSnapshot]
          .insertValue(lift(item))
          .returning(x => x)
      }
    }
  }

  inline def update(item: PlayerSnapshot): ZIO[Quill.Postgres[SnakeCase], SQLException, PlayerSnapshot] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        query[PlayerSnapshot]
          .filter(_.playerId == lift(item.playerId))
          .filter(_.since == lift(item.since))
          .updateValue(lift(item))
          .returning(x => x)
      }
    }
  }

  inline def delete(key: PlayerId, since: Instant): ZIO[Quill.Postgres[SnakeCase], SQLException, PlayerSnapshot] = {
    ZIO.serviceWithZIO[Quill.Postgres[SnakeCase]] { quill =>
      import io.getquill.*
      import quill.*
      run {
        query[PlayerSnapshot]
          .filter(_.playerId == lift(key))
          .filter(_.since == lift(since))
          .delete
          .returning(x => x)
      }
    }
  }
}
