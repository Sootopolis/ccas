package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.ZIO
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** `club_name` is kept in step with every write of `club.slug`, and its constraints hold the current-slice invariants
  * ADR 0016 puts in the database. Each test uses its own club ids, so the sequential suite shares one schema.
  */
object TestClubNameSql extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private def club(id: Long, slug: String): Club = Club(ClubId(id), t0, ClubSlug(slug), s"Club $id", None, None, None)

  override def spec: Spec[Any, Throwable] = suite("TestClubNameSql")(
    test("an upsert opens a current name, and re-upserting the same slug opens no second row") {
      val c = club(100, "first-club")
      for {
        _     <- Club.upsert(c)
        _     <- Club.upsert(c.copy(name = "Renamed display name"))
        names <- ClubName.selectClub(c.clubId)
      } yield assertTrue(names.map(n => (n.slug, n.until)) == List((c.slug, None)))
    },
    test("a rename closes the old name and opens the new one at the same instant") {
      val c = club(110, "before-rename")
      for {
        _     <- Club.upsert(c)
        _     <- Club.upsert(c.copy(slug = ClubSlug("after-rename")))
        names <- ClubName.selectClub(c.clubId)
      } yield assertTrue(
        names.map(_.slug) == List(ClubSlug("before-rename"), ClubSlug("after-rename")),
        names.head.until.contains(names(1).since),
        names(1).until.isEmpty
      )
    },
    test("recording a name held by another club closes that club's current name") {
      val former = club(130, "moving-name")
      val next   = club(131, "next-club")
      for {
        _          <- Club.upsert(former)
        _          <- Club.upsert(next)
        _          <- transactZIO(ClubName.record(next.clubId, Some(former.slug)))
        holder     <- ClubName.selectCurrentHolder(former.slug)
        formerRows <- ClubName.selectClub(former.clubId)
        holders    <- ClubName.selectHolders(former.slug)
      } yield assertTrue(
        holder.map(_.clubId).contains(next.clubId),
        formerRows.forall(_.until.isDefined),
        holders.map(_.clubId) == List(former.clubId, next.clubId)
      )
    },
    test("a tombstone closes the current name and opens none") {
      val c = club(140, "about-to-go-stale")
      for {
        _     <- Club.upsert(c)
        _     <- Club.upsert(c.copy(slug = ClubSlug("_stale_140")))
        names <- ClubName.selectClub(c.clubId)
      } yield assertTrue(names.map(_.slug) == List(c.slug), names.head.until.isDefined)
    },
    test("a current name dated at or after the observation is dropped, not closed into an empty window") {
      for {
        _     <- insertRawClub(147, "clock-skewed")
        _     <- rawName(147, "clock-skewed", "2999-01-01T00:00:00Z", None)
        _     <- Club.upsert(club(147, "observed-now"))
        names <- ClubName.selectClub(ClubId(147))
      } yield assertTrue(names.map(n => (n.slug, n.until)) == List((ClubSlug("observed-now"), None)))
    },
    test("a slug conflict moves the old holder to the name its match ref reports, and hands the slug on") {
      val stale    = club(145, "contested")
      val incoming = club(146, "contested")
      val matchJson = apiDailyMatchJson(
        matchId = 9700L,
        team1Club = "contested-renamed",
        team2Club = "someone-else",
        team1Players = List(("p1", 1)),
        team2Players = List(("p2", 1))
      )
      val routes = Routes(
        Method.GET / "pub" / "match" / long("matchId") -> handler((_: Long, _: Request) => Response.json(matchJson))
      )
      for {
        _        <- Club.upsert(stale)
        _        <- ClubMatchRef.insert(ClubMatchRef(stale.clubId, ClubMatchId(9700), isLive = false, isTeam1 = true))
        client   <- TestChessComClientSupport.fakeClient(routes)
        _        <- Club.upsertResolvingSlugConflict(incoming, client)
        contested <- ClubName.selectCurrentHolder(ClubSlug("contested"))
        renamed   <- ClubName.selectCurrentHolder(ClubSlug("contested-renamed"))
      } yield assertTrue(
        contested.map(_.clubId).contains(incoming.clubId),
        renamed.map(_.clubId).contains(stale.clubId)
      )
    },
    test("backfill opens a name for a club written without one, skips tombstones, and is idempotent") {
      for {
        _      <- insertRawClub(150, "written-by-old-binary")
        _      <- insertRawClub(151, "_stale_151")
        first  <- ClubName.backfill
        second <- ClubName.backfill
        live   <- ClubName.selectClub(ClubId(150))
        stale  <- ClubName.selectClub(ClubId(151))
      } yield assertTrue(
        first == 1,
        second == 0,
        live.map(_.slug) == List(ClubSlug("written-by-old-binary")),
        stale.isEmpty
      )
    },
    test("the exclusion constraint rejects two open names for one club") {
      for {
        _    <- Club.upsert(club(160, "open-one"))
        exit <- rawName(160, "open-two", "2030-01-01T00:00:00Z", None).either
      } yield assertTrue(violates(exit, "club_name_no_overlap"))
    },
    test("the partial unique index rejects two current holders of one name") {
      for {
        _    <- Club.upsert(club(170, "held-once"))
        _    <- Club.upsert(club(171, "other-name"))
        _    <- connectZIO(sql"DELETE FROM club_name WHERE club_id = ${ClubId(171)}".update.run())
        exit <- rawName(171, "held-once", "2030-01-01T00:00:00Z", None).either
      } yield assertTrue(violates(exit, "club_name_current"))
    },
    test("the CHECK rejects an empty window") {
      for {
        _    <- insertRawClub(180, "empty-window")
        exit <- rawName(180, "empty-window", "2030-01-01T00:00:00Z", Some("2030-01-01T00:00:00Z")).either
      } yield assertTrue(violates(exit, "club_name_window"))
    }
  ).provideShared(FreshSchemaLayer("test_club_name_sql", onInit = Tables.ensureTables)) @@ TestAspect.sequential

  // Names the constraint, so a failure for any other reason (a codec, a missing club row) cannot pass for it.
  private def violates(result: Either[SQLException, Int], constraint: String): Boolean =
    result.left.exists(e => Option(e.getMessage).exists(_.contains(constraint)))

  private def insertRawClub(id: Long, slug: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club (club_id, created, slug, name)
            VALUES (${ClubId(id)}, $t0, ${ClubSlug(slug)}, ${s"Club $id"})""".update.run()
    }

  private def rawName(
    id: Long,
    slug: String,
    since: String,
    until: Option[String]
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val sinceAt = Instant.parse(since)
      val untilAt = until.map(Instant.parse)
      sql"""INSERT INTO club_name (club_id, slug, since, until)
            VALUES (${ClubId(id)}, ${ClubSlug(slug)}, $sinceAt, $untilAt)""".update.run()
    }
}
