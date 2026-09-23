package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.ZIO
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.api.misc.subtypes.{ClubId, ClubSlug}
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
        _          <- transactZIO(ClubName.record(next.clubId, former.slug))
        holder     <- ClubName.selectCurrentHolder(former.slug)
        formerRows <- ClubName.selectClub(former.clubId)
        holders    <- ClubName.selectHolders(former.slug)
        names      <- ClubName.selectCurrentNames(holders)
      } yield assertTrue(
        holder.map(_.clubId).contains(next.clubId),
        formerRows.forall(_.until.isDefined),
        holders == List(former.clubId, next.clubId),
        names == Map(next.clubId -> former.slug)
      )
    },
    test("selectHeldSlugs answers with the names some club holds now, not the ones given up") {
      val c = club(160, "held-before")
      for {
        _     <- Club.upsert(c)
        _     <- Club.upsert(c.copy(slug = ClubSlug("held-now")))
        empty <- ClubName.selectHeldSlugs(Set.empty)
        held  <- ClubName.selectHeldSlugs(Set(ClubSlug("held-before"), ClubSlug("held-now"), ClubSlug("never-held")))
      } yield assertTrue(empty.isEmpty, held == Set(ClubSlug("held-now")))
    },
    // What the `_stale_<id>` tombstone used to stand in for (#254 step 4): the loser of a name holds none we know of
    // until we see the one it answers to, and its `club.slug` keeps the name as the display cache it now is.
    test("a club whose name another club takes is left holding none") {
      val losing = club(140, "handed-over")
      val taking = club(141, "handed-over")
      for {
        _       <- Club.upsert(losing)
        _       <- Club.upsert(taking)
        names   <- ClubName.selectClub(losing.clubId)
        current <- ClubName.selectCurrentName(losing.clubId)
        holder  <- ClubName.selectCurrentHolder(losing.slug)
      } yield assertTrue(
        names.map(_.slug) == List(losing.slug),
        names.head.until.isDefined,
        current.isEmpty,
        holder.map(_.clubId).contains(taking.clubId)
      )
    },
    test("a current name dated at or after the observation is dropped, not closed into an empty window") {
      for {
        _     <- insertRawClub(147, "clock-skewed")
        _     <- rawName(147, "clock-skewed", "2999-01-01T00:00:00Z", None)
        _     <- Club.upsert(club(147, "observed-now"))
        names <- ClubName.selectClub(ClubId(147))
      } yield assertTrue(names.map(n => (n.slug, n.until)) == List((ClubSlug("observed-now"), None)))
    },
    test("backfill opens a name for a club written without one, leaves one holding none alone, and is idempotent") {
      for {
        _        <- insertRawClub(150, "written-by-old-binary")
        first    <- ClubName.backfill
        second   <- ClubName.backfill
        live     <- ClubName.selectClub(ClubId(150))
        nameless <- ClubName.selectCurrentName(ClubId(140))
      } yield assertTrue(
        first == 1,
        second == 0,
        live.map(_.slug) == List(ClubSlug("written-by-old-binary")),
        nameless.isEmpty
      )
    },
    // The migration path itself: `club_name` lands on a database whose `club` rows already exist, and two of those can
    // share a slug now that `club_slug_key` is gone (#254 step 3b). The partial unique index picks one holder, and the
    // backfill must skip the other rather than fail the boot it runs in.
    test("backfill gives a slug two clubs share to one of them, and leaves the other holding none") {
      val shared = ClubSlug("shared-at-backfill")
      for {
        _      <- insertRawClub(190, shared.value)
        _      <- insertRawClub(191, shared.value)
        rows   <- ClubName.backfill
        first  <- ClubName.selectCurrentName(ClubId(190))
        second <- ClubName.selectCurrentName(ClubId(191))
        holder <- ClubName.selectCurrentHolder(shared)
      } yield assertTrue(
        rows == 1,
        List(first, second).flatten == List(shared),
        holder.map(_.clubId).exists(Set(ClubId(190), ClubId(191)).contains)
      )
    },
    // Two clubs are only ever observed holding one name because one observation is stale, so `record` leaves the
    // database to refuse the loser rather than serialising the two (there is no lock across clubs). Whatever the
    // interleaving, the name ends with exactly one current holder and any failure names the index that said so.
    test("two clubs claiming one name at once leave exactly one holder") {
      val contested = ClubSlug("claimed-at-once")
      for {
        _       <- Club.upsert(club(200, "claims-first"))
        _       <- Club.upsert(club(201, "claims-second"))
        results <- ZIO.foreachPar(List(200L, 201L))(id => Club.upsert(club(id, contested.value)).either)
        open    <- openHolders(contested)
        holder  <- ClubName.selectCurrentHolder(contested)
      } yield assertTrue(
        open == 1,
        results.exists(_.isRight),
        results.collect { case Left(error) => error }.forall(violatesConstraint(_, "club_name_current")),
        holder.map(_.clubId).exists(Set(ClubId(200), ClubId(201)).contains)
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
    result.left.exists(violatesConstraint(_, constraint))

  private def violatesConstraint(error: SQLException, constraint: String): Boolean =
    Option(error.getMessage).exists(_.contains(constraint))

  private def openHolders(slug: ClubSlug): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"SELECT count(*) FROM club_name WHERE slug = $slug AND until IS NULL".query[Int].run().head
    }

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
