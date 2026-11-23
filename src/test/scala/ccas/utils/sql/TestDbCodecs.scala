package ccas.utils.sql

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.http.URL
import zio.test.{assertTrue, typeCheck, Spec, ZIOSpecDefault}

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.connectZIO

/** Our codecs read SQL NULL through Magnum's `readSingleOption`, never as `Some(null)` (#536), and a list binds only
  * element types stored as their raw value (#538). Each test selects a literal, so the suite needs no tables.
  */
object TestDbCodecs extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestDbCodecs")(
    testAbsentOpaqueString,
    testAbsentEnum,
    testAbsentUrl,
    testAbsentTimestamp,
    testAbsentOpaqueNumber,
    testAbsentArray,
    testPresentValuesReadBack,
    testNullIntoNonOptionFails,
    testBareEnumCaseIsBound,
    testListArrayTypes,
    testListsReadBack,
    testUnencodableListsDoNotCompile
  ).provideShared(FreshSchemaLayer("test_db_codecs"))

  private def testAbsentOpaqueString = test("an absent opaque string reads as None") {
    connectZIO(sql"SELECT NULL::text".query[Option[Username]].run()).map(rows => assertTrue(rows == Vector(None)))
  }

  private def testAbsentEnum = test("an absent enum reads as None") {
    connectZIO(sql"SELECT NULL::text".query[Option[RunTrigger]].run()).map(rows => assertTrue(rows == Vector(None)))
  }

  private def testAbsentUrl = test("an absent URL reads as None") {
    connectZIO(sql"SELECT NULL::text".query[Option[URL]].run()).map(rows => assertTrue(rows == Vector(None)))
  }

  private def testAbsentTimestamp = test("an absent timestamp reads as None") {
    connectZIO(sql"SELECT NULL::timestamptz".query[Option[Instant]].run()).map(rows => assertTrue(rows == Vector(None)))
  }

  private def testAbsentOpaqueNumber = test("an absent opaque number reads as None, never as a validated zero") {
    connectZIO(sql"SELECT NULL::bigint".query[Option[PlayerId]].run()).map(rows => assertTrue(rows == Vector(None)))
  }

  private def testAbsentArray = test("an absent array reads as None") {
    connectZIO(sql"SELECT NULL::text[]".query[Option[List[String]]].run()).map(rows => assertTrue(rows == Vector(None)))
  }

  private def testPresentValuesReadBack = test("a present value of each codec reads back as itself") {
    val username = Username("Someone")
    val trigger  = RunTrigger.Scheduled
    val url      = URL.decode("https://www.chess.com/club/some-club").toOption.get
    val instant  = Instant.parse("2026-11-23T12:34:56Z")
    val playerId = PlayerId(42L)
    val names    = List("a", "b")
    connectZIO {
      sql"SELECT $username, $trigger, $url, $instant, $playerId, $names"
        .query[
          (Option[Username], Option[RunTrigger], Option[URL], Option[Instant], Option[PlayerId], Option[List[String]])
        ]
        .run()
    }.map { rows =>
      assertTrue(rows == Vector((Some(username), Some(trigger), Some(url), Some(instant), Some(playerId), Some(names))))
    }
  }

  // Before Magnum 2.0 the codec passed the NULL through and the field held `null`; a number still reads as 0, as JDBC
  // gives it. The query itself succeeds, so the failure is the decode's: a defect, not a SQL error.
  private def testNullIntoNonOptionFails =
    test("a NULL read into a validated name that is not an Option fails the read") {
      connectZIO(sql"SELECT NULL::text".query[Username].run()).exit.map { exit =>
        val defect = exit.causeOption.flatMap(_.dieOption)
        assertTrue(defect.exists(_.getCause.isInstanceOf[NullPointerException]))
      }
    }

  private def testBareEnumCaseIsBound = test("a bare enum case interpolates as a bound parameter") {
    val frag = sql"SELECT ${RunTrigger.Dependency}"
    connectZIO(frag.query[RunTrigger].run()).map { rows =>
      assertTrue(frag.sqlString == "SELECT ?", rows == Vector(RunTrigger.Dependency))
    }
  }

  private val strings   = List("a", "b")
  private val longs     = List(1L, 2L)
  private val ints      = List(1, 2)
  private val usernames = List(Username("Someone"), Username("Else"))
  private val playerIds = List(PlayerId(42L), PlayerId(43L))

  private def testListArrayTypes =
    test(
      "a list binds as an array of its element's column type: text, bigint, integer, opaque string and opaque number"
    ) {
      connectZIO {
        sql"""SELECT pg_typeof($strings)::text, pg_typeof($longs)::text, pg_typeof($ints)::text,
                     pg_typeof($usernames)::text, pg_typeof($playerIds)::text"""
          .query[(String, String, String, String, String)]
          .run()
      }.map(rows => assertTrue(rows == Vector(("text[]", "bigint[]", "integer[]", "text[]", "bigint[]"))))
    }

  private def testListsReadBack = test("a list of each array element type reads back as itself") {
    connectZIO {
      sql"SELECT $strings, $longs, $ints, $usernames, $playerIds"
        .query[(List[String], List[Long], List[Int], List[Username], List[PlayerId])]
        .run()
    }.map(rows => assertTrue(rows == Vector((strings, longs, ints, usernames, playerIds))))
  }

  // `typeCheck` compiles each snippet against this file's imports, so it sees the same givens as a table's SQL.
  private def testUnencodableListsDoNotCompile =
    test("a list of an enum or a URL does not compile, since neither is stored as its raw value") {
      for {
        enumList <- typeCheck("sql\"SELECT ${List(RunTrigger.Scheduled)}\"")
        urlList  <- typeCheck("sql\"SELECT ${List.empty[URL]}\"")
      } yield assertTrue(
        enumList.left.exists(_.contains("List[ccas.analysis.tables.RunTrigger]")),
        urlList.left.exists(_.contains("List[zio.http.URL]"))
      )
    }
}
