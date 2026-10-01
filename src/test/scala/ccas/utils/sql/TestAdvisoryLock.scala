package ccas.utils.sql

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*
import scala.util.Using

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.utils.sql.AdvisoryLock.Space

/** Postgres gives an advisory lock's numbers no meaning, so these checks are all that keep two kinds of lock from
  * sharing one (ADR 0020).
  */
object TestAdvisoryLock extends ZIOSpecDefault {

  private val Registry = Paths.get("src/main/scala/ccas/utils/sql/AdvisoryLock.scala")

  // Everything this project runs against a database: the application, and the scripts run by hand.
  private val Scanned = List("src/main", "sql", "scripts").map(Paths.get(_))

  // Every advisory-lock function Postgres has, in any case: `pg_advisory_lock`, `pg_try_advisory_xact_lock_shared`, …
  private val AdvisoryCall = "(?i)pg_(try_)?advisory".r

  override def spec: Spec[Any, Throwable] = suite("TestAdvisoryLock")(
    test("every lock space has a number of its own") {
      val numbers = Space.values.toList.map(_.classId)
      assertTrue(numbers.distinct == numbers)
    },
    test("advisory locks are taken only through AdvisoryLock, which registers every lock space") {
      val calls = for {
        root           <- Scanned
        file           <- filesUnder(root) if file != Registry
        (line, number) <- Files.readAllLines(file).asScala.toList.zipWithIndex
        if AdvisoryCall.findFirstIn(line).isDefined
      } yield s"$file:${number + 1}"
      assertTrue(calls.isEmpty)
    }
  )

  private def filesUnder(root: Path): List[Path] =
    Using.resource(Files.walk(root))(_.iterator.asScala.filter(Files.isRegularFile(_)).toList)
}
