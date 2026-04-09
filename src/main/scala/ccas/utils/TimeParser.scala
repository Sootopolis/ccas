package ccas.utils

import java.time.{Instant, LocalDate, ZoneOffset}

import zio.{IO, ZIO}

object TimeParser {

  /** Parse a date or instant string into an [[Instant]].
    *
    * Accepts:
    *   - Full ISO-8601 instant: `2026-03-23T00:00:00Z`
    *   - Plain date (midnight UTC): `2026-03-23`
    */
  def parseInstant(s: String): Either[String, Instant] =
    if (s.contains("T")) {
      try Right(Instant.parse(s))
      catch { case e: Exception => Left(s"Invalid instant: $s (${e.getMessage})") }
    } else {
      try Right(LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC))
      catch { case e: Exception => Left(s"Invalid date: $s (${e.getMessage})") }
    }

  /** ZIO variant of [[parseInstant]], failing with the error string. */
  def parseInstantZIO(s: String): IO[String, Instant] =
    ZIO.fromEither(parseInstant(s))
}
