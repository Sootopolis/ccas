package ccas.utils

import java.time.Instant
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.Locale

import scala.util.control.NonFatal

/** HTTP-date parser tolerating Chess.com's non-RFC format.
  *
  * Chess.com's public API emits `Last-Modified` values like `Thursday, 16-Apr-2026 23:13:22 GMT+0000` that match
  * none of the three HTTP-date forms in RFC 7231 §7.1.1.1 (IMF-fixdate, RFC 850, asctime), so zio-http's typed
  * [[zio.http.Header.LastModified]] parser returns `None` and we lose the value. This parser tries a layered list
  * of formatters: Chess.com's format first because it is empirically the only shape the API currently produces,
  * with the three standard forms behind it as forward-compat for any future server fix.
  *
  * Returns `None` for null / empty input or values that match none of the supported formats.
  */
object HttpDate {

  private val chessCom: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendPattern("EEEE, dd-MMM-yyyy HH:mm:ss ")
      .appendLiteral("GMT")
      .appendOffset("+HHMM", "+0000")
      .toFormatter(Locale.ENGLISH)

  private val imfFixdate: DateTimeFormatter = DateTimeFormatter.RFC_1123_DATE_TIME

  private val rfc850: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendPattern("EEEE, dd-MMM-")
      .appendValueReduced(ChronoField.YEAR, 2, 2, 1970)
      .appendPattern(" HH:mm:ss zzz")
      .toFormatter(Locale.ENGLISH)

  private val asctime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendPattern("EEE MMM [ ]d HH:mm:ss yyyy")
      .parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
      .toFormatter(Locale.ENGLISH)

  private val formatters: List[DateTimeFormatter] = List(chessCom, imfFixdate, rfc850, asctime)

  def parse(raw: String): Option[Instant] =
    if (raw == null || raw.isEmpty) None
    else
      formatters.iterator.flatMap { fmt =>
        try Some(Instant.from(fmt.parse(raw)))
        catch { case NonFatal(_) => None }
      }.nextOption()
}
