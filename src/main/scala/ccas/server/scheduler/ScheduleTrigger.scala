package ccas.server.scheduler

import java.time.ZoneId

import scala.util.Try

import cron4s.CronExpr

import ccas.utils.json.EnumJson
import ccas.utils.sql.EnumSql

/** Which kind of trigger a [[JobSchedule]] uses. Stored in `trigger_type` (PascalCase via EnumSql) and
  * carried on the wire snake_case via EnumJson (`interval` / `cron`).
  */
enum TriggerType {
  case Interval, Cron
}
object TriggerType extends EnumJson[TriggerType] with EnumSql[TriggerType]

/** What a cron schedule does when the server was down across one or more fire-times.
  *   - [[Skip]]: do not fire a boundary that elapsed more than the scheduler's grace window ago; resume on
  *     the next on-time boundary. Right for idempotent maintenance (e.g. ClubData) where a late run is noise.
  *   - [[CatchUp]]: on recovery, fire the most recent missed boundary exactly once, then resume. Right when a
  *     missed run still wants to happen on stale-but-acceptable data (e.g. a weekly Stats roll-up).
  */
enum MisfirePolicy {
  case Skip, CatchUp
}
object MisfirePolicy extends EnumJson[MisfirePolicy] with EnumSql[MisfirePolicy]

/** Decoded trigger of a [[JobSchedule]] — the typed view of the flat `trigger_type` / `interval_hours` /
  * `cron_expr` / `timezone` / `misfire_policy` columns.
  */
sealed trait ScheduleTrigger
object ScheduleTrigger {
  final case class Interval(hours: Short) extends ScheduleTrigger
  final case class Cron(expr: CronExpr, zone: ZoneId, misfire: MisfirePolicy) extends ScheduleTrigger

  /** Translate a 5-field unix cron (`min hour day-of-month month day-of-week`) into the 6-field cron4s form
    * (`sec min hour day-of-month month day-of-week`) this app persists.
    *
    * Two adaptations:
    *   - Prepend a `0` seconds field — cron4s is 6-field, but a 15-minute poll makes sub-minute precision
    *     meaningless, so seconds are always pinned to 0.
    *   - cron4s requires *exactly one* of day-of-month / day-of-week to be `?` (it cannot express unix's "both
    *     wildcards" or "both restricted / OR" forms). We map the unix wildcard semantics onto that rule:
    *     a wildcard (`*`) day field becomes `?` when the other day field carries the real constraint;
    *     restricting *both* day fields is rejected (cron4s has no equivalent). A `?` the user typed directly
    *     is trusted as-is.
    *
    * Returns the normalized 6-field string, or a human-readable error.
    */
  def normalize(unix5: String): Either[String, String] = {
    val fields = unix5.trim.split("\\s+").toVector
    if (fields.length != 5) {
      Left(s"expected a 5-field cron 'min hour day-of-month month day-of-week', got ${fields.length} field(s)")
    } else {
      val Vector(min, hour, dom, mon, dow) = fields
      val days: Either[String, (String, String)] =
        if (dom == "?" || dow == "?") { Right((dom, dow)) }       // user already disambiguated
        else {
          (dom, dow) match {
            case (_, "*")   => Right((dom, "?")) // dow wildcard (incl. both wildcard) -> blank the dow
            case ("*", _)   => Right(("?", dow)) // dom wildcard, dow restricted -> blank the dom
            case _          => Left("cron4s cannot restrict both day-of-month and day-of-week; set one to '*'")
          }
        }
      days.map { case (d, w) => s"0 $min $hour $d $mon $w" }
    }
  }

  /** Validate a 5-field unix cron and return the NORMALIZED 6-field string to persist (or an error). */
  def validateCron(unix5: String): Either[String, String] =
    normalize(unix5).flatMap { norm =>
      // Fully-qualified: the nested `ScheduleTrigger.Cron` case class shadows the bare name in this scope.
      cron4s.Cron.parse(norm) match {
        case Right(_)  => Right(norm)
        case Left(err) => Left(s"invalid cron '${unix5.trim}': ${Option(err.getMessage).getOrElse(err.toString)}")
      }
    }

  /** Validate an IANA timezone id, returning it unchanged (or an error). */
  def validateZone(tz: String): Either[String, String] =
    Try(ZoneId.of(tz)).toEither
      .map(_ => tz)
      .left.map(_ => s"invalid timezone (expected an IANA zone id like 'Europe/London'): $tz")
}
