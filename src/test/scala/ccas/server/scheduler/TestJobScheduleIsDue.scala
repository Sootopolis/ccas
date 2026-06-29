package ccas.server.scheduler

import java.time.{Duration, Instant, ZoneOffset, ZonedDateTime}

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.jobs.JobKind

/** Pure unit tests for [[JobSchedule.isDue]] — no DB, no clock. Covers both trigger types and every cron
  * misfire / first-enable / downtime edge case.
  */
object TestJobScheduleIsDue extends ZIOSpecDefault {

  private val grace: Duration = Duration.ofMinutes(30)

  private def at(year: Int, month: Int, day: Int, hour: Int, minute: Int): Instant =
    ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneOffset.UTC).toInstant

  // A cron that fires daily at 09:00 UTC (stored in normalized 6-field form).
  private def daily9(lastRunAt: Option[Instant], misfire: MisfirePolicy): JobSchedule =
    JobSchedule.cron(1L, JobKind.MatchRef, None, None, "0 0 9 * * ?", "UTC", misfire, enabled = true, lastRunAt)

  private def interval(hours: Short, lastRunAt: Option[Instant]): JobSchedule =
    JobSchedule.interval(1L, JobKind.MatchRef, None, None, hours, enabled = true, lastRunAt)

  override def spec: Spec[Any, Any] = suite("TestJobScheduleIsDue")(
    // --- Interval ---
    test("interval is always due when never run") {
      assertTrue(interval(24, None).isDue(at(2026, 6, 27, 12, 0), grace))
    },
    test("interval is not due before the interval elapses") {
      val lastRun = at(2026, 6, 27, 10, 0)
      assertTrue(!interval(24, Some(lastRun)).isDue(at(2026, 6, 27, 12, 0), grace))
    },
    test("interval is due once the interval has elapsed") {
      val lastRun = at(2026, 6, 27, 10, 0)
      assertTrue(interval(1, Some(lastRun)).isDue(at(2026, 6, 27, 12, 0), grace))
    },

    // --- Cron: first-enable (no backfire) ---
    test("cron does not backfire the boundary that preceded its creation instant") {
      // Created at noon; today's 09:00 boundary is before lastRunAt, so it must not fire.
      val created = at(2026, 6, 27, 12, 0)
      assertTrue(
        !daily9(Some(created), MisfirePolicy.CatchUp).isDue(created, grace),
        !daily9(Some(created), MisfirePolicy.Skip).isDue(created, grace)
      )
    },

    // --- Cron: CatchUp ---
    test("cron CatchUp fires when a boundary has passed since last run") {
      val lastRun = at(2026, 6, 27, 8, 0)
      assertTrue(daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 12, 0), grace))
    },
    test("cron CatchUp fires a boundary missed during long downtime (ignores grace)") {
      val lastRun = at(2026, 6, 25, 9, 0) // two days ago
      assertTrue(daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 12, 0), grace))
    },
    test("cron CatchUp does not re-fire a boundary already run") {
      val lastRun = at(2026, 6, 27, 9, 0) // just ran at today's boundary
      assertTrue(!daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 9, 20), grace))
    },

    // --- Cron: Skip ---
    test("cron Skip fires a boundary observed within the grace window") {
      val lastRun = at(2026, 6, 27, 8, 0)
      assertTrue(daily9(Some(lastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 10), grace))
    },
    test("cron Skip does NOT fire a boundary missed during downtime (past grace)") {
      val lastRun = at(2026, 6, 27, 6, 0)
      assertTrue(!daily9(Some(lastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 12, 0), grace))
    },
    test("cron Skip resumes on the next on-time boundary without an explicit baseline advance") {
      // lastRunAt is still stale (from before a skipped boundary); the next day's on-time boundary fires.
      val staleLastRun = at(2026, 6, 27, 6, 0)
      assertTrue(daily9(Some(staleLastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 28, 9, 5), grace))
    },
    test("cron Skip does not double-fire within one boundary's grace window") {
      val justFired = at(2026, 6, 27, 9, 0)
      assertTrue(!daily9(Some(justFired), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 20), grace))
    },

    // --- Cron: None lastRunAt ---
    test("cron with no last run fires the most recent boundary under both policies") {
      assertTrue(
        daily9(None, MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 9, 10), grace),
        daily9(None, MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 10), grace)
      )
    }
  )
}
