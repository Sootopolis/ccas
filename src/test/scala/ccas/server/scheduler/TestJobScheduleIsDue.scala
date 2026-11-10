package ccas.server.scheduler

import java.time.{Duration, Instant, ZonedDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.jobs.JobKind

/** Pure unit tests for [[JobSchedule.isDue]] — no DB, no clock. Covers both trigger types and every cron
  * misfire / first-enable / downtime edge case.
  */
object TestJobScheduleIsDue extends ZIOSpecDefault {

  // The Skip grace is two poll intervals: 30 minutes.
  private val pollInterval: Duration = Duration.ofMinutes(15)

  private def at(year: Int, month: Int, day: Int, hour: Int, minute: Int): Instant =
    ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneOffset.UTC).toInstant

  // A cron that fires daily at 09:00 UTC (stored in normalized 6-field form).
  private def daily9(lastRunAt: Option[Instant], misfire: MisfirePolicy): JobSchedule =
    JobSchedule.cron(
      id = 1L,
      kind = JobKind.MatchRef,
      clubIdOption = None,
      params = None,
      cronExpr = "0 0 9 * * ?",
      timezone = "UTC",
      misfire = misfire,
      enabled = true,
      lastRunAt = lastRunAt
    )

  private def interval(hours: Short, lastRunAt: Option[Instant]): JobSchedule =
    JobSchedule.interval(
      id = 1L,
      kind = JobKind.MatchRef,
      clubIdOption = None,
      params = None,
      intervalHours = hours,
      enabled = true,
      lastRunAt = lastRunAt
    )

  override def spec: Spec[Any, Any] = suite("TestJobScheduleIsDue")(
    // --- Interval ---
    test("interval is always due when never run") {
      assertTrue(interval(24, None).isDue(at(2026, 6, 27, 12, 0), pollInterval))
    },
    test("interval is not due before the interval elapses") {
      val lastRun = at(2026, 6, 27, 10, 0)
      assertTrue(!interval(24, Some(lastRun)).isDue(at(2026, 6, 27, 12, 0), pollInterval))
    },
    test("interval is due once the interval has elapsed") {
      val lastRun = at(2026, 6, 27, 10, 0)
      assertTrue(interval(1, Some(lastRun)).isDue(at(2026, 6, 27, 12, 0), pollInterval))
    },
    test(
      "an interval schedule is due at the tick nearest its time, though its last run was recorded after its own tick"
    ) {
      // #457: a run recorded 3 s after its tick, waiting on the database, used to miss the tick a day later.
      val lastRun = at(2026, 6, 27, 10, 0).plusSeconds(3)
      assertTrue(interval(24, Some(lastRun)).isDue(at(2026, 6, 28, 10, 0), pollInterval))
    },
    test("an interval schedule is not due more than half a poll interval early") {
      val lastRun = at(2026, 6, 27, 10, 0)
      val due     = at(2026, 6, 28, 9, 52).plusSeconds(30)
      assertTrue(
        interval(24, Some(lastRun)).isDue(due, pollInterval),
        !interval(24, Some(lastRun)).isDue(due.minusSeconds(1), pollInterval)
      )
    },

    // --- Cron: first-enable (no backfire) ---
    test("cron does not backfire the boundary that preceded its creation instant") {
      // Created at noon; today's 09:00 boundary is before lastRunAt, so it must not fire.
      val created = at(2026, 6, 27, 12, 0)
      assertTrue(
        !daily9(Some(created), MisfirePolicy.CatchUp).isDue(created, pollInterval),
        !daily9(Some(created), MisfirePolicy.Skip).isDue(created, pollInterval)
      )
    },

    // --- Cron: CatchUp ---
    test("cron CatchUp fires when a boundary has passed since last run") {
      val lastRun = at(2026, 6, 27, 8, 0)
      assertTrue(daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 12, 0), pollInterval))
    },
    test("cron CatchUp fires a boundary missed during long downtime (ignores grace)") {
      val lastRun = at(2026, 6, 25, 9, 0) // two days ago
      assertTrue(daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 12, 0), pollInterval))
    },
    test("cron CatchUp does not re-fire a boundary already run") {
      val lastRun = at(2026, 6, 27, 9, 0) // just ran at today's boundary
      assertTrue(!daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 9, 20), pollInterval))
    },

    // --- Cron: Skip ---
    test("cron Skip fires a boundary observed within the grace window") {
      val lastRun = at(2026, 6, 27, 8, 0)
      assertTrue(daily9(Some(lastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 10), pollInterval))
    },
    test("cron Skip does NOT fire a boundary missed during downtime (past grace)") {
      val lastRun = at(2026, 6, 27, 6, 0)
      assertTrue(!daily9(Some(lastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 12, 0), pollInterval))
    },
    test("cron Skip resumes on the next on-time boundary without an explicit baseline advance") {
      // lastRunAt is still stale (from before a skipped boundary); the next day's on-time boundary fires.
      val staleLastRun = at(2026, 6, 27, 6, 0)
      assertTrue(daily9(Some(staleLastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 28, 9, 5), pollInterval))
    },
    test("cron is due at the very instant of its boundary, under both policies") {
      val lastRun = at(2026, 6, 26, 9, 0)
      assertTrue(
        daily9(Some(lastRun), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 0), pollInterval),
        daily9(Some(lastRun), MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 9, 0), pollInterval)
      )
    },
    test("cron Skip does not double-fire within one boundary's grace window") {
      val justFired = at(2026, 6, 27, 9, 0)
      assertTrue(!daily9(Some(justFired), MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 20), pollInterval))
    },

    // --- Cron: None lastRunAt ---
    test("cron with no last run fires the most recent boundary under both policies") {
      assertTrue(
        daily9(None, MisfirePolicy.CatchUp).isDue(at(2026, 6, 27, 9, 10), pollInterval),
        daily9(None, MisfirePolicy.Skip).isDue(at(2026, 6, 27, 9, 10), pollInterval)
      )
    },

    // --- nextDue ---
    test("nextDue: an interval schedule is next due its interval less half a poll after its last run") {
      val lastRun  = at(2026, 6, 27, 10, 0)
      val expected = at(2026, 6, 28, 9, 52).plusSeconds(30)
      assertTrue(interval(24, Some(lastRun)).nextDue(at(2026, 6, 27, 12, 0), pollInterval).contains(expected))
    },
    test("nextDue: a schedule that has never run is due now") {
      val now = at(2026, 6, 27, 12, 0)
      assertTrue(interval(24, None).nextDue(now, pollInterval).contains(now))
    },
    test("nextDue: a cron Skip schedule past its grace is next due at its next boundary") {
      val lastRun = at(2026, 6, 27, 6, 0)
      assertTrue(
        daily9(Some(lastRun), MisfirePolicy.Skip).nextDue(at(2026, 6, 27, 12, 0), pollInterval).contains(
          at(2026, 6, 28, 9, 0)
        )
      )
    },
    test("nextDue: a cron CatchUp schedule with a missed boundary is due now") {
      val lastRun = at(2026, 6, 25, 9, 0)
      val now     = at(2026, 6, 27, 12, 0)
      assertTrue(daily9(Some(lastRun), MisfirePolicy.CatchUp).nextDue(now, pollInterval).contains(now))
    },
    test("nextDue: isDue holds at nextDue and not a second before") {
      val now = at(2026, 6, 27, 12, 0)
      val schedules = List(
        interval(24, Some(at(2026, 6, 27, 10, 0))),
        daily9(Some(at(2026, 6, 27, 6, 0)), MisfirePolicy.Skip),
        daily9(Some(at(2026, 6, 27, 9, 0)), MisfirePolicy.CatchUp)
      )
      val agrees = schedules.map { schedule =>
        schedule.nextDue(now, pollInterval).exists { due =>
          schedule.isDue(due, pollInterval) && !schedule.isDue(due.minusSeconds(1), pollInterval)
        }
      }
      assertTrue(agrees == List(true, true, true))
    }
  )
}
