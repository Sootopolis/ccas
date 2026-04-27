package ccas.analysis.apps

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

/** Shared timeline anchor for app test support files. `t0` is a fixed wall-clock origin (2025-06-01); `t1`/`t2`/`t3`
  * are 1-, 30-, and 60-day offsets used by membership and recruitment tests for "active", "inactive", "long-gone"
  * states.
  */
object TestTimes {
  val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
  val t1: Instant = t0.plus(Duration.ofDays(1))
  val t2: Instant = t0.plus(Duration.ofDays(30))
  val t3: Instant = t0.plus(Duration.ofDays(60))
}
