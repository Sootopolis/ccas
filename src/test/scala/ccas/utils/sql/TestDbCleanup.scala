package ccas.utils.sql

import com.augustnagro.magnum.sql
import zio.RIO

import ccas.utils.sql.PostgresClient.transactZIO

/** Feature-scoped, FK-aware DELETE helpers for tests. Each helper clears its named feature's tables in
  * child→parent order inside its own `transactZIO` block; composing helpers via `*>` therefore crosses transaction
  * boundaries (atomicity is per-helper, not per-composition). For test before-each cleanup this is fine — suites
  * are sequential and the next run re-attempts cleanup from a clean state. If a future suite needs single-shot
  * atomicity across features, inline a `transactZIO { ... }` block at the call site rather than chaining helpers.
  *
  * **Helpers are intentionally narrow.** Each one lists exactly which tables it clears; FK children of the
  * "root" entity that are not listed must be handled by the caller (compose with another helper or inline a
  * DELETE before the helper). New tables that FK to an existing root are NOT picked up automatically — bumping
  * a helper's scope is a deliberate change.
  */
object TestDbCleanup {

  /** Clears: `player_snapshot`, `player`.
    *
    * Does NOT clear other FK children of `player`: `player_match_ref`, `player_tournament_ref`, `player_ref_skip`,
    * `player_recruitment_cache`, `recruitment_candidate`, `recruitment_blacklist`, `club_admin`, `club_member`.
    * If a suite seeds any of those, clear them first (or compose with the relevant helper) — otherwise the
    * `DELETE FROM player` will fail with an FK violation.
    */
  val clearPlayer: RIO[PostgresClient, Unit] = transactZIO {
    val _ = sql"DELETE FROM player_snapshot".update.run()
    sql"DELETE FROM player".update.run()
  }.unit

  /** Clears: `club_match_game`, `club_match_board`, `club_match`. Use when a suite seeds match data without
    * touching `club_match_ref` or `unresolved_match_club` (both FK to `club_match`).
    */
  val clearMatches: RIO[PostgresClient, Unit] = transactZIO {
    val _ = sql"DELETE FROM club_match_game".update.run()
    val _ = sql"DELETE FROM club_match_board".update.run()
    sql"DELETE FROM club_match".update.run()
  }.unit

  /** Clears: `club_match_game`, `club_match_board`, `club_match`, `club_admin`, `club_member`, `club_match_ref`,
    * `unresolved_match_club`, `club`.
    *
    * Does NOT clear other FK children of `club`: `recruitment_blacklist`, `recruitment_alias`, `recruitment_run`,
    * `membership_run`, `club_ref_skip`, `history_member_query`, `history_pending_match`. If a suite seeds any
    * of those, clear them first — otherwise the `DELETE FROM club` will fail with an FK violation.
    *
    * Also does not touch `player`. Compose with `clearPlayer` either side: `club_admin` and `club_member`
    * (FK to `player`) are emptied here, so `DELETE FROM player` succeeds whether `clearPlayer` runs before
    * or after `clearClub` provided no `player`-FK-children survive elsewhere.
    */
  val clearClub: RIO[PostgresClient, Unit] = transactZIO {
    val _ = sql"DELETE FROM club_match_game".update.run()
    val _ = sql"DELETE FROM club_match_board".update.run()
    val _ = sql"DELETE FROM club_match".update.run()
    val _ = sql"DELETE FROM club_admin".update.run()
    val _ = sql"DELETE FROM club_member".update.run()
    val _ = sql"DELETE FROM club_match_ref".update.run()
    val _ = sql"DELETE FROM unresolved_match_club".update.run()
    sql"DELETE FROM club".update.run()
  }.unit

  /** `recruitment_blacklist`. */
  val clearRecruitmentBlacklist: RIO[PostgresClient, Unit] = transactZIO {
    sql"DELETE FROM recruitment_blacklist".update.run()
  }.unit

  /** Clears: `api_fetch_failure`, `api_response_cache`, `api_response_body`. Use when a suite drives the
    * `ChessComClient` through 404s or cache misses so subsequent runs start from an empty cache.
    */
  val clearApiCache: RIO[PostgresClient, Unit] = transactZIO {
    val _ = sql"DELETE FROM api_fetch_failure".update.run()
    val _ = sql"DELETE FROM api_response_cache".update.run()
    sql"DELETE FROM api_response_body".update.run()
  }.unit

  /** `job_run`. */
  val clearJobRuns: RIO[PostgresClient, Unit] = transactZIO {
    sql"DELETE FROM job_run".update.run()
  }.unit

  /** `job_schedule`. */
  val clearJobSchedules: RIO[PostgresClient, Unit] = transactZIO {
    sql"DELETE FROM job_schedule".update.run()
  }.unit
}
