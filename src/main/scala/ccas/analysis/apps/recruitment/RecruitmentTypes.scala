package ccas.analysis.apps.recruitment

import ccas.utils.sql.EnumSql

enum CandidateOutcome { case Invited, Rejected, AlreadyMember, Error }
object CandidateOutcome extends EnumSql[CandidateOutcome]

enum ExhaustionBehavior { case Stop, Explore }
object ExhaustionBehavior extends EnumSql[ExhaustionBehavior]
// Stop: finish the run with however many candidates were found
// Explore: discover new sources (e.g. clubs that source club members are in, match opponents)
// For this scaffold, only Stop is implemented; Explore is deferred

sealed trait RejectionReason { def message: String }
object RejectionReason       {
  // Will be populated in follow-up: ExcludedNationality, TooManyClubs, EloOutOfRange, etc.
}
