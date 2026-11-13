package ccas.analysis.apps.recruitment

import ccas.utils.sql.EnumSql

/** `RejectedFromCache` is a rejection on cached figures alone, which the `daysSinceRejected` cooldown ignores. */
enum CandidateOutcome { case Invited, Rejected, Error, Deferred, RejectedFromCache }

object CandidateOutcome extends EnumSql[CandidateOutcome]
