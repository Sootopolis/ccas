package ccas.analysis.apps.recruitment

import ccas.utils.sql.EnumSql

enum CandidateOutcome { case Invited, Rejected, AlreadyMember, Error }
object CandidateOutcome extends EnumSql[CandidateOutcome]
