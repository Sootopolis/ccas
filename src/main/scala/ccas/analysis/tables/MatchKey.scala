package ccas.analysis.tables

import ccas.api.misc.subtypes.ClubMatchId

/** Composite key identifying a specific team match: its ID and whether it's a live (vs daily) match. */
final case class MatchKey(matchId: ClubMatchId, isLive: Boolean)
