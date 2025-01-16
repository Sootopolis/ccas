package ccas.analysis.tables

import ccas.api.utils.subtypes.PlayerId

import java.time.Instant

case class Member(
  playerId: PlayerId,
  since: Instant,
  isClosed: Boolean,
  isBanned: Boolean
)
