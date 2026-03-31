package ccas.analysis.apps.history

import zio.{Promise, Ref, ZIO}

import ccas.analysis.tables.MatchKey
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.client.ChessComClient

private[history] object HistoryUtils {

  case class DiscoveredPlayer(playerId: PlayerId, username: Username)

  case class MemberSeedResult(
    seeded: Int,
    queried: Int,
    failed: Int,
    failedMembers: List[(Username, String)]
  )

  case class RunStats(
    membersQueried: Int = 0,
    membersSkipped: Int = 0,
    membersFailed: Int = 0,
    matchesSeeded: Int = 0,
    matchesProcessed: Int = 0,
    matchesFailed: Int = 0,
    matchesUnidentified: Int = 0,
    playersDiscovered: Int = 0,
    playersKnown: Int = 0,
    playersFailed: Int = 0,
    waveCount: Int = 0,
    pendingRemaining: Int = 0,
    waveDetails: List[(Int, Int)] = Nil,
    failedMatches: List[(MatchKey, String)] = Nil,
    failedMembers: List[(Username, String)] = Nil
  )

  /** Shared mutable state for concurrent Phase 3 processing: API response caches (deduplicated via Promises), a player
    * lookup map, counters for statistics, and error tracking.
    */
  class ProcessingContext(
    val client: ChessComClient,
    val clubId: ClubId,
    val clubSlug: ClubSlug,
    val matchCache: Ref[Map[ClubMatchId, Promise[Throwable, ApiDailyMatch]]],
    val liveMatchCache: Ref[Map[ClubMatchId, Promise[Throwable, ApiLiveMatch]]],
    val discoveryCache: Ref[Map[String, Promise[Throwable, Option[PlayerId]]]],
    val clubCache: Ref[Map[String, Promise[Throwable, Option[ClubId]]]],
    val knownPlayers: Ref[Map[String, PlayerId]],
    val newPlayers: Ref[Set[DiscoveredPlayer]],
    val matchesProcessed: Ref[Int],
    val matchesFailed: Ref[Int],
    val matchesUnidentified: Ref[Int],
    val playersDiscovered: Ref[Int],
    val playersKnown: Ref[Int],
    val playersFailed: Ref[Int],
    val failedMatches: Ref[List[(MatchKey, String)]]
  )

  object ProcessingContext {
    def make(
      client: ChessComClient,
      clubId: ClubId,
      clubSlug: ClubSlug,
      initialKnownPlayers: Map[String, PlayerId]
    ): ZIO[Any, Nothing, ProcessingContext] =
      for {
        matchCache          <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiDailyMatch]])
        liveMatchCache      <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiLiveMatch]])
        discoveryCache      <- Ref.make(Map.empty[String, Promise[Throwable, Option[PlayerId]]])
        clubCache           <- Ref.make(Map.empty[String, Promise[Throwable, Option[ClubId]]])
        knownPlayers        <- Ref.make(initialKnownPlayers)
        newPlayers          <- Ref.make(Set.empty[DiscoveredPlayer])
        matchesProcessed    <- Ref.make(0)
        matchesFailed       <- Ref.make(0)
        matchesUnidentified <- Ref.make(0)
        playersDiscovered   <- Ref.make(0)
        playersKnown        <- Ref.make(0)
        playersFailed       <- Ref.make(0)
        failedMatches       <- Ref.make(List.empty[(MatchKey, String)])
      } yield new ProcessingContext(
        client,
        clubId,
        clubSlug,
        matchCache,
        liveMatchCache,
        discoveryCache,
        clubCache,
        knownPlayers,
        newPlayers,
        matchesProcessed,
        matchesFailed,
        matchesUnidentified,
        playersDiscovered,
        playersKnown,
        playersFailed,
        failedMatches
      )
  }
}
