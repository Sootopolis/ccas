package ccas.analysis.apps.ref

import java.time.temporal.ChronoUnit
import java.time.Instant

import zio.{Promise, Ref, UIO}

import ccas.analysis.tables.{MatchKey, RefSkipReason}
import ccas.api.clubmatch.TeamMatchTeams
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.client.ChessComClient

private[ref] object RefUtils {

  final case class UnresolvedPlayer(playerId: PlayerId, username: Username)
  final case class UnresolvedClub(clubId: ClubId, slug: ClubSlug)

  enum ResolveResult {
    case Resolved
    case NotFound   // had candidates, tried them, none worked
    case NoData     // API returned empty candidate list
    case SkipPlayer // player ID mismatch — don't try more matches/tournaments
  }

  object RetryWindows {
    val NoData: Long           = 14 // days
    val NotFound: Long         = 30
    val IdMismatch: Long       = 90
    val ResolutionFailed: Long = 7
    val ApiError: Long         = 3

    def cutoff(reason: RefSkipReason, now: Instant): Instant = {
      val days = reason match {
        case RefSkipReason.NoData           => NoData
        case RefSkipReason.NotFound         => NotFound
        case RefSkipReason.IdMismatch       => IdMismatch
        case RefSkipReason.ResolutionFailed => ResolutionFailed
        case RefSkipReason.ApiError         => ApiError
      }
      now.minus(days, ChronoUnit.DAYS)
    }

    case class Cutoffs(
      noData: Instant,
      notFound: Instant,
      idMismatch: Instant,
      resolutionFailed: Instant,
      apiError: Instant
    )

    def allCutoffs(now: Instant): Cutoffs = Cutoffs(
      noData = cutoff(RefSkipReason.NoData, now),
      notFound = cutoff(RefSkipReason.NotFound, now),
      idMismatch = cutoff(RefSkipReason.IdMismatch, now),
      resolutionFailed = cutoff(RefSkipReason.ResolutionFailed, now),
      apiError = cutoff(RefSkipReason.ApiError, now)
    )
  }

  class RefContext(
    val client: ChessComClient,
    val cache: Ref[Map[MatchKey, Promise[Throwable, TeamMatchTeams]]],
    val failedUrls: Ref[Map[String, String]],
    val failedUrlSource: Ref[Map[String, String]],
    val clubsResolvedDb: Ref[Int],
    val clubsResolvedApi: Ref[Int],
    val playersResolvedDb: Ref[Int],
    val playersResolvedApi: Ref[Int],
    val skippedPlayers: Ref[List[(PlayerId, Username)]],
    val playersSkippedNew: Ref[Int],
    val clubsSkippedNew: Ref[Int]
  )

  object RefContext {
    def make(client: ChessComClient): UIO[RefContext] =
      for {
        cache              <- Ref.make(Map.empty[MatchKey, Promise[Throwable, TeamMatchTeams]])
        failedUrls         <- Ref.make(Map.empty[String, String])
        failedUrlSource    <- Ref.make(Map.empty[String, String])
        clubsResolvedDb    <- Ref.make(0)
        clubsResolvedApi   <- Ref.make(0)
        playersResolvedDb  <- Ref.make(0)
        playersResolvedApi <- Ref.make(0)
        skippedPlayers     <- Ref.make(List.empty[(PlayerId, Username)])
        playersSkippedNew  <- Ref.make(0)
        clubsSkippedNew    <- Ref.make(0)
      } yield new RefContext(
        client,
        cache,
        failedUrls,
        failedUrlSource,
        clubsResolvedDb,
        clubsResolvedApi,
        playersResolvedDb,
        playersResolvedApi,
        skippedPlayers,
        playersSkippedNew,
        clubsSkippedNew
      )
  }

  case class ReportData(
    clubsTotal: Int,
    clubsResolvedDb: Int,
    clubsResolvedApi: Int,
    clubsSkippedNew: Int,
    playersTotal: Int,
    playersResolvedDb: Int,
    playersResolvedApi: Int,
    playersSkippedNew: Int,
    skippedPlayers: List[(PlayerId, Username)],
    playerSkipsByReason: List[(RefSkipReason, Long)],
    clubSkipsByReason: List[(RefSkipReason, Long)],
    upgradeEligible: Int,
    upgradeSucceeded: Int,
    startedAt: Instant,
    completedAt: Instant,
    failedQueries: Map[String, String],
    failedUrlSources: Map[String, String]
  )
}
