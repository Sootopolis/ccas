package ccas.analysis.apps.recruitment

import java.time.Instant

import zio.{RIO, ZIO}

import ccas.analysis.apps.PlayerUpdater
import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.tables.*
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.ApiPlayerMatches
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

private[recruitment] object RecruitmentPersistence {

  /** Returns whether the candidate row was written. A player listed under two names is evaluated twice, and
    * whichever evaluation writes first keeps its result. A pass is written as `Deferred` until the run confirms it.
    */
  def persistCandidateResults(
    runId: RecruitmentRunId,
    now: Instant,
    candidate: CandidateContext,
    rejectionOption: Option[Rejection]
  ): RIO[PostgresClient, Boolean] = {
    val outcome = rejectionOption.fold(CandidateOutcome.Deferred)(_.outcome)
    persist(runId, now, candidate, outcome, rejectionOption.map(_.reason), errorMessageOption = None)
  }

  /** As [[persistCandidateResults]], for an evaluation that failed. */
  def persistCandidateError(
    runId: RecruitmentRunId,
    now: Instant,
    candidate: CandidateContext,
    errorMessage: String
  ): RIO[PostgresClient, Boolean] =
    persist(runId, now, candidate, CandidateOutcome.Error, reasonOption = None, Some(errorMessage))

  private def persist(
    runId: RecruitmentRunId,
    now: Instant,
    candidate: CandidateContext,
    outcome: CandidateOutcome,
    reasonOption: Option[RejectionReason],
    errorMessageOption: Option[String]
  ): RIO[PostgresClient, Boolean] =
    // No player data (transient API error) — skip persistence, retry next run
    ZIO.foreach(candidate.apiPlayerOption) { ap =>
      withTransaction {
        for {
          _ <-
            if (candidate.isNewPlayer) {
              // `insertIfNew`, not `insert`: a filter (e.g. CheckAdminOfDiscoveredClub via ClubAdminResolver) may have
              // already inserted this player into the table during evaluation, in which case the row is already current.
              Player.insertIfNew(
                Player(ap.playerId, ap.joinedAt, candidate.username, ap.status.category, ap.title, now)
              ).unit
            } else {
              // A failed evaluation leaves a stored player alone: its context is the one entering the failing filter,
              // which may predate a rename that filter recovered.
              ZIO.whenDiscard(outcome != CandidateOutcome.Error) {
                Player.selectIdForUpdate(ap.playerId).flatMap {
                  ZIO.foreachDiscard(_) { existing =>
                    PlayerUpdater.updateIfDrifted(
                      existing = existing,
                      newUsername = candidate.username,
                      newStatus = ap.status.category,
                      newTitle = ap.title,
                      since = now
                    )
                  }
                }
              }
            }
          _ <- ZIO.foreachDiscard(candidate.cacheOption)(PlayerRecruitmentCache.upsert)
          inserted <- RecruitmentCandidate.insertIfNew(
            RecruitmentCandidate(runId, ap.playerId, now, outcome, reasonOption, errorMessageOption)
          )
          _ <- ZIO.whenDiscard(inserted == 0) {
            ZIO.logInfo(s"[Recruitment] ${candidate.username} (player ${ap.playerId}) already evaluated in this run")
          }
        } yield inserted == 1
      }
    }.map(_.contains(true))

  def writePlayerMatchRef(
    client: ChessComClient,
    candidate: CandidateContext
  ): RIO[PostgresClient, Unit] =
    ZIO.foreachDiscard(candidate.apiPlayerOption) { ap =>
      val playerId = ap.playerId
      ZIO.whenZIODiscard(PlayerMatchRef.findOrInfer(playerId).map(_.isEmpty)) {
        ZIO.foreachDiscard(candidate.playerMatchesOption) { playerMatches =>
          resolvePlayerRefViaApi(client, playerId, candidate.username, playerMatches)
        }
      }
    }

  private def resolvePlayerRefViaApi(
    client: ChessComClient,
    playerId: PlayerId,
    username: Username,
    playerMatches: ApiPlayerMatches
  ): RIO[PostgresClient, Unit] = {
    val candidates = playerMatches.finished.filter(_.board.isDefined)
    ZIO.foreachDiscard(candidates.headOption) { m =>
      val parsed   = RefHelpers.parseMatchUrl(m.`@id`)
      val boardIdx = m.board.get.path.segments.lastOption.flatMap(_.toIntOption).map(_.toShort)
      ZIO.foreachDiscard(boardIdx) { idx =>
        RefHelpers.fetchTeamMatchTeamsOptional(client, parsed.matchId, parsed.isLive).flatMap { teamsOption =>
          ZIO.foreachDiscard(teamsOption.flatMap(_.findPlayerIsTeam1(username))) { isTeam1 =>
            PlayerMatchRef.upsert(PlayerMatchRef(playerId, parsed.matchId, parsed.isLive, isTeam1, idx)).unit
          }
        }
      }
    }
  }
}
