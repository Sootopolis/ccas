package ccas.analysis.apps.recruitment

import java.time.Instant

import zio.{RIO, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.apps.PlayerUpdater
import ccas.analysis.tables.*
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.ApiPlayerMatches
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

private[recruitment] object RecruitmentPersistence {

  def persistCandidateResults(
    runId: RecruitmentRunId,
    now: Instant,
    candidate: CandidateContext,
    outcome: CandidateOutcome,
    client: ChessComClient,
    errorMessage: Option[String] = None
  ): RIO[PostgresClient, Unit] =
    // No player data (transient API error) — skip persistence, retry next run
    ZIO.foreachDiscard(candidate.apiPlayer) { ap =>
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
              Player.selectIdForUpdate(ap.playerId).flatMap {
                ZIO.foreachDiscard(_) { existing =>
                  ZIO.whenDiscard(!existing.stateMatches(candidate.username, ap.status.category, ap.title)) {
                    PlayerUpdater.archiveAndUpdate(
                      existing = existing,
                      newUsername = candidate.username,
                      newStatus = ap.status.category,
                      newTitle = ap.title,
                      since = now,
                      client = client
                    ).unit
                  }
                }
              }
            }
          _ <- ZIO.foreachDiscard(candidate.cache)(PlayerRecruitmentCache.upsert)
          // Skip candidate row for cache-only rejections so they aren't blocked by daysSinceRejected
          // Passing candidates are written as Deferred; only flipped to Invited after confirmation at finalization
          dbOutcome = if (outcome == CandidateOutcome.Invited) CandidateOutcome.Deferred else outcome
          _ <- ZIO.unlessDiscard(candidate.cacheRejected)(
            RecruitmentCandidate.insert(RecruitmentCandidate(runId, ap.playerId, now, dbOutcome, errorMessage))
          )
        } yield ()
      }
    }

  def writePlayerMatchRef(
    client: ChessComClient,
    candidate: CandidateContext
  ): RIO[PostgresClient, Unit] =
    ZIO.foreachDiscard(candidate.apiPlayer) { ap =>
      val playerId = ap.playerId
      ZIO.whenZIODiscard(PlayerMatchRef.findOrInfer(playerId).map(_.isEmpty)) {
        ZIO.foreachDiscard(candidate.playerMatches) { playerMatches =>
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
        RefHelpers.fetchTeamMatchTeamsOptional(client, parsed.matchId, parsed.isLive).flatMap { teamsOpt =>
          ZIO.foreachDiscard(teamsOpt.flatMap(RefHelpers.findPlayerIsTeam1(_, username))) { isTeam1 =>
            PlayerMatchRef.upsert(PlayerMatchRef(playerId, parsed.matchId, parsed.isLive, isTeam1, idx)).unit
          }
        }
      }
    }
  }
}
