package ccas.analysis.apps.recruitment

import java.time.Instant

import zio.{RIO, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.apps.PlayerUpdater
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.ApiPlayerMatches
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

private[recruitment] object RecruitmentPersistence {

  def persistCandidateResults(
    runId: Long,
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
              Player.insert(
                Player(
                  ap.playerId,
                  ap.joinedAt,
                  candidate.username,
                  ap.status.category,
                  ap.title,
                  now
                )
              ).unit
            } else {
              Player.selectIdForUpdate(ap.playerId).flatMap {
                case Some(existing) if existing.stateMatches(candidate.username, ap.status.category, ap.title) =>
                  ZIO.unit
                case Some(existing) =>
                  PlayerUpdater.archiveAndUpdate(
                    existing,
                    candidate.username,
                    ap.status.category,
                    ap.title,
                    now,
                    client
                  ).unit
                case None => ZIO.unit
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
      PlayerMatchRef.selectId(playerId).flatMap {
        case Some(_) => ZIO.unit
        case None =>
          ClubMatchBoard.selectPlayerMatchRef(playerId).flatMap {
            case Some(ref) => PlayerMatchRef.insert(ref).unit
            case None =>
              ZIO.foreachDiscard(candidate.playerMatches) { playerMatches =>
                resolvePlayerRefViaApi(client, playerId, candidate.username, playerMatches)
              }
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
        RefHelpers.fetchTeamMatchTeams(client, parsed.matchId, parsed.isLive).flatMap { teams =>
          ZIO.foreachDiscard(RefHelpers.findPlayerIsTeam1(teams, username)) { t1 =>
            PlayerMatchRef.insert(PlayerMatchRef(playerId, parsed.matchId, parsed.isLive, t1, idx)).unit
          }
        }
      }
    }
  }
}
