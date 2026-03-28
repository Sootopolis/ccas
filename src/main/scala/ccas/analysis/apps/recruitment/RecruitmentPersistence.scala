package ccas.analysis.apps.recruitment

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.{RIO, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.ApiPlayerMatches
import ccas.utils.client.ChessComClient
import ccas.utils.sql.SqlZioTypes.withTransaction

private[recruitment] object RecruitmentPersistence {

  def persistCandidateResults(
    runId: Long,
    now: Instant,
    candidate: CandidateContext,
    outcome: CandidateOutcome,
    errorMessage: Option[String] = None
  ): RIO[Transactor, Unit] =
    // No player data (transient API error) — skip persistence, retry next run
    ZIO.foreachDiscard(candidate.apiPlayer) { ap =>
      withTransaction {
        for {
          _ <- ZIO.whenDiscard(candidate.isNewPlayer)(
            Player.insert(Player(ap.playerId, Instant.ofEpochSecond(ap.joined)))
          )
          _ <- {
            val snap = PlayerSnapshot(ap.playerId, now, candidate.username, ap.status.category, ap.title)
            PlayerSnapshot.selectIdLatest(ap.playerId).flatMap {
              case Some(latest)
                  if latest.username == snap.username && latest.status == snap.status && latest.title == snap.title =>
                ZIO.unit
              case _ =>
                PlayerSnapshot.insert(snap)
            }
          }
          _ <- ZIO.foreachDiscard(candidate.cache)(PlayerRecruitmentCache.upsert)
          // Skip candidate row for cache-only rejections so they aren't blocked by daysSinceRejected
          _ <- ZIO.unlessDiscard(candidate.cacheRejected)(
            RecruitmentCandidate.insert(RecruitmentCandidate(runId, ap.playerId, now, outcome, errorMessage))
          )
        } yield ()
      }
    }

  def writePlayerMatchRef(
    client: ChessComClient,
    candidate: CandidateContext
  ): RIO[Transactor, Unit] = {
    val data = for {
      ap <- candidate.apiPlayer
      pm <- candidate.playerMatches
    } yield (ap.playerId, candidate.username, pm)

    ZIO.foreachDiscard(data) { case (playerId, username, playerMatches) =>
      PlayerMatchRef.selectId(playerId).flatMap {
        case Some(_) => ZIO.unit
        case None =>
          ClubMatchBoard.selectPlayerMatchRef(playerId).flatMap {
            case Some(ref) => PlayerMatchRef.insert(ref).unit
            case None =>
              resolvePlayerRefViaApi(client, playerId, username, playerMatches)
          }
      }
    }
  }

  private def resolvePlayerRefViaApi(
    client: ChessComClient,
    playerId: PlayerId,
    username: Username,
    playerMatches: ApiPlayerMatches
  ): RIO[Transactor, Unit] = {
    val candidates = playerMatches.finished.filter(_.board.isDefined)
    ZIO.foreachDiscard(candidates.headOption) { m =>
      val parsed   = RefHelpers.parseMatchUrl(m.`@id`)
      val boardIdx = m.board.get.path.segments.lastOption.flatMap(_.toIntOption)
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
