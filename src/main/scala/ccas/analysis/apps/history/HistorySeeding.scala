package ccas.analysis.apps.history

import java.time.Instant
import com.augustnagro.magnum.{Transactor, sql}
import zio.{RIO, Ref, ZIO}
import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerMatches}
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO
import HistoryUtils.*

private[history] object HistorySeeding {

  /** Retries resolution for clubs previously recorded in `unresolved_match_club`. Groups entries by slug so each
    * unique club is resolved at most once. On success, patches all matching `club_match` rows and removes the
    * unresolved entries. Returns total count of resolved entries.
    */
  def retryUnresolvedClubs(client: ChessComClient): RIO[CcasLogger & Transactor, Int] =
    for {
      unresolved <- UnresolvedMatchClub.selectAll
      result <- if (unresolved.isEmpty) { ZIO.succeed(0) } else {
        val grouped = unresolved.groupBy(_.slug)
        val total = grouped.size
        CcasLogger.info(s"  Retrying ${unresolved.size} unresolved clubs ($total unique)...") *>
        ZIO.scoped {
          for {
            bar        <- CcasLogger.progressBar
            counterRef <- Ref.make(0)
            resolvedRef <- Ref.make(0)
            _ <- ZIO.foreachDiscard(grouped.toList) { case (slug, entries) =>
              Club.resolveOrFetch(client, slug).flatMap {
                case Some(clubId) =>
                  ZIO.foreachDiscard(entries) { entry =>
                    ClubMatch.updateTeamClubId(entry.matchId, entry.isTeam1, clubId) *>
                      UnresolvedMatchClub.delete(entry.matchId, entry.isTeam1)
                  } *> resolvedRef.update(_ + entries.size)
                case None => ZIO.unit
              }.catchAll(_ => ZIO.unit) *>
                counterRef.updateAndGet(_ + 1).flatMap(n => bar.print(n, total, s"  Retrying unresolved clubs: $n/$total"))
            }
            resolved <- resolvedRef.get
          } yield resolved
        }
      }
    } yield result

  /** Retries resolution for players previously recorded in `unresolved_board_player`. Groups entries by username so
    * each unique player is fetched at most once. On success, ensures the player exists in the DB, patches all matching
    * `club_match_board` rows, and removes the unresolved entries. Returns total count of resolved entries.
    */
  def retryUnresolvedPlayers(client: ChessComClient): RIO[CcasLogger & Transactor, Int] =
    for {
      unresolved <- UnresolvedBoardPlayer.selectAll
      result <- if (unresolved.isEmpty) { ZIO.succeed(0) } else {
        val grouped = unresolved.groupBy(_.username)
        val total = grouped.size
        CcasLogger.info(s"  Retrying ${unresolved.size} unresolved players ($total unique)...") *>
        ZIO.scoped {
          for {
            bar        <- CcasLogger.progressBar
            counterRef <- Ref.make(0)
            resolvedRef <- Ref.make(0)
            _ <- ZIO.foreachDiscard(grouped.toList) { case (username, entries) =>
              (for {
                apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
                playerId = apiPlayer.playerId
                _ <- Player.selectId(playerId).flatMap {
                  case Some(_) => ZIO.unit
                  case None =>
                    val joined = Instant.ofEpochSecond(apiPlayer.joined)
                    connectZIO {
                      sql"""INSERT INTO player (player_id, joined) VALUES ($playerId, $joined)
                            ON CONFLICT (player_id) DO NOTHING""".update.run()
                    } *> PlayerSnapshot.insert(PlayerSnapshot(
                      playerId, Instant.now(), username, apiPlayer.status.category, apiPlayer.title
                    )).unit
                }
                _ <- ZIO.foreachDiscard(entries) { entry =>
                  ClubMatchBoard.updatePlayerId(entry.matchId, entry.board, entry.isTeam1, playerId) *>
                    UnresolvedBoardPlayer.delete(entry.matchId, entry.board, entry.isTeam1)
                }
                _ <- resolvedRef.update(_ + entries.size)
              } yield ()).catchAll(_ => ZIO.unit) *>
                counterRef.updateAndGet(_ + 1).flatMap(n => bar.print(n, total, s"  Retrying unresolved players: $n/$total"))
            }
            resolved <- resolvedRef.get
          } yield resolved
        }
      }
    } yield result

  /** Fetches the club's match listing endpoint and inserts any not-yet-known match IDs as pending. */
  private[history] def seedFromClubMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug
  ): RIO[CcasLogger & Transactor, Int] =
    (for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))
      allDaily = clubMatches.dailyFinished ++ clubMatches.dailyInProgress ++ clubMatches.dailyRegistered
      dailyPending = allDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false))
      nonDaily = clubMatches.finished.filterNot(_.timeClass.isDaily) ++
        clubMatches.inProgress.filterNot(_.timeClass.isDaily) ++
        clubMatches.registered.filterNot(_.timeClass.isDaily)
      livePending = nonDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true))
      all = dailyPending ++ livePending
      knownIds <- ClubMatch.selectMatchIdsForClub(clubId)
      newOnly = all.filterNot(p => knownIds.contains(p.matchId))
      _ <- insertPendingMatches(newOnly)
    } yield newOnly.size).catchAll { error =>
      CcasLogger.warn(s"  Failed to fetch club matches: ${error.getMessage}").as(0)
    }

  /** Queries each un-queried member's match list to find club match IDs. Skips already-queried members unless --full. */
  def seedFromMemberMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    allMembers: List[ClubMember],
    queriedIds: Set[PlayerId],
    snapByPlayerId: Map[PlayerId, PlayerSnapshot],
    settledMatchIds: Set[ClubMatchId]
  ): RIO[CcasLogger & Transactor, MemberSeedResult] = {
    val toQuery = allMembers
      .filterNot(m => queriedIds.contains(m.playerId))
      .flatMap(m => snapByPlayerId.get(m.playerId).map(s => (m.playerId, s.username)))
      .distinctBy(_._1)
    for {
      counterRef       <- Ref.make(0)
      seedRef          <- Ref.make(0)
      failRef          <- Ref.make(0)
      failedMembersRef <- Ref.make(List.empty[(Username, String)])
      total = toQuery.size
      _ <- ZIO.scoped {
        for {
          bar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(toQuery) { case (playerId, username) =>
            seedMatchesForPlayer(client, clubId, clubSlug, playerId, username, settledMatchIds).foldZIO(
              error => failRef.update(_ + 1)
                *> failedMembersRef.update(_ :+ (username, error.getMessage))
                *> CcasLogger.warn(s"  $username: failed — ${error.getMessage}"),
              count => seedRef.update(_ + count) *> counterRef.updateAndGet(_ + 1).flatMap { n =>
                bar.print(n, total, s"  Querying member matches: $n/$total")
              }
            )
          }
        } yield ()
      }
      seeded        <- seedRef.get
      queried       <- counterRef.get
      failed        <- failRef.get
      failedMembers <- failedMembersRef.get
    } yield MemberSeedResult(seeded, queried, failed, failedMembers)
  }

  private[history] def isClubDailyMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.map(ClubSlug.wrap).contains(clubSlug)
      && !m.`@id`.path.segments.contains("live")

  private[history] def isClubLiveMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.map(ClubSlug.wrap).contains(clubSlug)
      && m.`@id`.path.segments.contains("live")

  def seedMatchesForPlayer(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    playerId: PlayerId,
    username: Username,
    settledMatchIds: Set[ClubMatchId]
  ): RIO[Transactor, Int] =
    for {
      playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(username))
      allMatches = playerMatches.finished ++ playerMatches.inProgress ++ playerMatches.registered
      dailyPending = allMatches.collect {
        case m if isClubDailyMatch(m, clubSlug) =>
          HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false)
      }
      livePending = allMatches.collect {
        case m if isClubLiveMatch(m, clubSlug) =>
          HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true)
      }
      all = (dailyPending ++ livePending).filterNot(p => settledMatchIds.contains(p.matchId))
      _ <- insertPendingMatches(all)
      _ <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubId, playerId, Instant.now()))
    } yield all.size

  private def insertPendingMatches(items: Iterable[HistoryPendingMatch]): RIO[Transactor, Unit] =
    ZIO.foreachDiscard(items.grouped(1000).toList)(HistoryPendingMatch.insertBatch)

  /** If --refresh, re-queues all known matches; otherwise only stale ones (unfinished or recently completed). */
  def seedStaleMatches(clubId: ClubId, refresh: Boolean): RIO[Transactor, Int] =
    if (refresh) {
      for {
        matchIds <- ClubMatch.selectMatchIdsForClub(clubId)
        _        <- insertPendingMatches(matchIds.map(id => HistoryPendingMatch(clubId, id, isLive = false)))
      } yield matchIds.size
    } else {
      for {
        staleIds <- ClubMatch.selectStaleForClub(clubId)
        _        <- insertPendingMatches(staleIds.map(id => HistoryPendingMatch(clubId, id, isLive = false)))
      } yield staleIds.size
    }
}
