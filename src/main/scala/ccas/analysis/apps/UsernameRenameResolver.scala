package ccas.analysis.apps

import zio.http.URL
import zio.{RIO, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.tables.*
import ccas.api.clubmatch.ApiMatchBoard
import ccas.api.clubmatch.ApiMatchBoard.ApiBoardPlayer
import ccas.api.clubmatch.TeamMatchPlayerStarted
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.api.tournament.ApiTournament
import ccas.utils.client.{ChessComClient, HttpStatusException, ReportedNotFound, onNotFound, swallowRecoveryErrors}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

/** Resolves the current canonical username for a player whose previously-known username 404s on Chess.com.
  *
  *  - **Tier A (DB lookup)** — never fires HTTP: `Player.selectByUsername` plus
  *    [[PlayerSnapshot.selectLatestPlayerIdByUsername]], for renames our DB already learned by another path.
  *  - **Tier B (board endpoint)** — only when Tier A returns `None` and a `playerIdHint` is supplied. Fetches a
  *    `PlayerMatchRef` board and identifies the player by eliminating the opposing side's canonical name, optionally
  *    falling back to `PlayerTournamentRef`.
  *
  * A verification fetch confirms the `playerId` before the name is returned; [[resolveAndReconcile]] also runs
  * `PlayerUpdater.reconcile` so future lookups skip the resolver. Tombstoned rows (`_stale_<playerId>`) are never
  * returned as fresh.
  *
  * Why the entry points gate on [[ccas.utils.client.ReportedNotFound]] rather than any 404, and what a failed
  * resolution does: `docs/adr/0010-rename-recovery-for-usernames-and-club-slugs.md`.
  */
object UsernameRenameResolver {

  /** Delegates to [[Player.isTombstoneUsername]] — single source of truth for the tombstone format. */
  def isTombstone(u: Username): Boolean = Player.isTombstoneUsername(u)

  def stalePlaceholder(playerId: PlayerId): Username =
    Username.wrap(s"_stale_${PlayerId.unwrap(playerId)}")

  /** Returns the current canonical username when `staleUsername` 404s, or `None` if no rename can be inferred.
    * Includes a verification fetch but does NOT update the `player` table. Callers that want the verified
    * `ApiPlayer` and a side-effecting reconcile should use [[resolveAndReconcile]] instead.
    */
  def resolveCurrentUsername(
    client: ChessComClient,
    staleUsername: Username,
    playerIdHint: Option[PlayerId],
    tournamentFallback: Boolean = true
  ): RIO[PostgresClient, Option[Username]] =
    resolveCandidate(staleUsername, playerIdHint, client, tournamentFallback).flatMap {
      case None => ZIO.none
      case Some(candidate) =>
        verify(client, candidate, playerIdHint).map(_.map(_._1))
    }

  /** Resolves the current canonical username AND verifies via `ApiPlayer` fetch. Does NOT update the `player` table
    * — callers that batch the rename into their own transaction (alongside other writes) should use this and run
    * `PlayerUpdater.reconcile` themselves so the rename and downstream writes commit atomically. Callers that just
    * want the rename persisted with no other side effects should use [[resolveAndReconcile]].
    */
  def resolveAndVerify(
    client: ChessComClient,
    staleUsername: Username,
    playerIdHint: Option[PlayerId],
    tournamentFallback: Boolean = true
  ): RIO[PostgresClient, Option[(Username, ApiPlayer)]] =
    resolveCandidate(staleUsername, playerIdHint, client, tournamentFallback).flatMap {
      case None            => ZIO.none
      case Some(candidate) => verify(client, candidate, playerIdHint)
    }

  /** Resolves the current canonical username AND persists the discovery via `PlayerUpdater.reconcile` in a dedicated
    * transaction. Returns the verified `ApiPlayer`. Use this when the caller has no other writes to coordinate;
    * otherwise prefer [[resolveAndVerify]] + caller-side reconcile inside the caller's existing transaction.
    */
  def resolveAndReconcile(
    client: ChessComClient,
    staleUsername: Username,
    playerIdHint: Option[PlayerId],
    tournamentFallback: Boolean = true
  ): RIO[PostgresClient, Option[(Username, ApiPlayer)]] =
    resolveAndVerify(client, staleUsername, playerIdHint, tournamentFallback).tap {
      case Some((_, apiPlayer)) => withTransaction(PlayerUpdater.reconcile(apiPlayer, client))
      case None                 => ZIO.unit
    }

  /** Convenience: fetches `/pub/player/{username}` and falls back to rename recovery on 404. Returns the verified
    * (post-recovery) `ApiPlayer`; on 404 with no rename inferred, the original 404 propagates. Suitable for callers
    * that will run their own `PlayerUpdater.reconcile` inside a transaction with subsequent writes.
    */
  def fetchOrRecover(
    client: ChessComClient,
    username: Username,
    playerIdHint: Option[PlayerId] = None
  ): RIO[PostgresClient, ApiPlayer] =
    client.getUncached[ApiPlayer](ApiPlayer.getUrl(username)).catchSome {
      case e: ReportedNotFound =>
        resolveAndVerify(client, username, playerIdHint).flatMap {
          case Some((_, apiPlayer)) => ZIO.succeed(apiPlayer)
          case None                 => ZIO.fail(e)
        }
    }

  /** Board-keyed entry point used by `HistorySeeding.retryUnresolvedPlayers`. Given a stale username plus the board
    * coordinates from `unresolved_board_player`, fetches the board endpoint, eliminates the opposing side's known
    * canonical name, and returns the surviving username. Verification is the caller's responsibility (the existing
    * unresolved-player retry flow already calls `resolveByUsername` after this).
    */
  def resolveFromBoard(
    client: ChessComClient,
    staleUsername: Username,
    matchId: ClubMatchId,
    board: Short,
    isTeam1: Boolean,
    isLive: Boolean
  ): RIO[PostgresClient, Option[Username]] =
    for {
      boardData <- fetchBoard(client, matchId, board, isLive)
      usernames = extractBoardUsernames(boardData)
      result <-
        if (usernames.size != 2) { ZIO.none }
        else {
          opposingCurrentUsername(client, matchId, board, isTeam1, isLive).map(_.flatMap { other =>
            usernames.filterNot(_ == other) match {
              case ours :: Nil if ours != staleUsername => Some(ours)
              case _                                    => None
            }
          })
        }
    } yield result

  // ---- Tier A: DB lookup ----

  private def tierADb(
    staleUsername: Username,
    playerIdHint: Option[PlayerId]
  ): RIO[PostgresClient, Option[Username]] =
    Player.selectByUsername(staleUsername).flatMap {
      case Some(currentHolder) =>
        playerIdHint match {
          // Hint matches the current holder of the stale username — this is NOT a rename. The 404 is a deletion.
          case Some(hint) if hint == currentHolder.playerId => ZIO.none
          // Hint differs from the current holder: the freed handle has been recycled. Look up our hint's current name.
          case Some(hint) => Player.selectId(hint).map(_.map(_.username).filterNot(isTombstone))
          // No hint — can't disambiguate deletion vs recycle, default conservative.
          case None => ZIO.none
        }
      case None =>
        // No current player holds the stale username; check the snapshot reverse-lookup.
        PlayerSnapshot.selectLatestPlayerIdByUsername(staleUsername).flatMap { candidates =>
          playerIdHint match {
            case Some(hint) if candidates.contains(hint) =>
              Player.selectId(hint).map(_.map(_.username).filterNot(isTombstone))
            case Some(_) => ZIO.none
            case None =>
              candidates match {
                case pid :: Nil => Player.selectId(pid).map(_.map(_.username).filterNot(isTombstone))
                case _          => ZIO.none // ambiguous (multiple historical holders) or empty
              }
          }
        }
    }

  // ---- Tier B: Board endpoint trick ----

  private def tierBBoard(
    client: ChessComClient,
    playerId: PlayerId,
    staleUsername: Username,
    tournamentFallback: Boolean
  ): RIO[PostgresClient, Option[Username]] =
    PlayerMatchRef.findOrInfer(playerId).flatMap {
      case Some(ref) => resolveFromBoard(client, staleUsername, ref.matchId, ref.boardIdx, ref.isTeam1, ref.isLive)
      case None =>
        if (tournamentFallback) {
          PlayerTournamentRef.selectId(playerId).flatMap {
            case None      => ZIO.none
            case Some(ref) => resolveFromTournament(client, ref, staleUsername)
          }
        } else { ZIO.none }
    }.swallowRecoveryErrors(s"Tier B rename recovery for $staleUsername")

  private def fetchBoard(
    client: ChessComClient,
    matchId: ClubMatchId,
    board: Short,
    isLive: Boolean
  ): RIO[Any, ApiMatchBoard] = {
    val url = if (isLive) { ApiMatchBoard.liveUrl(matchId, board.toInt) }
    else { ApiMatchBoard.dailyUrl(matchId, board.toInt) }
    client.get[ApiMatchBoard](url)
  }

  private[apps] def extractBoardUsernames(boardData: ApiMatchBoard): List[Username] =
    boardData.games.toList.flatMap { game =>
      List(extractSideUsername(game.white), extractSideUsername(game.black)).flatten
    }.distinct

  /** Returns the side's username, or `None` for closed-account placeholders that decode as a bare URL with an empty
    * path (defensive against Chess.com edge cases — every observed URL has a `/pub/player/<name>` shape, but the
    * resolver should not throw if the API ever ships an unusual response).
    */
  private[apps] def extractSideUsername(side: Either[URL, ApiBoardPlayer]): Option[Username] =
    side match {
      case Right(player) => Some(player.username)
      case Left(url)     => url.path.segments.lastOption.map(Username.wrap)
    }

  /** Finds the opposing player's current username on this board, preferring the DB-first path. If the opposing side
    * is already linked on `club_match_board`, reads their current username from `player`. Otherwise falls back to
    * the match endpoint's match-time username (still authoritative for the common case where only one side was
    * renamed). The fallback dispatches to the daily or live match endpoint via `RefHelpers.fetchTeamMatchTeams`.
    */
  private def opposingCurrentUsername(
    client: ChessComClient,
    matchId: ClubMatchId,
    board: Short,
    isTeam1: Boolean,
    isLive: Boolean
  ): RIO[PostgresClient, Option[Username]] =
    for {
      rows <- ClubMatchBoard.selectMatch(matchId)
      opposingPidOpt = rows.find(_.board == board).flatMap { row =>
        if (isTeam1) { row.team2PlayerId } else { row.team1PlayerId }
      }
      result <- opposingPidOpt match {
        case Some(pid) => Player.selectId(pid).map(_.map(_.username).filterNot(isTombstone))
        case None      => opposingUsernameFromMatchEndpoint(client, matchId, board, isTeam1, isLive)
      }
    } yield result

  private def opposingUsernameFromMatchEndpoint(
    client: ChessComClient,
    matchId: ClubMatchId,
    board: Short,
    isTeam1: Boolean,
    isLive: Boolean
  ): RIO[Any, Option[Username]] =
    RefHelpers.fetchTeamMatchTeams(client, matchId, isLive).map { teams =>
      val opposingTeam = if (isTeam1) { teams.team2 } else { teams.team1 }
      opposingTeam.players.collectFirst {
        case p: TeamMatchPlayerStarted if p.board.path.segments.lastOption.exists(_.toShort == board) => p.username
      }
    }.catchSome { case _: HttpStatusException => ZIO.none }

  private def resolveFromTournament(
    client: ChessComClient,
    ref: PlayerTournamentRef,
    staleUsername: Username
  ): RIO[Any, Option[Username]] =
    client.get[ApiTournament](ApiTournament.getUrl(ref.tournamentSlug)).map { tournament =>
      tournament.players.lift(ref.playerIdx).map(_.username).filter(_ != staleUsername)
    }.catchSome { case _: HttpStatusException => ZIO.none }

  // ---- Orchestration ----

  /** Tier A first; if it returns None and we have a hint, try Tier B. Any failure inside Tier A or Tier B is
    * swallowed and logged at debug — recovery must never replace the caller's original 404 with a different error,
    * since the caller's onNotFound block uses the resolver's `None` to signal "fall through to original failure."
    */
  private def resolveCandidate(
    staleUsername: Username,
    playerIdHint: Option[PlayerId],
    client: ChessComClient,
    tournamentFallback: Boolean
  ): RIO[PostgresClient, Option[Username]] =
    tierADb(staleUsername, playerIdHint).flatMap {
      case some @ Some(_) => ZIO.succeed(some)
      case None =>
        playerIdHint match {
          case Some(pid) => tierBBoard(client, pid, staleUsername, tournamentFallback)
          case None      => ZIO.none
        }
    }.swallowRecoveryErrors(s"resolver for $staleUsername")

  /** Verification fetch. Confirms the candidate username resolves on Chess.com and (when hint is present) that the
    * playerId matches our expectation. Returns `None` on 404 (resolver guessed wrong) or on playerId mismatch.
    */
  private def verify(
    client: ChessComClient,
    candidate: Username,
    playerIdHint: Option[PlayerId]
  ): RIO[Any, Option[(Username, ApiPlayer)]] =
    client.getUncached[ApiPlayer](ApiPlayer.getUrl(candidate)).map { apiPlayer =>
      val matches = playerIdHint.forall(_ == apiPlayer.playerId)
      Option.when(matches)((apiPlayer.username, apiPlayer))
    }.onNotFound(_ => ZIO.none)
}

/** Adds a `withPlayerRenameRecovery` combinator that wraps any 404-prone effect with the resolver and a caller-
  * supplied retry. The caller passes the URL's username and an optional `playerIdHint`; on a 404 we attempt the
  * full Tier-A → Tier-B → verification → reconcile flow and re-execute `retryWith(freshUsername)` on success. Any
  * other failure (or a resolver miss) propagates the original error.
  */
extension [R, A](self: ZIO[R, Throwable, A])
  def withPlayerRenameRecovery(
    client: ChessComClient,
    stale: Username,
    playerIdHint: Option[PlayerId]
  )(retryWith: Username => ZIO[R, Throwable, A])
    : ZIO[R & PostgresClient, Throwable, A] =
    self.catchSome { case e: ReportedNotFound =>
      UsernameRenameResolver.resolveAndReconcile(client, stale, playerIdHint).flatMap {
        case Some((fresh, _)) =>
          ZIO.logInfo(s"  Rename recovered: $stale → $fresh; retrying") *> retryWith(fresh)
        case None => ZIO.fail(e)
      }
    }
