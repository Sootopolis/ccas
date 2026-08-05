package ccas.analysis.apps.ref

import java.time.Instant

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{Promise, RIO, Task, UIO, ZIO}
import zio.http.URL
import RefUtils.*

import ccas.analysis.apps.ref.RefHelpers.parseMatchUrl
import ccas.analysis.apps.{ClubSlugRenameResolver, UsernameRenameResolver}
import ccas.analysis.tables.{
  ClubMatch,
  ClubMatchBoard,
  ClubMatchRef,
  ClubRefSkip,
  MatchKey,
  PlayerMatchRef,
  PlayerRefSkip,
  PlayerTournamentRef,
  RefSkipReason
}
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.TeamMatchTeams
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, TournamentSlug, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerMatches, ApiPlayerTournaments}
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch
import ccas.api.tournament.ApiTournamentRound
import ccas.utils.client.{ChessComClient, NetworkUnavailableException, ReportedNotFound}
import ccas.utils.errors.safeMessage
import ccas.utils.ProgressDisplay

private[ref] object RefResolution {

  // --- Player resolution ---

  def resolvePlayer(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[ProgressDisplay & PostgresClient, Boolean] =
    (for {
      dbRef <- ClubMatchBoard.inferPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) =>
          for {
            _ <- withTransaction {
              PlayerMatchRef.upsert(ref) *> PlayerRefSkip.deleteId(player.playerId)
            }
            _ <- ctx.playersResolvedDb.update(_ + 1)
            _ <- ZIO.logDebug(s"  ${player.username}: resolved via DB")
          } yield true
        case None =>
          resolvePlayerViaMatch(ctx, player, countResolved = true).flatMap {
            case ResolveResult.Resolved   => ZIO.succeed(true)
            case ResolveResult.SkipPlayer => skipPlayer(ctx, player, RefSkipReason.IdMismatch).as(false)
            case matchOutcome => // NotFound or NoData
              resolvePlayerViaTournament(ctx, player).flatMap {
                case ResolveResult.Resolved   => ZIO.succeed(true)
                case ResolveResult.SkipPlayer => skipPlayer(ctx, player, RefSkipReason.IdMismatch).as(false)
                case tournamentOutcome =>
                  val reason = (matchOutcome, tournamentOutcome) match {
                    case (ResolveResult.NoData, ResolveResult.NoData) => RefSkipReason.NoData
                    case _                                            => RefSkipReason.ResolutionFailed
                  }
                  skipPlayer(ctx, player, reason).as(false)
              }
          }
      }
    } yield resolved).catchAll {
      case e: ReportedNotFound =>
        // Canonical `"X not found."` 404 body — genuine rename-or-deletion. Try the rename resolver before recording
        // a permanent NotFound skip. The resolver returns Some only on rename (with the canonical fresh handle
        // reconciled into the Player table); on deletion or unresolvable cases it returns None and we fall back to
        // the NotFound skip. Recursion terminates because `resolveAndReconcile` updates Player.username before the
        // recursive call: a second 404 on the same playerId hits Tier A's deletion case (hint matches current
        // holder → None) and falls through to skipPlayer. Transient 404s (Chess.com backend hiccup, `error_type`
        // stays `HttpStatusException` not `ReportedNotFound`) fall through to the generic arm below and skip via
        // `ApiError`, picked up next run by the short `ApiError` retry window — see issue #3.
        for {
          recovered <- UsernameRenameResolver.resolveAndReconcile(ctx.client, player.username, Some(player.playerId))
          result <- recovered match {
            case Some((fresh, _)) =>
              ZIO.logInfo(s"  ${player.username}: rename recovered → $fresh; retrying resolution") *>
                resolvePlayer(ctx, player.copy(username = fresh))
            case None =>
              for {
                _ <- ZIO.logWarning(s"  ${player.username}: 404 — ${e.safeMessage}")
                _ <- skipPlayer(ctx, player, RefSkipReason.NotFound, Some(e.safeMessage))
              } yield false
          }
        } yield result
      case e: NetworkUnavailableException =>
        // Systemic network/DNS outage (survived the client's retry schedule), not this player's fault. Abort the run
        // instead of recording a bogus ApiError skip that would suppress the player for 3 days; the next run retries.
        ZIO.fail(e)
      case error =>
        for {
          _ <- ZIO.logWarning(s"  ${player.username}: error — ${error.safeMessage}")
          _ <- skipPlayer(ctx, player, RefSkipReason.ApiError, Some(error.safeMessage))
        } yield false
    }

  private def resolvePlayerViaMatch(
    ctx: RefContext,
    player: UnresolvedPlayer,
    countResolved: Boolean
  ): RIO[ProgressDisplay & PostgresClient, ResolveResult] =
    ctx.client.getCacheable[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username)).flatMap { result =>
      def iterate(playerMatches: ApiPlayerMatches): RIO[ProgressDisplay & PostgresClient, ResolveResult] = {
        val candidates = (playerMatches.finished ++ playerMatches.inProgress).filter(_.board.isDefined)
        if (candidates.isEmpty) {
          ZIO.logDebug(s"  ${player.username}: no match with board").as(ResolveResult.NoData)
        } else {
          tryMatches(ctx, player, candidates.toList, countResolved)
        }
      }
      unchangedGate(result, PlayerRefSkip.selectId(player.playerId))(
        for {
          _ <- ctx.playerMatchesUnchanged.update(_ + 1)
          _ <- ZIO.logDebug(s"  ${player.username}: player-matches unchanged, skipping candidate iteration")
        } yield ResolveResult.NotFound
      )(iterate)
    }

  private def tryMatches(
    ctx: RefContext,
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerMatch],
    countResolved: Boolean
  ): RIO[ProgressDisplay & PostgresClient, ResolveResult] =
    ZIO.foldLeft(candidates)(ResolveResult.NotFound: ResolveResult) { (status, m) =>
      status match {
        case ResolveResult.NotFound => tryOneMatch(ctx, player, m, countResolved)
        case other                  => ZIO.succeed(other)
      }
    }

  private def tryOneMatch(
    ctx: RefContext,
    player: UnresolvedPlayer,
    m: ApiPlayerMatch,
    countResolved: Boolean
  ): RIO[ProgressDisplay & PostgresClient, ResolveResult] = {
    val parsed      = parseMatchUrl(m.`@id`)
    val boardIdxOpt = m.board.get.path.segments.lastOption.flatMap(_.toIntOption).map(_.toShort)
    boardIdxOpt match {
      case None =>
        ZIO.logDebug(s"  ${player.username}: malformed board URL ${m.board.get}").as(ResolveResult.NotFound)
      case Some(boardIdx) =>
        isFailedUrl(ctx, parsed.matchUrl).flatMap {
          case true => ZIO.succeed(ResolveResult.NotFound)
          case false =>
            fetchMatch(ctx, parsed.matchId, parsed.isLive).foldZIO(
              error =>
                NetworkUnavailableException.recoverUnless(error)(
                  recordFailedUrl(ctx, parsed.matchUrl, error, "player").as(ResolveResult.NotFound)
                ),
              teams =>
                RefHelpers.findPlayerIsTeam1(teams, player.username) match {
                  case None => ZIO.succeed(ResolveResult.NotFound)
                  case Some(isTeam1) =>
                    handleVerification(ctx, player) {
                      val ref = PlayerMatchRef(player.playerId, parsed.matchId, parsed.isLive, isTeam1, boardIdx)
                      for {
                        _ <- withTransaction {
                          PlayerMatchRef.upsert(ref) *> PlayerRefSkip.deleteId(player.playerId)
                        }
                        _ <- ZIO.whenDiscard(countResolved)(ctx.playersResolvedApi.update(_ + 1))
                      } yield ResolveResult.Resolved
                    }
                }
            )
        }
    }
  }

  private def resolvePlayerViaTournament(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[ProgressDisplay & PostgresClient, ResolveResult] =
    ctx.client.getCacheable[ApiPlayerTournaments](ApiPlayerTournaments.getUrl(player.username)).flatMap { result =>
      def iterate(playerTournaments: ApiPlayerTournaments): RIO[ProgressDisplay & PostgresClient, ResolveResult] = {
        val eligible = (playerTournaments.finished ++ playerTournaments.inProgress)
          .sortBy(_.totalPlayers.getOrElse(Int.MaxValue))
        if (eligible.isEmpty) {
          ZIO.logDebug(s"  ${player.username}: no eligible tournaments").as(ResolveResult.NoData)
        } else {
          tryTournaments(ctx, player, eligible.toList)
        }
      }
      unchangedGate(result, PlayerRefSkip.selectId(player.playerId))(
        for {
          _ <- ctx.playerTournamentsUnchanged.update(_ + 1)
          _ <- ZIO.logDebug(s"  ${player.username}: player-tournaments unchanged, skipping candidate iteration")
        } yield ResolveResult.NotFound
      )(iterate)
    }

  private def tryTournaments(
    ctx: RefContext,
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerTournaments.ApiPlayerTournament]
  ): RIO[ProgressDisplay & PostgresClient, ResolveResult] =
    ZIO.foldLeft(candidates)(ResolveResult.NotFound: ResolveResult) { (status, t) =>
      status match {
        case ResolveResult.NotFound => tryOneTournament(ctx, player, t)
        case other                  => ZIO.succeed(other)
      }
    }

  private def tryOneTournament(
    ctx: RefContext,
    player: UnresolvedPlayer,
    t: ApiPlayerTournaments.ApiPlayerTournament
  ): RIO[ProgressDisplay & PostgresClient, ResolveResult] = {
    val slug     = TournamentSlug.fromUrl(t.`@id`)
    val roundUrl = ApiTournamentRound.getUrl(slug, 1)
    isFailedUrl(ctx, roundUrl).flatMap {
      case true => ZIO.succeed(ResolveResult.NotFound)
      case false =>
        ctx.client.get[ApiTournamentRound](roundUrl).foldZIO(
          error =>
            NetworkUnavailableException.recoverUnless(error)(
              recordFailedUrl(ctx, roundUrl, error, "player").as(ResolveResult.NotFound)
            ),
          round => {
            val playerIdx = round.players.indexWhere(rp => rp.username == player.username)
            if (playerIdx < 0) {
              ZIO.succeed(ResolveResult.NotFound)
            } else {
              handleVerification(ctx, player) {
                val ref = PlayerTournamentRef(player.playerId, slug, playerIdx)
                for {
                  _ <- withTransaction {
                    PlayerTournamentRef.upsert(ref) *> PlayerRefSkip.deleteId(player.playerId)
                  }
                  _ <- ctx.newTournamentRefPlayerIds.update(_ + player.playerId)
                  _ <- ctx.playersResolvedApi.update(_ + 1)
                } yield ResolveResult.Resolved
              }
            }
          }
        )
    }
  }

  // --- Club resolution ---

  def resolveClub(
    ctx: RefContext,
    club: UnresolvedClub
  ): RIO[ProgressDisplay & PostgresClient, Boolean] =
    (for {
      dbRef <- ClubMatch.inferClubMatchRef(club.clubId)
      resolved <- dbRef match {
        case Some(ref) =>
          for {
            _ <- withTransaction {
              ClubMatchRef.upsert(ref) *> ClubRefSkip.deleteId(club.clubId)
            }
            _ <- ctx.clubsResolvedDb.update(_ + 1)
            _ <- ZIO.logDebug(s"  ${club.slug}: resolved via DB")
          } yield true
        case None =>
          ctx.client.getCacheable[ApiClubMatches](ApiClubMatches.getUrl(club.slug)).flatMap { result =>
            def iterate(clubMatches: ApiClubMatches): RIO[ProgressDisplay & PostgresClient, Boolean] =
              if (clubMatches.finished.isEmpty) {
                skipClub(ctx, club, RefSkipReason.NoData).as(false)
              } else {
                tryClubMatches(ctx, club, clubMatches.finished.toList).flatMap {
                  case true  => ZIO.succeed(true)
                  case false => skipClub(ctx, club, RefSkipReason.ResolutionFailed).as(false)
                }
              }
            unchangedGate(result, ClubRefSkip.selectId(club.clubId))(
              for {
                _ <- ctx.clubMatchesUnchanged.update(_ + 1)
                _ <- ZIO.logDebug(s"  ${club.slug}: club-matches unchanged, skipping candidate iteration")
                _ <- skipClub(ctx, club, RefSkipReason.ResolutionFailed)
              } yield false
            )(iterate)
          }
      }
    } yield resolved).catchAll {
      case e: ReportedNotFound =>
        // Symmetric to `resolvePlayer` above. Recursion terminates because `resolveAndPersist` updates Club.slug
        // before the recursive call: a second 404 on the same clubId hits Tier A's `current.slug == staleSlug`
        // case (returns None) and falls through to skipClub. Transient 404s fall through to the generic arm below
        // and skip via `ApiError`.
        for {
          recovered <- ClubSlugRenameResolver.resolveAndPersist(ctx.client, club.slug, Some(club.clubId))
          result <- recovered match {
            case Some((fresh, _)) =>
              ZIO.logInfo(s"  ${club.slug}: slug rename recovered → $fresh; retrying resolution") *>
                resolveClub(ctx, club.copy(slug = fresh))
            case None =>
              for {
                _ <- ZIO.logWarning(s"  ${club.slug}: 404 — ${e.safeMessage}")
                _ <- skipClub(ctx, club, RefSkipReason.NotFound, Some(e.safeMessage))
              } yield false
          }
        } yield result
      case e: NetworkUnavailableException =>
        // Systemic network/DNS outage — abort rather than record a bogus ApiError skip (symmetric to resolvePlayer).
        ZIO.fail(e)
      case error =>
        for {
          _ <- ZIO.logWarning(s"  ${club.slug}: error — ${error.safeMessage}")
          _ <- skipClub(ctx, club, RefSkipReason.ApiError, Some(error.safeMessage))
        } yield false
    }

  private def tryClubMatches(
    ctx: RefContext,
    club: UnresolvedClub,
    candidates: List[ApiClubMatches.ApiClubMatchFinished]
  ): RIO[ProgressDisplay & PostgresClient, Boolean] =
    ZIO.foldLeft(candidates)(false) { (resolved, m) =>
      if (resolved) { ZIO.succeed(true) }
      else { tryOneClubMatch(ctx, club, m) }
    }

  private def tryOneClubMatch(
    ctx: RefContext,
    club: UnresolvedClub,
    m: ApiClubMatches.ApiClubMatchFinished
  ): RIO[PostgresClient, Boolean] = {
    val parsed = parseMatchUrl(m.`@id`)
    isFailedUrl(ctx, parsed.matchUrl).flatMap {
      case true => ZIO.succeed(false)
      case false =>
        fetchMatch(ctx, parsed.matchId, parsed.isLive).foldZIO(
          error =>
            NetworkUnavailableException.recoverUnless(error)(
              recordFailedUrl(ctx, parsed.matchUrl, error, "club").as(false)
            ),
          teams =>
            RefHelpers.findClubIsTeam1(teams, club.slug) match {
              case None => ZIO.succeed(false)
              case Some(isTeam1) =>
                val ref = ClubMatchRef(club.clubId, parsed.matchId, parsed.isLive, isTeam1)
                for {
                  _ <- withTransaction {
                    ClubMatchRef.upsert(ref) *> ClubRefSkip.deleteId(club.clubId)
                  }
                  _ <- ctx.clubsResolvedApi.update(_ + 1)
                } yield true
            }
        )
    }
  }

  // --- Unchanged-listing short-circuit helper ---

  /** Branch on [[CacheableResult]]: when the listing body is unchanged *and* there is evidence of a prior failed
    * resolution attempt for this subject (an expired skip row present in the unresolved pool), run `ifSkipped`
    * without decoding the body. Otherwise decode and run the full `onBody` pipeline.
    *
    * The skip-row existence check guards against a subtle false-positive: a cache entry may have been warmed by an
    * unrelated app (HistoryApp seeding player matches, say), so `isUnchanged` alone is not sufficient evidence
    * that *we* have tried and failed before. Only pool members carrying an expired skip row are safe to
    * short-circuit.
    */
  private def unchangedGate[T, A](
    result: ccas.utils.client.CacheableResult[T],
    priorSkip: RIO[PostgresClient, Option[?]]
  )(ifSkipped: RIO[ProgressDisplay & PostgresClient, A])(
    onBody: T => RIO[ProgressDisplay & PostgresClient, A]
  ): RIO[ProgressDisplay & PostgresClient, A] =
    if (result.isUnchanged) {
      priorSkip.flatMap {
        case Some(_) => ifSkipped
        case None    => result.getValue.flatMap(onBody)
      }
    } else {
      result.getValue.flatMap(onBody)
    }

  // --- Match fetching ---

  private def fetchMatch(
    ctx: RefContext,
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    for {
      promise <- Promise.make[Throwable, TeamMatchTeams]
      key = MatchKey(matchId, isLive)
      action <- ctx.cache.modify { m =>
        m.get(key) match {
          case Some(existing) => (existing.await, m)
          case None           => (fetchAndComplete(ctx.client, promise, matchId, isLive), m + (key -> promise))
        }
      }
      result <- action
    } yield result

  private def fetchAndComplete(
    client: ChessComClient,
    promise: Promise[Throwable, TeamMatchTeams],
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    RefHelpers.fetchTeamMatchTeams(client, matchId, isLive).tapBoth(promise.fail, promise.succeed)

  // --- Shared helpers ---

  private def verifyPlayerId(
    client: ChessComClient,
    username: Username,
    expectedPlayerId: PlayerId
  ): Task[Either[PlayerId, Unit]] =
    client.getUncached[ApiPlayer](ApiPlayer.getUrl(username)).map { apiPlayer =>
      if (apiPlayer.playerId == expectedPlayerId) { Right(()) }
      else { Left(apiPlayer.playerId) }
    }

  private def handleVerification(
    ctx: RefContext,
    player: UnresolvedPlayer
  )(onVerified: => RIO[PostgresClient, ResolveResult]): RIO[ProgressDisplay & PostgresClient, ResolveResult] =
    verifyPlayerId(ctx.client, player.username, player.playerId).foldZIO(
      error =>
        NetworkUnavailableException.recoverUnless(error)(
          ZIO.logWarning(s"  ${player.username}: verification error — ${error.safeMessage}").as(ResolveResult.NotFound)
        ),
      {
        case Left(actualId) =>
          for {
            _ <- ZIO.logWarning(
              s"  ${player.username}: player_id mismatch (expected ${player.playerId}, actual $actualId), skipping"
            )
            _ <- ctx.skippedPlayers.update(_ :+ SkippedPlayer(player.playerId, player.username))
          } yield ResolveResult.SkipPlayer
        case Right(()) => onVerified
      }
    )

  private def isFailedUrl(ctx: RefContext, url: URL): UIO[Boolean] =
    ctx.failedUrls.get.map(_.contains(url.encode))

  private def recordFailedUrl(ctx: RefContext, url: URL, error: Throwable, source: String): UIO[Unit] =
    ctx.failedUrls.update(_ + (url.encode -> error.safeMessage)) *>
      ctx.failedUrlSource.update(_ + (url.encode -> source))

  private def skipPlayer(
    ctx: RefContext,
    player: UnresolvedPlayer,
    reason: RefSkipReason,
    detail: Option[String] = None
  ): RIO[PostgresClient, Unit] =
    PlayerRefSkip.upsert(PlayerRefSkip(player.playerId, reason, detail, Instant.now())) *>
      ctx.playersSkippedNew.update(_ + 1)

  private def skipClub(
    ctx: RefContext,
    club: UnresolvedClub,
    reason: RefSkipReason,
    detail: Option[String] = None
  ): RIO[PostgresClient, Unit] =
    ClubRefSkip.upsert(ClubRefSkip(club.clubId, reason, detail, Instant.now())) *>
      ctx.clubsSkippedNew.update(_ + 1)

  // --- Upgrade: tournament ref → match ref ---

  /** Attempt to find a match ref for a player who currently only has a tournament ref. Returns true if upgraded
    * successfully. Does not create skip records or bump resolution counters. The caller is responsible for deleting the
    * old tournament ref on success.
    */
  private[ref] def tryUpgradeToMatchRef(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[ProgressDisplay & PostgresClient, Boolean] =
    (for {
      dbRef <- ClubMatchBoard.inferPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) =>
          for {
            _ <- PlayerMatchRef.upsert(ref)
            _ <- ZIO.logDebug(s"  ${player.username}: upgraded via DB")
          } yield true
        case None =>
          resolvePlayerViaMatch(ctx, player, countResolved = false).map {
            case ResolveResult.Resolved => true
            case _                      => false
          }
      }
    } yield resolved).catchAll(_ => ZIO.succeed(false))

  // --- Upgrade: tournament ref → smaller tournament ref ---

  /** Attempt to replace a tournament ref with one from a smaller tournament. Returns true if a better ref was found and
    * stored. Does not create skip records or bump resolution counters.
    */
  private[ref] def tryUpgradeTournamentRef(
    ctx: RefContext,
    trp: TournamentRefPlayer
  ): RIO[ProgressDisplay & PostgresClient, Boolean] =
    (for {
      playerTournaments <- ctx.client.get[ApiPlayerTournaments](ApiPlayerTournaments.getUrl(trp.username))
      eligible = (playerTournaments.finished ++ playerTournaments.inProgress)
        .sortBy(_.totalPlayers.getOrElse(Int.MaxValue))
      result <-
        if (eligible.isEmpty) ZIO.succeed(false)
        else tryBetterTournaments(ctx, trp, eligible.toList)
    } yield result).catchAll(_ => ZIO.succeed(false))

  private def tryBetterTournaments(
    ctx: RefContext,
    trp: TournamentRefPlayer,
    candidates: List[ApiPlayerTournaments.ApiPlayerTournament]
  ): RIO[ProgressDisplay & PostgresClient, Boolean] =
    ZIO.foldLeft(candidates)(Option.empty[Boolean]) { (done, t) =>
      done match {
        case Some(_) => ZIO.succeed(done)
        case None    => tryOneTournamentUpgrade(ctx, trp, t)
      }
    }.map(_.getOrElse(false))

  private def tryOneTournamentUpgrade(
    ctx: RefContext,
    trp: TournamentRefPlayer,
    t: ApiPlayerTournaments.ApiPlayerTournament
  ): RIO[ProgressDisplay & PostgresClient, Option[Boolean]] = {
    val slug = TournamentSlug.fromUrl(t.`@id`)
    if (slug == trp.tournamentSlug) { ZIO.succeed(Some(false)) }
    else {
      val roundUrl = ApiTournamentRound.getUrl(slug, 1)
      isFailedUrl(ctx, roundUrl).flatMap {
        case true => ZIO.succeed(None)
        case false =>
          ctx.client.get[ApiTournamentRound](roundUrl).foldZIO(
            error => recordFailedUrl(ctx, roundUrl, error, "player").as(None),
            round => {
              val playerIdx = round.players.indexWhere(rp => rp.username == trp.username)
              if (playerIdx < 0) { ZIO.succeed(None) }
              else {
                val ref = PlayerTournamentRef(trp.playerId, slug, playerIdx)
                for {
                  _ <- PlayerTournamentRef.upsert(ref)
                  _ <- ZIO.logDebug(s"  ${trp.username}: tournament ref upgraded to $slug")
                } yield Some(true)
              }
            }
          )
      }
    }
  }
}
