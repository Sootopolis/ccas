package ccas.analysis.apps.ref

import java.time.Instant

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{Promise, RIO, Task, UIO, ZIO}
import zio.http.URL
import RefUtils.*

import ccas.analysis.apps.ref.RefHelpers.parseMatchUrl
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
import ccas.utils.client.{ChessComClient, HttpStatusException}
import ccas.utils.errors.safeMessage
import ccas.utils.CcasLogger

private[ref] object RefResolution {

  // --- Player resolution ---

  def resolvePlayer(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[CcasLogger & PostgresClient, Boolean] =
    (for {
      dbRef <- ClubMatchBoard.selectPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) =>
          withTransaction {
            PlayerMatchRef.upsert(ref) *> PlayerRefSkip.deleteId(player.playerId)
          } *> ctx.playersResolvedDb.update(_ + 1) *>
            CcasLogger.debug(s"  ${player.username}: resolved via DB").as(true)
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
      case e: HttpStatusException if e.statusCode == 404 =>
        CcasLogger.warn(s"  ${player.username}: 404 — ${e.safeMessage}") *>
          skipPlayer(ctx, player, RefSkipReason.NotFound, Some(e.safeMessage)).as(false)
      case error =>
        CcasLogger.warn(s"  ${player.username}: error — ${error.safeMessage}") *>
          skipPlayer(ctx, player, RefSkipReason.ApiError, Some(error.safeMessage)).as(false)
    }

  private def resolvePlayerViaMatch(
    ctx: RefContext,
    player: UnresolvedPlayer,
    countResolved: Boolean
  ): RIO[CcasLogger & PostgresClient, ResolveResult] =
    for {
      playerMatches <- ctx.client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(player.username))
      candidates = (playerMatches.finished ++ playerMatches.inProgress).filter(_.board.isDefined)
      result <-
        if (candidates.isEmpty) {
          CcasLogger.debug(s"  ${player.username}: no match with board").as(ResolveResult.NoData)
        } else {
          tryMatches(ctx, player, candidates.toList, countResolved)
        }
    } yield result

  private def tryMatches(
    ctx: RefContext,
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerMatch],
    countResolved: Boolean
  ): RIO[CcasLogger & PostgresClient, ResolveResult] =
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
  ): RIO[CcasLogger & PostgresClient, ResolveResult] = {
    val parsed      = parseMatchUrl(m.`@id`)
    val boardIdxOpt = m.board.get.path.segments.lastOption.flatMap(_.toIntOption).map(_.toShort)
    boardIdxOpt match {
      case None =>
        CcasLogger.debug(s"  ${player.username}: malformed board URL ${m.board.get}").as(ResolveResult.NotFound)
      case Some(boardIdx) =>
        isFailedUrl(ctx, parsed.matchUrl).flatMap {
          case true => ZIO.succeed(ResolveResult.NotFound)
          case false =>
            fetchMatch(ctx, parsed.matchId, parsed.isLive).foldZIO(
              error => recordFailedUrl(ctx, parsed.matchUrl, error, "player").as(ResolveResult.NotFound),
              teams =>
                RefHelpers.findPlayerIsTeam1(teams, player.username) match {
                  case None => ZIO.succeed(ResolveResult.NotFound)
                  case Some(isTeam1) =>
                    handleVerification(ctx, player) {
                      val ref = PlayerMatchRef(player.playerId, parsed.matchId, parsed.isLive, isTeam1, boardIdx)
                      withTransaction {
                        PlayerMatchRef.upsert(ref) *> PlayerRefSkip.deleteId(player.playerId)
                      } *> ZIO.whenDiscard(countResolved)(ctx.playersResolvedApi.update(_ + 1)).as(ResolveResult.Resolved)
                    }
                }
            )
        }
    }
  }

  private def resolvePlayerViaTournament(
    ctx: RefContext,
    player: UnresolvedPlayer
  ): RIO[CcasLogger & PostgresClient, ResolveResult] =
    for {
      playerTournaments <- ctx.client.get[ApiPlayerTournaments](ApiPlayerTournaments.getUrl(player.username))
      eligible = (playerTournaments.finished ++ playerTournaments.inProgress)
        .sortBy(_.totalPlayers.getOrElse(Int.MaxValue))
      result <-
        if (eligible.isEmpty) {
          CcasLogger.debug(s"  ${player.username}: no eligible tournaments").as(ResolveResult.NoData)
        } else {
          tryTournaments(ctx, player, eligible.toList)
        }
    } yield result

  private def tryTournaments(
    ctx: RefContext,
    player: UnresolvedPlayer,
    candidates: List[ApiPlayerTournaments.ApiPlayerTournament]
  ): RIO[CcasLogger & PostgresClient, ResolveResult] =
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
  ): RIO[CcasLogger & PostgresClient, ResolveResult] = {
    val slug     = TournamentSlug.fromUrl(t.`@id`)
    val roundUrl = ApiTournamentRound.getUrl(slug, 1)
    isFailedUrl(ctx, roundUrl).flatMap {
      case true => ZIO.succeed(ResolveResult.NotFound)
      case false =>
        ctx.client.get[ApiTournamentRound](roundUrl).foldZIO(
          error => recordFailedUrl(ctx, roundUrl, error, "player").as(ResolveResult.NotFound),
          round => {
            val playerIdx = round.players.indexWhere(rp => rp.username == player.username)
            if (playerIdx < 0) {
              ZIO.succeed(ResolveResult.NotFound)
            } else {
              handleVerification(ctx, player) {
                val ref = PlayerTournamentRef(player.playerId, slug, playerIdx)
                withTransaction {
                  PlayerTournamentRef.upsert(ref) *> PlayerRefSkip.deleteId(player.playerId)
                } *> ctx.playersResolvedApi.update(_ + 1).as(ResolveResult.Resolved)
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
  ): RIO[CcasLogger & PostgresClient, Boolean] =
    (for {
      dbRef <- ClubMatch.selectClubMatchRef(club.clubId)
      resolved <- dbRef match {
        case Some(ref) =>
          withTransaction {
            ClubMatchRef.upsert(ref) *> ClubRefSkip.deleteId(club.clubId)
          } *> ctx.clubsResolvedDb.update(_ + 1) *>
            CcasLogger.debug(s"  ${club.slug}: resolved via DB").as(true)
        case None =>
          for {
            clubMatches <- ctx.client.get[ApiClubMatches](ApiClubMatches.getUrl(club.slug))
            result <-
              if (clubMatches.finished.isEmpty) {
                skipClub(ctx, club, RefSkipReason.NoData).as(false)
              } else {
                tryClubMatches(ctx, club, clubMatches.finished.toList).flatMap {
                  case true  => ZIO.succeed(true)
                  case false => skipClub(ctx, club, RefSkipReason.ResolutionFailed).as(false)
                }
              }
          } yield result
      }
    } yield resolved).catchAll {
      case e: HttpStatusException if e.statusCode == 404 =>
        CcasLogger.warn(s"  ${club.slug}: 404 — ${e.safeMessage}") *>
          skipClub(ctx, club, RefSkipReason.NotFound, Some(e.safeMessage)).as(false)
      case error =>
        CcasLogger.warn(s"  ${club.slug}: error — ${error.safeMessage}") *>
          skipClub(ctx, club, RefSkipReason.ApiError, Some(error.safeMessage)).as(false)
    }

  private def tryClubMatches(
    ctx: RefContext,
    club: UnresolvedClub,
    candidates: List[ApiClubMatches.ApiClubMatchFinished]
  ): RIO[CcasLogger & PostgresClient, Boolean] =
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
          error => recordFailedUrl(ctx, parsed.matchUrl, error, "club").as(false),
          teams =>
            RefHelpers.findClubIsTeam1(teams, club.slug) match {
              case None => ZIO.succeed(false)
              case Some(isTeam1) =>
                val ref = ClubMatchRef(club.clubId, parsed.matchId, parsed.isLive, isTeam1)
                withTransaction {
                  ClubMatchRef.upsert(ref) *> ClubRefSkip.deleteId(club.clubId)
                } *> ctx.clubsResolvedApi.update(_ + 1).as(true)
            }
        )
    }
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
    client.get[ApiPlayer](ApiPlayer.getUrl(username)).map { apiPlayer =>
      if (apiPlayer.playerId == expectedPlayerId) { Right(()) }
      else { Left(apiPlayer.playerId) }
    }

  private def handleVerification(
    ctx: RefContext,
    player: UnresolvedPlayer
  )(onVerified: => RIO[PostgresClient, ResolveResult]): RIO[CcasLogger & PostgresClient, ResolveResult] =
    verifyPlayerId(ctx.client, player.username, player.playerId).foldZIO(
      error =>
        CcasLogger.warn(s"  ${player.username}: verification error — ${error.safeMessage}").as(ResolveResult.NotFound),
      {
        case Left(actualId) =>
          CcasLogger.warn(
            s"  ${player.username}: player_id mismatch (expected ${player.playerId}, actual $actualId), skipping"
          ) *> ctx.skippedPlayers.update(_ :+ SkippedPlayer(player.playerId, player.username)).as(ResolveResult.SkipPlayer)
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
  ): RIO[CcasLogger & PostgresClient, Boolean] =
    (for {
      dbRef <- ClubMatchBoard.selectPlayerMatchRef(player.playerId)
      resolved <- dbRef match {
        case Some(ref) =>
          PlayerMatchRef.upsert(ref) *>
            CcasLogger.debug(s"  ${player.username}: upgraded via DB").as(true)
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
  ): RIO[CcasLogger & PostgresClient, Boolean] =
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
  ): RIO[CcasLogger & PostgresClient, Boolean] =
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
  ): RIO[CcasLogger & PostgresClient, Option[Boolean]] = {
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
                PlayerTournamentRef.upsert(ref) *>
                  CcasLogger.debug(s"  ${trp.username}: tournament ref upgraded to $slug").as(Some(true))
              }
            }
          )
      }
    }
  }
}
