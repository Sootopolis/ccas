package ccas.analysis.apps.ref

import java.time.{Duration as JDuration, Instant}

import com.augustnagro.magnum.sql
import zio.{Clock, RIO, Ref, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import RefUtils.*

import ccas.analysis.tables.{ClubRefSkip, PlayerRefSkip, PlayerTournamentRef, SkipCount, Tables}
import ccas.utils.{display, ApiConcurrency, CcasLogger, OutputFile}
import ccas.utils.client.{ChessComClient, HttpClientLayer}
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

object RefApp extends ZIOAppDefault {
  private val help = "Usage: RefApp [--force-skipped] [--upgrade-refs]"

  final case class ReportData(
    clubsTotal: Int,
    clubsResolvedDb: Int,
    clubsResolvedApi: Int,
    clubsSkippedNew: Int,
    playersTotal: Int,
    playersResolvedDb: Int,
    playersResolvedApi: Int,
    playersSkippedNew: Int,
    skippedPlayers: List[SkippedPlayer],
    playerSkipsByReason: List[SkipCount],
    clubSkipsByReason: List[SkipCount],
    upgradeEligible: Int,
    upgradeSucceeded: Int,
    tournamentUpgradeEligible: Int,
    tournamentUpgradeSucceeded: Int,
    startedAt: Instant,
    completedAt: Instant,
    failedQueries: Map[String, String],
    failedUrlSources: Map[String, String]
  )

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      _    <- ZIO.whenDiscard(args.contains("--help"))(ZIO.fail(BadRequestException(help)))
      forceSkipped = args.contains("--force-skipped")
      upgradeRefs  = args.contains("--upgrade-refs")
      data <- populate(forceSkipped = forceSkipped, upgradeRefs = upgradeRefs)
      _    <- OutputFile.writeAndLogGlobal("ref", formatReport(data), "_ccas")
    } yield ()).provideSome[ZIOAppArgs & Scope](
      CcasLogger.live(showProgress = true),
      ChessComClient.live("ref"),
      HttpClientLayer.live,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  // --- Entry point ---

  def populate(
    forceSkipped: Boolean,
    upgradeRefs: Boolean
  ): RIO[CcasLogger & ChessComClient & PostgresClient, ReportData] =
    for {
      startedAt                                                                <- Clock.instant
      client                                                                   <- ZIO.service[ChessComClient]
      ctx                                                                      <- RefContext.make(client)
      (clubsTotal, clubsResolvedDb, clubsResolvedApi, clubsSkippedNew)         <- resolveClubs(ctx, forceSkipped)
      (playersTotal, playersResolvedDb, playersResolvedApi, playersSkippedNew) <- resolvePlayers(ctx, forceSkipped)
      (upgradeEligible, upgradeSucceeded, tournamentUpgradeEligible, tournamentUpgradeSucceeded) <-
        upgradeTournamentPlayers(ctx, upgradeRefs)
      skipped                             <- ctx.skippedPlayers.get
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      _ <- CcasLogger.info(s"Duration: ${duration.display}")
      failed              <- ctx.failedUrls.get
      failedSrc           <- ctx.failedUrlSource.get
      playerSkipsByReason <- PlayerRefSkip.countByReason
      clubSkipsByReason   <- ClubRefSkip.countByReason
    } yield ReportData(
      clubsTotal = clubsTotal,
      clubsResolvedDb = clubsResolvedDb,
      clubsResolvedApi = clubsResolvedApi,
      clubsSkippedNew = clubsSkippedNew,
      playersTotal = playersTotal,
      playersResolvedDb = playersResolvedDb,
      playersResolvedApi = playersResolvedApi,
      playersSkippedNew = playersSkippedNew,
      skippedPlayers = skipped,
      playerSkipsByReason = playerSkipsByReason,
      clubSkipsByReason = clubSkipsByReason,
      upgradeEligible = upgradeEligible,
      upgradeSucceeded = upgradeSucceeded,
      tournamentUpgradeEligible = tournamentUpgradeEligible,
      tournamentUpgradeSucceeded = tournamentUpgradeSucceeded,
      startedAt = startedAt,
      completedAt = completedAt,
      failedQueries = failed,
      failedUrlSources = failedSrc
    )

  private def resolveClubs(
    ctx: RefContext,
    forceSkipped: Boolean
  ): RIO[CcasLogger & ChessComClient & PostgresClient, (Int, Int, Int, Int)] = {
    val parallelism = ApiConcurrency.fiberCap(ctx.client)
    for {
      clubs <- selectUnresolvedClubs(forceSkipped)
      _     <- CcasLogger.info(s"Clubs without match ref: ${clubs.size}")
      _ <- ZIO.scoped {
        CcasLogger.foreachParProgress(clubs, parallelism)((n, total) => s"  Resolving clubs: $n/$total") { club =>
          RefResolution.resolveClub(ctx, club)
        }
      }.onInterrupt {
        for {
          db  <- ctx.clubsResolvedDb.get
          api <- ctx.clubsResolvedApi.get
          sk  <- ctx.clubsSkippedNew.get
          _   <- CcasLogger.info(s"Interrupted resolveClubs: $db (DB) + $api (API) resolved, $sk skipped / ${clubs.size}")
        } yield ()
      }
      db         <- ctx.clubsResolvedDb.get
      api        <- ctx.clubsResolvedApi.get
      skippedNew <- ctx.clubsSkippedNew.get
      unchanged <- ctx.clubMatchesUnchanged.get
      _ <- CcasLogger.info(
        s"Clubs resolved: $db (DB) + $api (API) = ${db + api} / ${clubs.size}, skipped: $skippedNew new"
      )
      _ <- ZIO.whenDiscard(unchanged > 0)(
        CcasLogger.info(s"Club-matches listings unchanged (skipped candidate iteration): $unchanged")
      )
    } yield (clubs.size, db, api, skippedNew)
  }

  private def resolvePlayers(
    ctx: RefContext,
    forceSkipped: Boolean
  ): RIO[CcasLogger & ChessComClient & PostgresClient, (Int, Int, Int, Int)] = {
    val parallelism = ApiConcurrency.fiberCap(ctx.client)
    for {
      players <- selectUnresolvedPlayers(forceSkipped)
      _       <- CcasLogger.info(s"Players without match ref: ${players.size}")
      _ <- ZIO.scoped {
        CcasLogger.foreachParProgress(players, parallelism)((n, total) => s"  Resolving players: $n/$total") { player =>
          RefResolution.resolvePlayer(ctx, player)
        }
      }.onInterrupt {
        for {
          db  <- ctx.playersResolvedDb.get
          api <- ctx.playersResolvedApi.get
          sk  <- ctx.playersSkippedNew.get
          _   <- CcasLogger.info(s"Interrupted resolvePlayers: $db (DB) + $api (API) resolved, $sk skipped / ${players.size}")
        } yield ()
      }
      db         <- ctx.playersResolvedDb.get
      api        <- ctx.playersResolvedApi.get
      skippedNew <- ctx.playersSkippedNew.get
      skipped    <- ctx.skippedPlayers.get
      matchesUnchanged     <- ctx.playerMatchesUnchanged.get
      tournamentsUnchanged <- ctx.playerTournamentsUnchanged.get
      _ <- CcasLogger.info(
        s"Players resolved: $db (DB) + $api (API) = ${db + api} / ${players.size}, skipped: $skippedNew new"
      )
      _ <- ZIO.whenDiscard(matchesUnchanged > 0 || tournamentsUnchanged > 0)(
        CcasLogger.info(
          s"Player listings unchanged (skipped candidate iteration): matches=$matchesUnchanged, tournaments=$tournamentsUnchanged"
        )
      )
      _ <- ZIO.whenDiscard(skipped.nonEmpty)(
        CcasLogger.warn(s"Players skipped (ID mismatch): ${skipped.size}")
      )
    } yield (players.size, db, api, skippedNew)
  }

  // --- Queries ---

  // selectUnresolvedPlayers and selectUnresolvedClubs share identical retry-window OR-chains
  // (5 reasons × Cutoffs fields) inside their LEFT JOIN-on clauses. They cannot be DRYed via
  // a shared Frag because Magnum's sql interpolator only splices DbCodec values, not Frags.
  // Keep both queries' OR-chains in lockstep when adding/removing reasons or windows.

  private def selectUnresolvedPlayers(forceSkipped: Boolean): RIO[PostgresClient, List[UnresolvedPlayer]] =
    if (forceSkipped) {
      connectZIO {
        sql"""SELECT p.player_id, p.username
              FROM player p
              LEFT JOIN player_match_ref pmr ON p.player_id = pmr.player_id
              LEFT JOIN player_tournament_ref ptr ON p.player_id = ptr.player_id
              WHERE pmr.player_id IS NULL AND ptr.player_id IS NULL""".query[UnresolvedPlayer].run().toList
      }
    } else {
      val c = RetryWindows.allCutoffs(Instant.now())
      connectZIO {
        sql"""SELECT p.player_id, p.username
              FROM player p
              LEFT JOIN player_match_ref pmr ON p.player_id = pmr.player_id
              LEFT JOIN player_tournament_ref ptr ON p.player_id = ptr.player_id
              LEFT JOIN player_ref_skip prs ON p.player_id = prs.player_id
                AND ((prs.reason = 'NoData'           AND prs.last_attempted > ${c.noData})
                  OR (prs.reason = 'NotFound'         AND prs.last_attempted > ${c.notFound})
                  OR (prs.reason = 'IdMismatch'       AND prs.last_attempted > ${c.idMismatch})
                  OR (prs.reason = 'ResolutionFailed' AND prs.last_attempted > ${c.resolutionFailed})
                  OR (prs.reason = 'ApiError'         AND prs.last_attempted > ${c.apiError}))
              WHERE pmr.player_id IS NULL AND ptr.player_id IS NULL AND prs.player_id IS NULL""".query[
          UnresolvedPlayer
        ].run().toList
      }
    }

  private def selectUnresolvedClubs(forceSkipped: Boolean): RIO[PostgresClient, List[UnresolvedClub]] =
    if (forceSkipped) {
      connectZIO {
        sql"""SELECT c.club_id, c.slug
              FROM club c
              LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
              WHERE cmr.club_id IS NULL""".query[UnresolvedClub].run().toList
      }
    } else {
      val c = RetryWindows.allCutoffs(Instant.now())
      connectZIO {
        sql"""SELECT c.club_id, c.slug
              FROM club c
              LEFT JOIN club_match_ref cmr ON c.club_id = cmr.club_id
              LEFT JOIN club_ref_skip crs ON c.club_id = crs.club_id
                AND ((crs.reason = 'NoData'           AND crs.last_attempted > ${c.noData})
                  OR (crs.reason = 'NotFound'         AND crs.last_attempted > ${c.notFound})
                  OR (crs.reason = 'IdMismatch'       AND crs.last_attempted > ${c.idMismatch})
                  OR (crs.reason = 'ResolutionFailed' AND crs.last_attempted > ${c.resolutionFailed})
                  OR (crs.reason = 'ApiError'         AND crs.last_attempted > ${c.apiError}))
              WHERE cmr.club_id IS NULL AND crs.club_id IS NULL""".query[UnresolvedClub].run().toList
      }
    }

  // --- Upgrade: tournament ref → match ref ---

  private def upgradeTournamentPlayers(
    ctx: RefContext,
    upgradeRefs: Boolean
  ): RIO[CcasLogger & PostgresClient, (Int, Int, Int, Int)] =
    if (!upgradeRefs) { ZIO.succeed((0, 0, 0, 0)) }
    else {
      val parallelism = ApiConcurrency.fiberCap(ctx.client)
      for {
        allPlayers   <- selectTournamentOnlyPlayersWithSlug
        newTournRefs <- ctx.newTournamentRefPlayerIds.get
        players       = allPlayers.filterNot(trp => newTournRefs.contains(trp.playerId))
        skipped       = allPlayers.size - players.size
        _ <- CcasLogger.info(
          s"Tournament-only players eligible for upgrade: ${players.size}" +
            (if (skipped > 0) s" ($skipped skipped, created this run)" else "")
        )
        matchUpgraded <- Ref.make(0)
        tournUpgraded <- Ref.make(0)
        _ <- ZIO.scoped {
          CcasLogger.foreachParProgress(players, parallelism)((n, total) => s"  Upgrading refs: $n/$total") { trp =>
            val player = UnresolvedPlayer(trp.playerId, trp.username)
            RefResolution.tryUpgradeToMatchRef(ctx, player).flatMap {
              case true =>
                PlayerTournamentRef.deleteId(trp.playerId) *> matchUpgraded.update(_ + 1)
              case false =>
                RefResolution.tryUpgradeTournamentRef(ctx, trp).flatMap { success =>
                  ZIO.whenDiscard(success)(tournUpgraded.update(_ + 1))
                }
            }
          }
        }.onInterrupt {
          for {
            mc <- matchUpgraded.get
            tc <- tournUpgraded.get
            _  <- CcasLogger.info(s"Interrupted upgradeTournamentPlayers: $mc match, $tc tournament upgrades / ${players.size}")
          } yield ()
        }
        matchCount <- matchUpgraded.get
        tournCount <- tournUpgraded.get
        _ <- CcasLogger.info(
          s"Upgraded: $matchCount to match refs, $tournCount to smaller tournaments / ${players.size}"
        )
      } yield (players.size, matchCount, players.size - matchCount, tournCount)
    }

  private def selectTournamentOnlyPlayersWithSlug: RIO[PostgresClient, List[TournamentRefPlayer]] =
    connectZIO {
      sql"""SELECT p.player_id, p.username, ptr.tournament_slug
            FROM player p
            INNER JOIN player_tournament_ref ptr ON p.player_id = ptr.player_id
            LEFT JOIN player_match_ref pmr ON p.player_id = pmr.player_id
            WHERE pmr.player_id IS NULL""".query[TournamentRefPlayer].run().toList
    }

  // --- Report ---

  def formatReport(d: ReportData): String = {
    val duration = JDuration.between(d.startedAt, d.completedAt)
    val sb       = new StringBuilder

    sb.append("=== Ref Resolution Report ===\n\n")
    sb.append(s"Started:   ${d.startedAt}\n")
    sb.append(s"Completed: ${d.completedAt}\n")
    sb.append(s"Duration:  ${duration.display}\n\n")

    sb.append("--- Clubs ---\n")
    sb.append(s"Total:          ${d.clubsTotal}\n")
    sb.append(s"Resolved (DB):  ${d.clubsResolvedDb}\n")
    sb.append(s"Resolved (API): ${d.clubsResolvedApi}\n")
    sb.append(s"Skipped (new):  ${d.clubsSkippedNew}\n")
    sb.append(s"Unresolved:     ${d.clubsTotal - d.clubsResolvedDb - d.clubsResolvedApi - d.clubsSkippedNew}\n\n")

    sb.append("--- Players ---\n")
    sb.append(s"Total:          ${d.playersTotal}\n")
    sb.append(s"Resolved (DB):  ${d.playersResolvedDb}\n")
    sb.append(s"Resolved (API): ${d.playersResolvedApi}\n")
    sb.append(s"Skipped (new):  ${d.playersSkippedNew}\n")
    sb.append(
      s"Unresolved:     ${d.playersTotal - d.playersResolvedDb - d.playersResolvedApi - d.playersSkippedNew}\n\n"
    )

    if (d.upgradeEligible > 0) {
      sb.append("--- Tournament → Match Upgrades ---\n")
      sb.append(s"Eligible:  ${d.upgradeEligible}\n")
      sb.append(s"Upgraded:  ${d.upgradeSucceeded}\n\n")
    }

    if (d.tournamentUpgradeEligible > 0) {
      sb.append("--- Tournament → Smaller Tournament Upgrades ---\n")
      sb.append(s"Eligible:  ${d.tournamentUpgradeEligible}\n")
      sb.append(s"Upgraded:  ${d.tournamentUpgradeSucceeded}\n\n")
    }

    if (d.skippedPlayers.nonEmpty) {
      sb.append(s"--- Skipped Players — ID Mismatch (${d.skippedPlayers.size}) ---\n")
      d.skippedPlayers.sortBy(_.username.toString).foreach { case SkippedPlayer(pid, username) =>
        sb.append(s"  $username (player_id=$pid)\n")
      }
      sb.append("\n")
    }

    if (d.playerSkipsByReason.nonEmpty || d.clubSkipsByReason.nonEmpty) {
      sb.append("--- Skip Totals ---\n")
      if (d.playerSkipsByReason.nonEmpty) {
        sb.append("  Players:\n")
        d.playerSkipsByReason.sortBy(_.reason.toString).foreach { case SkipCount(reason, count) =>
          sb.append(s"    $reason: $count\n")
        }
      }
      if (d.clubSkipsByReason.nonEmpty) {
        sb.append("  Clubs:\n")
        d.clubSkipsByReason.sortBy(_.reason.toString).foreach { case SkipCount(reason, count) =>
          sb.append(s"    $reason: $count\n")
        }
      }
      sb.append("\n")
    }

    val clubFailed   = d.failedQueries.filter { case (url, _) => d.failedUrlSources.get(url).contains("club") }
    val playerFailed = d.failedQueries.filter { case (url, _) => d.failedUrlSources.get(url).contains("player") }

    if (clubFailed.nonEmpty) {
      sb.append(s"--- Failed Club Queries (${clubFailed.size}) ---\n")
      clubFailed.toList.sortBy(_._1).foreach { case (url, error) =>
        sb.append(s"  $url: $error\n")
      }
      sb.append("\n")
    }

    if (playerFailed.nonEmpty) {
      sb.append(s"--- Failed Player Queries (${playerFailed.size}) ---\n")
      playerFailed.toList.sortBy(_._1).foreach { case (url, error) =>
        sb.append(s"  $url: $error\n")
      }
      sb.append("\n")
    }

    sb.toString
  }
}
