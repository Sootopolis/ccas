package ccas.analysis.apps.ref

import java.time.{Duration as JDuration, Instant}
import scala.annotation.nowarn
import com.augustnagro.magnum.{sql, Transactor}
import zio.{RIO, Ref, Scope, ZIO, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.{ClubRefSkip, PlayerRefSkip, RunTrigger, Tables}
import ccas.utils.{CcasLogger, OutputFile, display}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO
import RefUtils.*

object RefApp extends ZIOAppDefault {
  override def run: RIO[Scope, Unit] =
    populate().provideSome[Scope](
      CcasLogger.live(showProgress = true),
      ChessComClient.live,
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  // --- Entry point ---

  // trigger accepted for consistency with other app entry points but not persisted (no run table)
  @nowarn("msg=unused explicit parameter")
  def populate(_trigger: RunTrigger = RunTrigger.Cli, outputDir: Option[String] = Some("_ccas")): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      startedAt <- ZIO.succeed(Instant.now())
      client    <- ZIO.service[ChessComClient]
      ctx       <- RefContext.make(client)
      (clubsTotal, clubsResolvedDb, clubsResolvedApi, clubsSkippedNew) <- resolveClubs(ctx)
      (playersTotal, playersResolvedDb, playersResolvedApi, playersSkippedNew) <- resolvePlayers(ctx)
      skipped <- ctx.skippedPlayers.get
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      _ <- CcasLogger.info(s"Duration: ${duration.display}")
      // Output report
      failed              <- ctx.failedUrls.get
      failedSrc           <- ctx.failedUrlSource.get
      playerSkipsByReason <- PlayerRefSkip.countByReason
      clubSkipsByReason   <- ClubRefSkip.countByReason
      report = formatReport(ReportData(
        clubsTotal = clubsTotal, clubsResolvedDb = clubsResolvedDb, clubsResolvedApi = clubsResolvedApi,
        clubsSkippedNew = clubsSkippedNew,
        playersTotal = playersTotal, playersResolvedDb = playersResolvedDb, playersResolvedApi = playersResolvedApi,
        playersSkippedNew = playersSkippedNew,
        skippedPlayers = skipped,
        playerSkipsByReason = playerSkipsByReason,
        clubSkipsByReason = clubSkipsByReason,
        startedAt = startedAt, completedAt = completedAt,
        failedQueries = failed, failedUrlSources = failedSrc
      ))
      _ <- ZIO.whenCaseDiscard(outputDir) { case Some(dir) => OutputFile.writeAndLogGlobal("ref", report, dir) }
    } yield ()

  private def resolveClubs(ctx: RefContext): RIO[CcasLogger & ChessComClient & Transactor, (Int, Int, Int, Int)] =
    for {
      clubs <- selectUnresolvedClubs
      _     <- CcasLogger.info(s"Clubs without match ref: ${clubs.size}")
      clubProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          clubBar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(clubs) { club =>
            RefResolution.resolveClub(ctx, club)
              *> clubProcessed.updateAndGet(_ + 1).flatMap(n =>
                clubBar.print(n, clubs.size, s"  Resolving clubs: $n/${clubs.size}")
              )
          }
        } yield ()
      }
      db         <- ctx.clubsResolvedDb.get
      api        <- ctx.clubsResolvedApi.get
      skippedNew <- ctx.clubsSkippedNew.get
      _ <- CcasLogger.info(s"Clubs resolved: $db (DB) + $api (API) = ${db + api} / ${clubs.size}, skipped: $skippedNew new")
    } yield (clubs.size, db, api, skippedNew)

  private def resolvePlayers(ctx: RefContext): RIO[CcasLogger & ChessComClient & Transactor, (Int, Int, Int, Int)] =
    for {
      players <- selectUnresolvedPlayers
      _       <- CcasLogger.info(s"Players without match ref: ${players.size}")
      playerProcessed <- Ref.make(0)
      _ <- ZIO.scoped {
        for {
          playerBar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(players) { player =>
            RefResolution.resolvePlayer(ctx, player)
              *> playerProcessed.updateAndGet(_ + 1).flatMap(n =>
                playerBar.print(n, players.size, s"  Resolving players: $n/${players.size}")
              )
          }
        } yield ()
      }
      db         <- ctx.playersResolvedDb.get
      api        <- ctx.playersResolvedApi.get
      skippedNew <- ctx.playersSkippedNew.get
      skipped    <- ctx.skippedPlayers.get
      _ <- CcasLogger.info(
        s"Players resolved: $db (DB) + $api (API) = ${db + api} / ${players.size}, skipped: $skippedNew new"
      )
      _ <- ZIO.whenDiscard(skipped.nonEmpty)(
        CcasLogger.warn(s"Players skipped (ID mismatch): ${skipped.size}")
      )
    } yield (players.size, db, api, skippedNew)

  // --- Queries ---

  private def selectUnresolvedPlayers: RIO[Transactor, List[UnresolvedPlayer]] = {
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
            WHERE pmr.player_id IS NULL AND ptr.player_id IS NULL AND prs.player_id IS NULL""".query[UnresolvedPlayer].run().toList
    }
  }

  private def selectUnresolvedClubs: RIO[Transactor, List[UnresolvedClub]] = {
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

  // --- Report ---

  private def formatReport(d: ReportData): String = {
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
    sb.append(s"Unresolved:     ${d.playersTotal - d.playersResolvedDb - d.playersResolvedApi - d.playersSkippedNew}\n\n")

    if (d.skippedPlayers.nonEmpty) {
      sb.append(s"--- Skipped Players — ID Mismatch (${d.skippedPlayers.size}) ---\n")
      d.skippedPlayers.sortBy(_._2.toString).foreach { case (pid, username) =>
        sb.append(s"  $username (player_id=$pid)\n")
      }
      sb.append("\n")
    }

    if (d.playerSkipsByReason.nonEmpty || d.clubSkipsByReason.nonEmpty) {
      sb.append("--- Skip Totals ---\n")
      if (d.playerSkipsByReason.nonEmpty) {
        sb.append("  Players:\n")
        d.playerSkipsByReason.sortBy(_._1.toString).foreach { case (reason, count) =>
          sb.append(s"    $reason: $count\n")
        }
      }
      if (d.clubSkipsByReason.nonEmpty) {
        sb.append("  Clubs:\n")
        d.clubSkipsByReason.sortBy(_._1.toString).foreach { case (reason, count) =>
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
