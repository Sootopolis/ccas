package ccas.analysis.apps.clubdata

import java.time.Instant

import zio.{RIO, Ref, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.{Club, ClubAdmin, ClubMatch, Tables}
import ccas.api.club.{ApiClub, ApiClubMatches}
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.{CcasLogger, OutputFile}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

object ClubDataApp extends ZIOAppDefault {

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      data <- args.toList match {
        case Nil   => refresh
        case slugs => refreshSlugs(slugs.map(ClubSlug.wrap))
      }
      _ <- OutputFile.writeAndLogGlobal("clubdata", formatReport(data), "_ccas")
    } yield ()).provideSome[ZIOAppArgs & Scope](
      CcasLogger.live(showProgress = true),
      ChessComClient.live("clubdata"),
      Client.default,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  case class RefreshResult(clubsProcessed: Int, clubsFailed: Int, clubsAdminChanged: Int, totalAdmins: Int)

  private case class ClubResult(adminCount: Int, failed: Boolean, adminChanged: Boolean)
  private val ClubFailed = ClubResult(0, failed = true, adminChanged = false)

  /** Refreshes club profile data (admins, member count, slug, latest_match_at) for all known clubs. */
  def refresh: RIO[CcasLogger & ChessComClient & PostgresClient, RefreshResult] =
    Club.selectAll.flatMap(refreshClubs)

  /** Refreshes only the clubs whose slugs match. Unknown slugs are logged and skipped. */
  def refreshSlugs(slugs: List[ClubSlug]): RIO[CcasLogger & ChessComClient & PostgresClient, RefreshResult] =
    for {
      resolved <- ZIO.foreach(slugs)(slug => Club.selectBySlug(slug).map(slug -> _))
      (missing, found) = resolved.partitionMap {
        case (slug, None)    => Left(slug)
        case (_, Some(club)) => Right(club)
      }
      _ <- ZIO.whenDiscard(missing.nonEmpty)(
        CcasLogger.info(s"[ClubData] Unknown slugs (skipped): ${missing.mkString(", ")}")
      )
      result <- refreshClubs(found)
    } yield result

  private def refreshClubs(clubs: List[Club]): RIO[CcasLogger & ChessComClient & PostgresClient, RefreshResult] =
    ZIO.scoped {
      for {
        client    <- ZIO.service[ChessComClient]
        total     = clubs.size
        _         <- CcasLogger.info(s"[ClubData] Refreshing $total clubs")
        bar       <- CcasLogger.progressBar
        resultRef <- Ref.make(RefreshResult(0, 0, 0, 0))
        _ <- ZIO.foreachPar(clubs) { club =>
          for {
            r <- refreshClub(client, club).catchAll { error =>
              CcasLogger.info(s"[ClubData] Failed to refresh ${club.slug}: ${error.getMessage}").as(ClubFailed)
            }
            updated <- resultRef.updateAndGet(acc =>
              acc.copy(
                clubsProcessed = acc.clubsProcessed + 1,
                clubsFailed = acc.clubsFailed + (if (r.failed) 1 else 0),
                clubsAdminChanged = acc.clubsAdminChanged + (if (r.adminChanged) 1 else 0),
                totalAdmins = acc.totalAdmins + r.adminCount
              )
            )
            _ <- bar.print(updated.clubsProcessed, total, s"Refreshing clubs: ${updated.clubsProcessed}/$total")
          } yield ()
        }.withParallelism(8).onInterrupt {
          (for {
            partial <- resultRef.get
            _       <- logSummary("Interrupted", partial, total)
          } yield ()).orDie
        }
        result <- resultRef.get
        _      <- logSummary("Done", result, total)
      } yield result
    }

  private def logSummary(label: String, r: RefreshResult, total: Int): RIO[CcasLogger, Unit] =
    CcasLogger.info(
      s"[ClubData] $label: ${r.clubsProcessed}/$total clubs, ${r.clubsFailed} failed, " +
        s"${r.clubsAdminChanged} admin changes, ${r.totalAdmins} total admins"
    )

  private def refreshClub(client: ChessComClient, club: Club): RIO[CcasLogger & PostgresClient, ClubResult] =
    for {
      // Refresh the activity signal first. The matches endpoint sometimes succeeds even when the profile endpoint
      // returns an error (some clubs have erroneous profile pages but working match pages), so we don't want a profile
      // failure below to prevent us from updating latest_match_at.
      _ <- refreshLatestMatchAt(client, club)

      apiClub <- ApiClub.get(client, club.slug)
      _       <- Club.upsertResolvingSlugConflict(Club.fromApi(apiClub, club.slug), client)

      adminUsernames   = ClubAdmin.extractAdminUsernames(apiClub)
      existingAdminIds <- ClubAdmin.selectPlayerIdsByClub(club.clubId)
      allAdminIds      <- ClubAdminResolver.resolveAndPersistAdmins(client, club.clubId, adminUsernames, existingAdminIds)
    } yield ClubResult(allAdminIds.size, failed = false, adminChanged = allAdminIds != existingAdminIds)

  /** Refreshes `club.latest_match_at` using a tiered strategy to minimise API calls:
    *   1. If the cached value on `club` is fresher than [[ClubAdmin.ApiSkipThreshold]], trust it and do nothing.
    *   2. Otherwise scan our `club_match` table — it's biased toward matches against our home clubs but cheap. If it
    *      shows a match within the skip threshold, store that and stop.
    *   3. Otherwise fall back to the Chess.com `ApiClubMatches` endpoint and store whichever value (cached / DB / API)
    *      is most recent. The API call is what catches active clubs that don't play our home clubs.
    *
    * On API failure we keep whatever the cached/DB value was (possibly None) — the next refresh will try again.
    */
  private def refreshLatestMatchAt(client: ChessComClient, club: Club): RIO[CcasLogger & PostgresClient, Unit] = {
    val now             = Instant.now()
    val skipCutoff      = now.minus(ClubAdmin.ApiSkipThreshold)
    val cachedFreshEnough = club.latestMatchAt.exists(_.isAfter(skipCutoff))
    if (cachedFreshEnough) ZIO.unit
    else
      ClubMatch.selectLatestActivityForClub(club.clubId).flatMap { dbLatest =>
        val dbFreshEnough = dbLatest.exists(_.isAfter(skipCutoff))
        if (dbFreshEnough) Club.updateLatestMatchAt(club.clubId, dbLatest).unit
        else apiLatestActivity(client, club.slug, now).catchAll { error =>
          CcasLogger.info(s"[ClubData] Match fetch failed for ${club.slug}: ${error.getMessage}").as(None)
        }.flatMap { apiLatest =>
          val combined = List(club.latestMatchAt, dbLatest, apiLatest).flatten.maxOption
          ZIO.whenDiscard(combined != club.latestMatchAt)(Club.updateLatestMatchAt(club.clubId, combined))
        }
      }
  }

  /** Fetches `ApiClubMatches` and returns the most recent activity timestamp: `now` if any registered match exists
    * (signalling current activity), otherwise the max `start_time` across in-progress and finished matches.
    */
  private def apiLatestActivity(client: ChessComClient, clubSlug: ClubSlug, now: Instant): Task[Option[Instant]] =
    client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug)).map { matches =>
      if (matches.registered.nonEmpty) Some(now)
      else (matches.inProgress.map(_.startTime) ++ matches.finished.map(_.startTime))
        .map(Instant.ofEpochSecond).maxOption
    }

  private def formatReport(data: RefreshResult): String =
    s"""Clubs processed: ${data.clubsProcessed}
       |Clubs failed: ${data.clubsFailed}
       |Clubs with admin changes: ${data.clubsAdminChanged}
       |Total admins stored: ${data.totalAdmins}
       |""".stripMargin
}
