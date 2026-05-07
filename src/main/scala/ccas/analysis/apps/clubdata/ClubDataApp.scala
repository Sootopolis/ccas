package ccas.analysis.apps.clubdata

import java.time.{Duration, Instant}

import zio.{Chunk, RIO, Ref, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.tables.{Club, ClubAdmin, ClubMatch, ClubMatchRef, Tables}
import ccas.api.club.{ApiClub, ApiClubMatches}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.{OutputFile, ProgressDisplay}
import ccas.analysis.apps.{ClubSlugRenameResolver, withClubSlugRenameRecovery}
import ccas.utils.client.{ChessComClient, HttpClientLayer, onNotFound}
import ccas.utils.sql.PostgresClient

object ClubDataApp extends ZIOAppDefault {

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      rawArgs <- ZIOAppArgs.getArgs
      parsed  <- ZIO.fromEither(parseArgs(rawArgs)).mapError(new IllegalArgumentException(_))
      data <- parsed.slugs match {
        case Nil   => refresh(parsed.minAgeHours)
        case slugs => refreshSlugs(slugs, parsed.minAgeHours)
      }
      _ <- OutputFile.writeAndLogGlobal("clubdata", formatReport(data), "_ccas")
    } yield ()).provideSome[ZIOAppArgs & Scope](
      ProgressDisplay.live(showProgress = true),
      ChessComClient.live("clubdata"),
      HttpClientLayer.live,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  /** @param slugs empty list means "refresh all known clubs" — see `run`'s `Nil` branch. */
  private[clubdata] case class ClubDataAppArgs(minAgeHours: Option[Int], slugs: List[ClubSlug])

  /** Parses CLI args into `ClubDataAppArgs`. Strips `--min-age <hours>` (bare `--min-age` is an error because, unlike
    * HistoryApp's `--refresh`, ClubDataApp's default behaviour already refreshes everything, so bare `--min-age` would
    * silently do nothing); drops any other `--`-prefixed tokens (unknown flags); wraps remaining positional tokens as
    * `ClubSlug`s.
    */
  private[clubdata] def parseArgs(args: Chunk[String]): Either[String, ClubDataAppArgs] = {
    def slugsFrom(rest: Chunk[String]): List[ClubSlug] =
      rest.iterator.filterNot(_.startsWith("--")).map(ClubSlug.wrap).toList
    val idx = args.indexOf("--min-age")
    if (idx < 0) { Right(ClubDataAppArgs(None, slugsFrom(args))) }
    else {
      args.lift(idx + 1).flatMap(_.toIntOption) match {
        case Some(hours) => Right(ClubDataAppArgs(Some(hours), slugsFrom(args.patch(idx, Chunk.empty, 2))))
        case None        => Left("--min-age requires an integer hours argument, e.g. --min-age 24")
      }
    }
  }

  case class RefreshResult(clubsProcessed: Int, clubsFailed: Int, clubsAdminChanged: Int, totalAdmins: Int)

  private case class ClubResult(adminCount: Int, failed: Boolean, adminChanged: Boolean)
  private val ClubFailed = ClubResult(0, failed = true, adminChanged = false)

  /** Refreshes club profile data (admins, member count, slug, latest_match_at, fetched_at) for all known clubs. When
    * `minAgeHours` is set, clubs refreshed within the last N hours are skipped; clubs with a null `fetched_at` (never
    * refreshed by ClubDataApp) are always eligible.
    */
  def refresh(minAgeHours: Option[Int]): RIO[ProgressDisplay & ChessComClient & PostgresClient, RefreshResult] =
    Club.selectAll.flatMap(refreshEligible(_, minAgeHours))

  /** Refreshes only the clubs whose slugs match. Unknown slugs are logged and skipped. When `minAgeHours` is set, the
    * age filter is applied to the resolved clubs (skipped entries logged separately from unknown slugs).
    */
  private def refreshSlugs(
    slugs: List[ClubSlug],
    minAgeHours: Option[Int]
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, RefreshResult] =
    for {
      resolved <- ZIO.foreach(slugs)(slug => Club.selectBySlug(slug).map(slug -> _))
      (missing, found) = resolved.partitionMap {
        case (slug, None)    => Left(slug)
        case (_, Some(club)) => Right(club)
      }
      _ <- ZIO.whenDiscard(missing.nonEmpty)(
        ZIO.logInfo(s"[ClubData] Unknown slugs (skipped): ${missing.mkString(", ")}")
      )
      result <- refreshEligible(found, minAgeHours)
    } yield result

  private def refreshEligible(
    clubs: List[Club],
    minAgeHours: Option[Int]
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, RefreshResult] = {
    val eligible = minAgeHours match {
      case None => clubs
      case Some(hours) =>
        val cutoff = Instant.now().minus(Duration.ofHours(hours.toLong))
        clubs.filter(_.fetchedAt.forall(_.isBefore(cutoff)))
    }
    val skipped = clubs.size - eligible.size
    ZIO.whenDiscard(skipped > 0)(
      ZIO.logInfo(s"[ClubData] --min-age filter: skipping $skipped recently-refreshed clubs")
    ) *> refreshClubs(eligible)
  }

  private def refreshClubs(clubs: List[Club]): RIO[ProgressDisplay & ChessComClient & PostgresClient, RefreshResult] =
    ZIO.scoped {
      for {
        client    <- ZIO.service[ChessComClient]
        total     = clubs.size
        _         <- ZIO.logInfo(s"[ClubData] Refreshing $total clubs")
        bar       <- ProgressDisplay.progressBar
        resultRef <- Ref.make(RefreshResult(0, 0, 0, 0))
        _ <- ZIO.foreachPar(clubs) { club =>
          for {
            r <- refreshClub(client, club).catchAll { error =>
              ZIO.logInfo(s"[ClubData] Failed to refresh ${club.slug}: ${error.getMessage}").as(ClubFailed)
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

  private def logSummary(label: String, r: RefreshResult, total: Int): RIO[ProgressDisplay, Unit] =
    ZIO.logInfo(
      s"[ClubData] $label: ${r.clubsProcessed}/$total clubs, ${r.clubsFailed} failed, " +
        s"${r.clubsAdminChanged} admin changes, ${r.totalAdmins} total admins"
    )

  private def refreshClub(client: ChessComClient, club: Club): RIO[ProgressDisplay & PostgresClient, ClubResult] =
    for {
      // Refresh the activity signal first. The matches endpoint sometimes succeeds even when the profile endpoint
      // returns an error (some clubs have erroneous profile pages but working match pages), so we don't want a profile
      // failure below to prevent us from updating latest_match_at. NOTE: when `club.slug` has been renamed, both
      // this step and the `fetchApiClubWithRenameRecovery` below independently invoke the slug resolver — Tier A on
      // the second invocation hits the row this step's recovery just updated, so cost is bounded (1 extra
      // `ApiClub.get` on the stale slug + 1 verify on the fresh slug). Hoisting recovery above this step would
      // lose the error-isolation property and is deferred.
      _ <- refreshLatestMatchAt(client, club)

      fetched <- fetchApiClubWithRenameRecovery(client, club)
      (apiClub, resolvedSlug) = fetched
      _ <- Club.upsertResolvingSlugConflict(Club.fromApi(apiClub, resolvedSlug), client)

      adminUsernames   = ClubAdmin.extractAdminUsernames(apiClub)
      existingAdminIds <- ClubAdmin.selectPlayerIdsByClub(club.clubId)
      allAdminIds      <- ClubAdminResolver.resolveAndPersistAdmins(client, club.clubId, adminUsernames, existingAdminIds)
      // Must remain the last step: --min-age relies on fetched_at being stamped only on full success.
      _ <- Club.updateFetchedAt(club.clubId, Instant.now())
    } yield ClubResult(allAdminIds.size, failed = false, adminChanged = allAdminIds != existingAdminIds)

  /** Fetches `ApiClub` for the given club, delegating rename-404 recovery to [[ClubSlugRenameResolver]]. The resolver
    * tries the DB first (if some other path already learned the new slug), then falls back to discovering the slug
    * via a `ClubMatchRef` board's team URL.
    */
  private def fetchApiClubWithRenameRecovery(
    client: ChessComClient,
    club: Club
  ): RIO[ProgressDisplay & PostgresClient, (ApiClub, ClubSlug)] =
    ApiClub.get(client, club.slug).map(_ -> club.slug).onNotFound { e =>
      ClubSlugRenameResolver.resolveAndPersist(client, club.slug, Some(club.clubId)).flatMap {
        case Some((newSlug, apiClub)) =>
          ZIO.logInfo(s"[ClubData] ${club.slug} returned 404; retrying with rediscovered slug $newSlug")
            .as(apiClub -> newSlug)
        case None => ZIO.fail(e)
      }
    }

  /** Refreshes `club.latest_match_at` using a tiered strategy to minimise API calls:
    *   1. If the cached value on `club` is fresher than [[ClubAdmin.ApiSkipThreshold]], trust it and do nothing.
    *   2. Otherwise scan our `club_match` table — it's biased toward matches against our home clubs but cheap. If it
    *      shows a match within the skip threshold, store that and stop.
    *   3. Otherwise fall back to the Chess.com `ApiClubMatches` endpoint and store whichever value (cached / DB / API)
    *      is most recent. The API call is what catches active clubs that don't play our home clubs.
    *
    * On API failure we keep whatever the cached/DB value was (possibly None) — the next refresh will try again.
    */
  private def refreshLatestMatchAt(client: ChessComClient, club: Club): RIO[ProgressDisplay & PostgresClient, Unit] = {
    val now             = Instant.now()
    val skipCutoff      = now.minus(ClubAdmin.ApiSkipThreshold)
    val cachedFreshEnough = club.latestMatchAt.exists(_.isAfter(skipCutoff))
    ZIO.unlessDiscard(cachedFreshEnough) {
      ClubMatch.selectLatestActivityForClub(club.clubId).flatMap { dbLatest =>
        val dbFreshEnough = dbLatest.exists(_.isAfter(skipCutoff))
        if (dbFreshEnough) Club.updateLatestMatchAt(club.clubId, dbLatest).unit
        else {
          fetchClubMatches(client, club.slug)
            .withClubSlugRenameRecovery(client, club.slug, Some(club.clubId))(fresh => fetchClubMatches(client, fresh))
            .asSome
            .catchAll { error =>
              ZIO.logInfo(s"[ClubData] Match fetch failed for ${club.slug}: ${error.getMessage}").as(None)
            }
            .flatMap { matchesOpt =>
              val apiLatest = matchesOpt.flatMap(latestTimestamp(_, now))
              val combined  = List(club.latestMatchAt, dbLatest, apiLatest).flatten.maxOption
              ZIO.whenDiscard(combined != club.latestMatchAt)(Club.updateLatestMatchAt(club.clubId, combined)) *>
                ZIO.foreachDiscard(matchesOpt)(tryPopulateClubMatchRef(client, club.clubId, club.slug, _))
            }
        }
      }
    }
  }

  private def fetchClubMatches(client: ChessComClient, clubSlug: ClubSlug): Task[ApiClubMatches] =
    client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))

  /** Returns the most recent activity timestamp: `now` if any registered match exists (signalling current activity),
    * otherwise the max `start_time` across in-progress and finished matches.
    */
  private def latestTimestamp(matches: ApiClubMatches, now: Instant): Option[Instant] =
    if (matches.registered.nonEmpty) Some(now)
    else (matches.inProgress.map(_.startTime) ++ matches.finished.map(_.startTime))
      .map(Instant.ofEpochSecond).maxOption

  /** Opportunistically populates a `ClubMatchRef` for a club that doesn't have one yet, using already-fetched
    * `ApiClubMatches` data. Failures are silently caught — RefApp will pick up any missed clubs on its next run.
    */
  private def tryPopulateClubMatchRef(
    client: ChessComClient,
    clubId: ClubId,
    slug: ClubSlug,
    matches: ApiClubMatches
  ): RIO[ProgressDisplay & PostgresClient, Unit] =
    ZIO.unlessDiscard(matches.finished.isEmpty) {
      ClubMatchRef.selectId(clubId).flatMap {
        case Some(_) => ZIO.unit
        case None =>
          val parsed = RefHelpers.parseMatchUrl(matches.finished.head.`@id`)
          RefHelpers.fetchTeamMatchTeams(client, parsed.matchId, parsed.isLive).flatMap { teams =>
            ZIO.foreachDiscard(RefHelpers.findClubIsTeam1(teams, slug)) { isTeam1 =>
              ClubMatchRef.upsert(ClubMatchRef(clubId, parsed.matchId, parsed.isLive, isTeam1)).unit
            }
          }
      }
    }.catchAll { error =>
      ZIO.logDebug(s"[ClubData] Opportunistic ref failed for $slug: ${error.getMessage}")
    }

  private def formatReport(data: RefreshResult): String =
    s"""Clubs processed: ${data.clubsProcessed}
       |Clubs failed: ${data.clubsFailed}
       |Clubs with admin changes: ${data.clubsAdminChanged}
       |Total admins stored: ${data.totalAdmins}
       |""".stripMargin
}
