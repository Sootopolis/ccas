package ccas.analysis.apps.recruitment

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{IO, RIO, ZIO}

import RecruitmentStatsHelpers.*
import RejectionReason.*

import ccas.analysis.apps.{withClubSlugRenameRecovery, withPlayerRenameRecovery, UsernameRenameResolver}
import ccas.analysis.apps.clubdata.ClubAdminResolver
import ccas.analysis.tables.*
import ccas.api.club.ApiClub
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubSlug, PlayerId, Username}
import ccas.api.player.*
import ccas.utils.errors.safeMessage
import ccas.utils.sql.PostgresClient

private[recruitment] object RecruitmentFilterDefs {

  // --- Shared helpers ---

  private def requireApiPlayer(env: FilterEnv): IO[NoSuchElementException, ApiPlayer] =
    ZIO.fromOption(env.candidate.apiPlayerOption)
      .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run first"))

  /** Fetches under the candidate's handle, recovering a rename on 404. Returns the result with the handle that
    * answered, for the filter to write back into the candidate.
    */
  private def fetchRecovering[A](env: FilterEnv, playerId: PlayerId)(
    fetch: Username => RIO[PostgresClient, A]
  ): RIO[PostgresClient, (Username, A)] = {
    def fetchUnder(username: Username): RIO[PostgresClient, (Username, A)] = fetch(username).map((username, _))
    fetchUnder(env.candidate.username)
      .withPlayerRenameRecovery(env.run.client, env.candidate.username, Some(playerId))(fetchUnder)
  }

  // A fresh row keeps its older date, which undersells a figure written now but never oversells one; a stale row is
  // restarted, so its unusable figures cannot take the new date (#429).
  private def writeFigures(
    env: FilterEnv,
    playerId: PlayerId
  )(update: PlayerRecruitmentCache => PlayerRecruitmentCache): PlayerRecruitmentCache = {
    val now = env.run.now
    val base = env.candidate.cacheOption.fold(PlayerRecruitmentCache.empty(playerId, now)) { cache =>
      if (figuresFresh(cache, now)) cache else cache.restartAt(now)
    }
    update(base)
  }

  // --- Filter implementations ---

  object FetchAndCheckPlayer extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        // Use fetchOrRecover (no resolver-side reconcile). RecruitmentPersistence.persistCandidateResults handles the
        // actual Player table writes based on `isNewPlayer`, so a single transactional write covers both rename
        // archival (existingPlayerOption.isDefined) and fresh insert.
        apiPlayer            <- UsernameRenameResolver.fetchOrRecover(env.run.client, env.candidate.username)
        existingPlayerOption <- Player.selectId(apiPlayer.playerId)

        // Load existing cache
        cached <- PlayerRecruitmentCache.selectId(apiPlayer.playerId)
      } yield {
        val statusCat = apiPlayer.status.category
        val criteria  = env.run.criteria
        val now       = env.run.now
        val updatedCtx = env.candidate.copy(
          username = apiPlayer.username,
          apiPlayerOption = Some(apiPlayer),
          isNewPlayer = existingPlayerOption.isEmpty,
          cacheOption = cached
        )
        val registeredTooRecently = criteria.minDaysSinceRegistration.exists { days =>
          ChronoUnit.DAYS.between(Instant.ofEpochSecond(apiPlayer.joined), now) < days
        }
        // A `def`: reading the country can fail, so only a player the checks before it passed reaches it.
        def wrongNationality = criteria.nationalityCountries.nonEmpty && {
          val countryCode = apiPlayer.country.path.segments.last
          val listed      = criteria.nationalityCountries.contains(countryCode)
          criteria.nationalityExclude == listed
        }
        val rejectionOption =
          if (statusCat != PlayerStatusCategory.Active) Some(AccountNotActive)
          else if (registeredTooRecently) Some(MinDaysSinceRegistration)
          else Option.when(wrongNationality)(Nationality)
        FilterResult(rejectionOption, updatedCtx)
      }
  }

  object CheckInvitedTooRecently extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer    <- requireApiPlayer(env)
        recentInvite <- RecruitmentCandidate.selectLatestInvitedByClub(apiPlayer.playerId, env.run.clubId)
      } yield {
        val invitedRecently = env.run.criteria.daysSinceLastInvited.exists { days =>
          recentInvite.exists(c => ChronoUnit.DAYS.between(c.evaluatedAt, env.run.now) < days)
        }
        FilterResult(Option.when(invitedRecently)(DaysSinceLastInvited), env.candidate)
      }
  }

  object CheckBlacklist extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer   <- requireApiPlayer(env)
        blacklisted <- RecruitmentBlacklist.isBlacklisted(env.run.clubId, apiPlayer.playerId, env.run.now)
      } yield FilterResult(Option.when(blacklisted)(Blacklisted), env.candidate)
  }

  object CheckAdminOfSizableClub extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      requireApiPlayer(env).map { apiPlayer =>
        val admin = env.run.adminExcludedPlayerIds.contains(apiPlayer.playerId)
        FilterResult(Option.when(admin)(AdminOfSizableClub), env.candidate)
      }
  }

  object CheckFormerMember extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      requireApiPlayer(env).map { apiPlayer =>
        val formerMember = env.run.formerMemberIds.contains(apiPlayer.playerId)
        FilterResult(Option.when(formerMember)(FormerMember), env.candidate)
      }
  }

  object CheckCacheCriteria extends RecruitmentFilter {
    override val rejectedAs: CandidateOutcome = CandidateOutcome.RejectedFromCache

    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      ZIO.succeed {
        val rejectionOption = env.candidate.cacheOption.flatMap { cache =>
          val check = CacheCheck(cache, env.run.criteria, env.run.now)
          CacheCriterion.values.find(_.fires(check)).map(_.reason)
        }
        FilterResult(rejectionOption, env.candidate)
      }
  }

  object CheckOpponentMatch extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] = {
      def fetch(uname: Username): RIO[PostgresClient, ApiPlayerMatches] =
        env.run.client.getUncached[ApiPlayerMatches](ApiPlayerMatches.getUrl(uname))
      for {
        apiPlayer                 <- requireApiPlayer(env)
        (username, playerMatches) <- fetchRecovering(env, apiPlayer.playerId)(fetch)
      } yield {
        val registeredIds = playerMatches.registered.map(_.`@id`).toSet ++
          playerMatches.inProgress.map(_.`@id`).toSet
        FilterResult(
          rejectionOption = Option.when(registeredIds.exists(env.run.clubMatchIds.contains))(InClubMatch),
          candidate = env.candidate.copy(username = username, playerMatchesOption = Some(playerMatches))
        )
      }
    }
  }

  object CheckClubs extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] = {
      def fetch(uname: Username): RIO[PostgresClient, ApiPlayerClubs] =
        env.run.client.getUncached[ApiPlayerClubs](ApiPlayerClubs.getUrl(uname))
      for {
        apiPlayer               <- requireApiPlayer(env)
        (username, playerClubs) <- fetchRecovering(env, apiPlayer.playerId)(fetch)
        clubNames = playerClubs.clubs.map(_.clubName).toSet
      } yield {
        val clubCount = playerClubs.clubs.size
        val criteria  = env.run.criteria
        val rejectionOption =
          if (criteria.maxClubs.exists(clubCount > _)) Some(MaxClubs)
          else Option.when(env.run.excludedSlugs.exists(clubNames.contains))(ExcludeClubs)
        val updatedCache = writeFigures(env, apiPlayer.playerId)(_.copy(clubCount = Some(clubCount)))
        FilterResult(
          rejectionOption = rejectionOption,
          candidate = env.candidate.copy(
            username = username,
            cacheOption = Some(updatedCache),
            playerClubsOption = Some(playerClubs)
          )
        )
      }
    }
  }

  /** Late-confirm pass for the `avoidAdminMinClubSize` criterion. The early [[CheckAdminOfSizableClub]] only sees clubs
    * already crawled into our DB; this filter walks the candidate's actual club list (fetched by [[CheckClubs]]) and
    * for any club we lack admin data on, fetches `ApiClub` on demand, persists `club` + `club_admin` rows via
    * [[ClubAdminResolver.resolveAndPersistAdmins]], and rejects the candidate if they're an admin of a sizable, active,
    * non-over-administered club.
    *
    * Slugs whose `ApiClub.get` fails (e.g. a Chess.com internal-error 404) are recorded in `runCtx.failedAdminSlugs`
    * and skipped for the rest of the run.
    */
  object CheckAdminOfDiscoveredClub extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer <- requireApiPlayer(env)
        playerClubs <- ZIO.fromOption(env.candidate.playerClubsOption)
          .orElseFail(
            new NoSuchElementException("playerClubs not set — CheckClubs must run before CheckAdminOfDiscoveredClub")
          )
        min <- ZIO.fromOption(env.run.criteria.avoidAdminMinClubSize)
          .orElseFail(
            new IllegalStateException("CheckAdminOfDiscoveredClub should only run when avoidAdminMinClubSize is set")
          )
        cutoff = env.run.now.minus(ClubAdmin.MaxInactivity)
        // foldLeft short-circuits via the boolean accumulator: once rejected, no further API work.
        rejected <- ZIO.foldLeft(playerClubs.clubs)(false) { (rejected, playerClub) =>
          if (rejected) ZIO.succeed(true)
          else evaluateClub(env.run, playerClub.clubName, apiPlayer.playerId, min, cutoff)
        }
      } yield FilterResult(Option.when(rejected)(AdminOfDiscoveredClub), env.candidate)

    private def evaluateClub(
      run: RunContext,
      slug: ClubSlug,
      candidatePlayerId: PlayerId,
      min: Int,
      cutoff: Instant
    ): RIO[PostgresClient, Boolean] =
      run.failedAdminSlugs.get.flatMap { failed =>
        if (failed.contains(slug)) ZIO.succeed(false)
        else
          for {
            existingClubOption <- ClubName.selectCurrentHolder(slug)
            existingAdmins <- existingClubOption.fold(ZIO.succeed(Set.empty[PlayerId])) { c =>
              ClubAdmin.selectPlayerIdsByClub(c.clubId)
            }
            rejected <- (existingClubOption, existingAdmins.nonEmpty) match {
              case (Some(club), true) =>
                // Have everything locally — the early prune at run start either evaluated this club already, OR an
                // earlier candidate's late-confirm pass in this run persisted these rows. In the second case the
                // early prune missed them, so re-apply the gate predicate against current local data and check the
                // candidate's membership directly. Skips the API call.
                val membersCount = club.membersCount.getOrElse(0)
                val gateOk       = passesGate(membersCount, existingAdmins.size, club.latestMatchAt, min, cutoff)
                ZIO.succeed(gateOk && existingAdmins.contains(candidatePlayerId))
              case _ =>
                fetchEvaluatePersist(run, slug, existingClubOption, candidatePlayerId, min, cutoff)
            }
          } yield rejected
      }

    private def fetchEvaluatePersist(
      run: RunContext,
      slug: ClubSlug,
      existingClubOption: Option[Club],
      candidatePlayerId: PlayerId,
      min: Int,
      cutoff: Instant
    ): RIO[PostgresClient, Boolean] = {
      // Activity check uses local latest_match_at, NOT apiClub.lastActivity (the API field is unreliable —
      // active clubs sometimes report 12-year-old timestamps). For freshly-fetched clubs the DB value is None,
      // which passesGate treats as active by convention (matching the SQL early-prune). ClubDataApp will fill
      // it in later.
      val latestMatchAt = existingClubOption.flatMap(_.latestMatchAt)
      // With no club holding the slug now, the resolver can derive a hint only from a club that held it before
      // (`deriveHint`). Every tier needs one, so for a slug never seen the wrap is a no-op until a later run has
      // persisted it.
      (for {
        apiClub <- ApiClub.get(run.client, slug)
          .withClubSlugRenameRecovery(client = run.client, stale = slug, clubIdHint = existingClubOption.map(_.clubId)) {
            fresh =>
              ApiClub.get(run.client, fresh)
          }
        // Persist the Club row regardless — value for future runs and for ClubDataApp's slug index.
        // `Club.upsert` deliberately preserves an existing latest_match_at, so this doesn't clobber DB state.
        _ <- Club.upsert(Club.fromApi(apiClub))
        rejected <-
          if (!passesGate(apiClub.membersCount, apiClub.admin.size, latestMatchAt, min, cutoff)) ZIO.succeed(false)
          else {
            // Resolve + persist admin rows. The returned set is the canonical admin set for this club —
            // we both store it (for future early-prune runs) and consume it for the rejection check.
            // We only reach here from the empty-admin-rows branch, so existingAdminIds is always empty.
            val adminUsernames = apiClub.adminUsernames
            ClubAdminResolver
              .resolveAndPersistAdmins(
                client = run.client,
                clubId = apiClub.clubId,
                adminUsernames = adminUsernames,
                existingAdminIds = Set.empty
              )
              .map(_.contains(candidatePlayerId))
          }
      } yield rejected).catchAll { error =>
        run.failedAdminSlugs.update(_ + slug) *>
          ZIO.logInfo(s"[Recruitment] Admin late-check failed for $slug: ${error.safeMessage}").as(false)
      }
    }

    /** Mirrors the SQL gate from `ClubAdmin.selectPlayerIdsForSizableClubs` so the in-filter check stays consistent
      * with the run-start early prune: a club qualifies when it has at least `min` members, was active within
      * `MaxInactivity` (NULL `latestMatchAt` is treated as active — benefit of the doubt for unrefreshed clubs), and
      * isn't over-administered.
      */
    private def passesGate(
      membersCount: Int,
      adminCount: Int,
      latestMatchAt: Option[Instant],
      min: Int,
      cutoff: Instant
    ): Boolean = {
      val sizable      = membersCount >= min
      val active       = latestMatchAt.forall(_.isAfter(cutoff))
      val adminRatioOk = membersCount > 0 && adminCount.toDouble / membersCount < ClubAdmin.MaxAdminRatio
      sizable && active && adminRatioOk
    }
  }

  object CheckDailyStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] = {
      def fetch(uname: Username): RIO[PostgresClient, ApiPlayerStats] =
        env.run.client.getUncached[ApiPlayerStats](ApiPlayerStats.getUrl(uname))
      for {
        apiPlayer               <- requireApiPlayer(env)
        (username, playerStats) <- fetchRecovering(env, apiPlayer.playerId)(fetch)
        candidate = env.candidate.copy(username = username)
        result <- playerStats.chessDaily match {
          case None             => ZIO.succeed(FilterResult(Some(NoDailyStats), candidate))
          case Some(dailyStats) => applyDailyStats(env.copy(candidate = candidate), dailyStats)
        }
      } yield result
    }

    private def applyDailyStats(
      env: FilterEnv,
      dailyStats: ApiPlayerStats.ApiPlayerDailyStats
    ): RIO[PostgresClient, FilterResult] = {
      val dailyElo           = dailyStats.last.rating
      val dailyTimeoutPct    = dailyStats.record.timeoutPercent
      val dailyGamesFinished = dailyStats.record.nGames
      for {
        // Fetch 90-day archives only when player has timed out before (timeoutPct > 0)
        archives <-
          ZIO.when(dailyTimeoutPct > 0 || env.run.criteria.dailyMinGamesFinished.isDefined) {
            val months = recentArchiveMonths(env.run.now, StatsWindowDays)
            ZIO.foreachPar(months) { ym =>
              val url = ApiPlayerArchive.getUrl(env.candidate.username, ym.getYear, ym.getMonthValue)
              env.run.client.getUncached[ApiPlayerArchive](url)
            }
          }
        apiPlayer <- requireApiPlayer(env)
      } yield {
        val cutoff90d = env.run.now.minus(StatsWindowDays, ChronoUnit.DAYS)
        val dailyGamesFinished90d = archives.map(
          _.flatMap(_.games.filter(g => g.timeClass == "daily" && g.endTime >= cutoff90d.getEpochSecond)).size
        )
        val lastDailyTimeoutAt = archives.flatMap(extractLastDailyTimeout(_, env.candidate.username))
        val mergedDailyTimeout = mergeOptionalInstants(
          a = lastDailyTimeoutAt,
          b = env.candidate.cacheOption.flatMap(_.lastDailyTimeoutAt)
        )
        val dailyTimePerMove = dailyStats.record.timePerMove
        val criteria         = env.run.criteria
        val gamesFinished    = dailyGamesFinished90d.getOrElse(dailyGamesFinished)
        val slowMoves        = criteria.dailyMaxHoursPerMove.exists(hours => dailyTimePerMove > hours * 3600)
        val rejectionOption =
          if (criteria.dailyMinElo.exists(min => dailyElo.value < min.value)) Some(DailyMinElo)
          else if (criteria.dailyMaxElo.exists(max => dailyElo.value > max.value)) Some(DailyMaxElo)
          else if (criteria.dailyMinScoreRate.exists(dailyStats.record.scoreRate < _)) Some(DailyMinScoreRate)
          else if (criteria.dailyMaxScoreRate.exists(dailyStats.record.scoreRate > _)) Some(DailyMaxScoreRate)
          else if (criteria.dailyMaxTimeoutPercent.exists(dailyTimeoutPct > _)) Some(DailyMaxTimeoutPercent)
          else if (criteria.dailyMinGamesFinished.exists(gamesFinished < _)) Some(DailyMinGamesFinished)
          else Option.when(slowMoves)(DailyMaxHoursPerMove)
        val updatedCache = writeFigures(env, apiPlayer.playerId) {
          _.copy(
            dailyElo = Some(dailyElo),
            dailyTimeoutPct = Some(dailyTimeoutPct),
            dailyGamesFinished = Some(gamesFinished),
            lastDailyTimeoutAt = mergedDailyTimeout,
            dailyScoreRate = Some(dailyStats.record.scoreRate)
          )
        }
        FilterResult(
          rejectionOption,
          env.candidate.copy(cacheOption = Some(updatedCache), recentArchivesOption = archives)
        )
      }
    }
  }

  object CheckOngoingGames extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] = {
      def fetch(uname: Username): RIO[PostgresClient, ApiPlayerGamesCurrent] =
        env.run.client.getUncached[ApiPlayerGamesCurrent](ApiPlayerGamesCurrent.getUrl(uname))
      for {
        apiPlayer                <- requireApiPlayer(env)
        (username, currentGames) <- fetchRecovering(env, apiPlayer.playerId)(fetch)
      } yield {
        val ongoingGames       = currentGames.games.size
        val ongoingTeamMatches = currentGames.games.count(_.`match`.isDefined)
        val criteria           = env.run.criteria
        val fewTeamMatches     = criteria.dailyMinOngoingTeamMatches.exists(ongoingTeamMatches < _)
        val rejectionOption =
          if (criteria.dailyMinOngoingGames.exists(ongoingGames < _)) Some(DailyMinOngoingGames)
          else if (criteria.dailyMaxOngoingGames.exists(ongoingGames > _)) Some(DailyMaxOngoingGames)
          else Option.when(fewTeamMatches)(DailyMinOngoingTeamMatches)
        val updatedCache = writeFigures(env, apiPlayer.playerId)(
          _.copy(ongoingGames = Some(ongoingGames), ongoingTeamMatches = Some(ongoingTeamMatches))
        )
        FilterResult(rejectionOption, env.candidate.copy(username = username, cacheOption = Some(updatedCache)))
      }
    }
  }

  object CheckTmStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer <- requireApiPlayer(env)
        tmStats <- fetchTmStats(
          client = env.run.client,
          username = env.candidate.username,
          playerIdHint = apiPlayer.playerId,
          criteria = env.run.criteria,
          now = env.run.now,
          recentArchivesOption = env.candidate.recentArchivesOption
        )
        _ <- env.run.discoveredOpponents.update(_ ++ tmStats.opponentUsernames)
      } yield {
        val updatedCache = writeFigures(env, apiPlayer.playerId) { cache =>
          cache.copy(
            tmGamesFinished90d = Some(tmStats.gamesFinished),
            tmTimeoutPct90d = tmStats.timeoutPct,
            lastTmTimeoutAt = mergeOptionalInstants(tmStats.lastTimeoutAt, cache.lastTmTimeoutAt)
          )
        }
        val criteria   = env.run.criteria
        val tmTimeouts = criteria.dailyMaxTmTimeoutPercent.exists(max => tmStats.timeoutPct.exists(_ > max))
        val rejectionOption =
          if (criteria.dailyMinTmGamesFinished.exists(tmStats.gamesFinished < _)) Some(DailyMinTmGamesFinished)
          else Option.when(tmTimeouts)(DailyMaxTmTimeoutPercent)
        val candidate = env.candidate.copy(username = tmStats.username, cacheOption = Some(updatedCache))
        FilterResult(rejectionOption, candidate)
      }
  }
}
