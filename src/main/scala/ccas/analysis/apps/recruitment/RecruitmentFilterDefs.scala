package ccas.analysis.apps.recruitment

import java.time.temporal.ChronoUnit
import java.time.Instant

import ccas.utils.sql.PostgresClient
import zio.{RIO, ZIO}
import RecruitmentStatsHelpers.*

import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.player.*

private[recruitment] object RecruitmentFilterDefs {

  // --- Shared helpers ---

  private def requireApiPlayer(env: FilterEnv): zio.IO[NoSuchElementException, ApiPlayer] =
    ZIO.fromOption(env.candidate.apiPlayer)
      .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run first"))

  private def getOrUpdateCache(
    env: FilterEnv
  )(update: PlayerRecruitmentCache => PlayerRecruitmentCache): PlayerRecruitmentCache = {
    val playerId = env.candidate.apiPlayer.get.playerId
    val base = env.candidate.cache.getOrElse(
      PlayerRecruitmentCache.empty(playerId, env.run.now, None)
    )
    update(base)
  }

  // --- Filter implementations ---

  object FetchAndCheckPlayer extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer      <- env.run.client.get[ApiPlayer](ApiPlayer.getUrl(env.candidate.username))
        existingPlayer <- Player.selectId(apiPlayer.playerId)

        // Load existing cache
        cached <- PlayerRecruitmentCache.selectId(apiPlayer.playerId)
      } yield {
        val statusCat = apiPlayer.status.category
        val criteria  = env.run.criteria
        val now       = env.run.now
        val updatedCtx = env.candidate.copy(
          apiPlayer = Some(apiPlayer),
          isNewPlayer = existingPlayer.isEmpty,
          cache = cached
        )
        val rejected =
          statusCat != PlayerStatusCategory.Active
            || criteria.minDaysSinceRegistration.exists { days =>
              ChronoUnit.DAYS.between(Instant.ofEpochSecond(apiPlayer.joined), now) < days
            }
            || (criteria.nationalityCountries.nonEmpty && {
              val countryCode = apiPlayer.country.path.segments.last
              val listed      = criteria.nationalityCountries.contains(countryCode)
              criteria.nationalityExclude == listed
            })
        FilterResult(rejected, updatedCtx)
      }
  }

  object CheckInvitedTooRecently extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer    <- requireApiPlayer(env)
        recentInvite <- RecruitmentCandidate.selectLatestInvited(apiPlayer.playerId)
      } yield {
        val rejected = env.run.criteria.daysSinceLastInvited.exists { days =>
          recentInvite.exists(c => ChronoUnit.DAYS.between(c.evaluatedAt, env.run.now) < days)
        }
        FilterResult(rejected, env.candidate)
      }
  }

  object CheckBlacklist extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        apiPlayer   <- requireApiPlayer(env)
        blacklisted <- RecruitmentBlacklist.isBlacklisted(env.run.clubId, apiPlayer.playerId, env.run.now)
      } yield FilterResult(blacklisted, env.candidate)
  }

  object CheckFormerMember extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      requireApiPlayer(env).map { apiPlayer =>
        FilterResult(env.run.formerMemberIds.contains(apiPlayer.playerId), env.candidate)
      }
  }

  // --- Per-criterion cache checks ---

  private case class CacheCriterion(
    stalenessDays: Long,
    check: (PlayerRecruitmentCache, RecruitmentCriteria) => Boolean
  )

  private val cacheCriteria: List[CacheCriterion] = List(
    // Zero-tolerance daily timeout (90d — matches API stats window)
    CacheCriterion(
      90L,
      (cache, criteria) => criteria.dailyMaxTimeoutPercent.contains(0.0) && cache.lastDailyTimeoutAt.isDefined
    ),
    // Zero-tolerance TM timeout (90d — matches API stats window)
    CacheCriterion(
      90L,
      (cache, criteria) => criteria.dailyMaxTmTimeoutPercent.contains(0.0) && cache.lastTmTimeoutAt.isDefined
    ),
    // Max clubs (30d)
    CacheCriterion(30L, (cache, criteria) => criteria.maxClubs.exists(max => cache.clubCount.exists(_ > max))),
    // Min daily ELO (30d)
    CacheCriterion(30L, (cache, criteria) => criteria.dailyMinElo.exists(min => cache.dailyElo.exists(_.value < min.value))),
    // Max daily ELO (30d)
    CacheCriterion(30L, (cache, criteria) => criteria.dailyMaxElo.exists(max => cache.dailyElo.exists(_.value > max.value))),
    // Min daily score rate (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMinScoreRate.exists(min => cache.dailyScoreRate.exists(_ < min))
    ),
    // Max daily score rate (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMaxScoreRate.exists(max => cache.dailyScoreRate.exists(_ > max))
    ),
    // Max daily timeout % (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMaxTimeoutPercent.exists(max => cache.dailyTimeoutPct.exists(_ > max))
    ),
    // Min daily games finished (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMinGamesFinished.exists(min => cache.dailyGamesFinished.exists(_ < min))
    ),
    // Min ongoing games (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMinOngoingGames.exists(min => cache.ongoingGames.exists(_ < min))
    ),
    // Max ongoing games (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMaxOngoingGames.exists(max => cache.ongoingGames.exists(_ > max))
    ),
    // Min ongoing team matches (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMinOngoingTeamMatches.exists(min => cache.ongoingTeamMatches.exists(_ < min))
    ),
    // Min TM games finished 90d (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMinTmGamesFinished.exists(min => cache.tmGamesFinished90d.exists(_ < min))
    ),
    // Max TM timeout % 90d (30d)
    CacheCriterion(
      30L,
      (cache, criteria) => criteria.dailyMaxTmTimeoutPercent.exists(max => cache.tmTimeoutPct90d.exists(_ > max))
    )
  )

  private def runCacheCriteria(
    cache: PlayerRecruitmentCache,
    criteria: RecruitmentCriteria,
    now: Instant
  ): Boolean = {
    val ageDays = ChronoUnit.DAYS.between(cache.fetchedAt, now)
    cacheCriteria.exists(c => c.stalenessDays > ageDays && c.check(cache, criteria))
  }

  object CheckCacheCriteria extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      ZIO.succeed {
        val rejected = env.candidate.cache.exists(runCacheCriteria(_, env.run.criteria, env.run.now))
        FilterResult(rejected, if (rejected) env.candidate.copy(cacheRejected = true) else env.candidate)
      }
  }

  object CheckOpponentMatch extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      env.run.client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(env.candidate.username)).map { playerMatches =>
        val registeredIds = playerMatches.registered.map(_.`@id`).toSet ++
          playerMatches.inProgress.map(_.`@id`).toSet
        FilterResult(
          registeredIds.exists(env.run.clubMatchIds.contains),
          env.candidate.copy(playerMatches = Some(playerMatches))
        )
      }
  }

  object CheckClubs extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        playerClubs <- env.run.client.get[ApiPlayerClubs](ApiPlayerClubs.getUrl(env.candidate.username))
        clubNames = playerClubs.clubs.map(_.clubName).toSet
        _ <- requireApiPlayer(env)
        _ <- env.run.discoveredClubs.update(_ ++ clubNames)
      } yield {
        val clubCount = playerClubs.clubs.size
        val criteria  = env.run.criteria
        val rejected =
          criteria.maxClubs.exists(clubCount > _)
            || env.run.excludedSlugs.exists(clubNames.contains)
        val updatedCache = getOrUpdateCache(env)(_.copy(clubCount = Some(clubCount)))
        FilterResult(rejected, env.candidate.copy(cache = Some(updatedCache)))
      }
  }

  object CheckDailyStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      env.run.client.get[ApiPlayerStats](ApiPlayerStats.getUrl(env.candidate.username)).flatMap { playerStats =>
        playerStats.chessDaily match {
          case None             => ZIO.succeed(FilterResult(true, env.candidate))
          case Some(dailyStats) => applyDailyStats(env, dailyStats)
        }
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
            val months = recentArchiveMonths(env.run.now, 90)
            ZIO.foreachPar(months) { ym =>
              val url = ApiPlayerArchive.getUrl(env.candidate.username, ym.getYear, ym.getMonthValue)
              env.run.client.get[ApiPlayerArchive](url)
            }
          }
        _ <- requireApiPlayer(env)
      } yield {
        val cutoff90d = env.run.now.minus(90, ChronoUnit.DAYS)
        val dailyGamesFinished90d = archives.map(
          _.flatMap(_.games.filter(g => g.timeClass == "daily" && g.endTime >= cutoff90d.getEpochSecond)).size
        )
        val lastDailyTimeoutAt = archives.flatMap(extractLastDailyTimeout(_, env.candidate.username))
        val mergedDailyTimeout = mergeOptionalInstants(
          lastDailyTimeoutAt,
          env.candidate.cache.flatMap(_.lastDailyTimeoutAt)
        )
        val dailyTimePerMove = dailyStats.record.timePerMove
        val criteria         = env.run.criteria
        val rejected =
          criteria.dailyMinElo.exists(min => dailyElo.value < min.value)
            || criteria.dailyMaxElo.exists(max => dailyElo.value > max.value)
            || criteria.dailyMinScoreRate.exists(dailyStats.record.scoreRate < _)
            || criteria.dailyMaxScoreRate.exists(dailyStats.record.scoreRate > _)
            || criteria.dailyMaxTimeoutPercent.exists(dailyTimeoutPct > _)
            || criteria.dailyMinGamesFinished.exists(min => dailyGamesFinished90d.getOrElse(dailyGamesFinished) < min)
            || criteria.dailyMaxHoursPerMove.exists(maxHours => dailyTimePerMove > maxHours * 3600)
        val updatedCache = getOrUpdateCache(env)(
          _.copy(
            fetchedAt = env.run.now,
            dailyElo = Some(dailyElo),
            dailyTimeoutPct = Some(dailyTimeoutPct),
            dailyGamesFinished = Some(dailyGamesFinished90d.getOrElse(dailyGamesFinished)),
            lastDailyTimeoutAt = mergedDailyTimeout,
            dailyScoreRate = Some(dailyStats.record.scoreRate)
          )
        )
        FilterResult(rejected, env.candidate.copy(cache = Some(updatedCache), recentArchives = archives))
      }
    }
  }

  object CheckOngoingGames extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        currentGames <- env.run.client.get[ApiPlayerGamesCurrent](ApiPlayerGamesCurrent.getUrl(env.candidate.username))
        _            <- requireApiPlayer(env)
      } yield {
        val ongoingGames       = currentGames.games.size
        val ongoingTeamMatches = currentGames.games.count(_.`match`.isDefined)
        val criteria           = env.run.criteria
        val rejected =
          criteria.dailyMinOngoingGames.exists(ongoingGames < _)
            || criteria.dailyMaxOngoingGames.exists(ongoingGames > _)
            || criteria.dailyMinOngoingTeamMatches.exists(ongoingTeamMatches < _)
        val updatedCache = getOrUpdateCache(env)(
          _.copy(ongoingGames = Some(ongoingGames), ongoingTeamMatches = Some(ongoingTeamMatches))
        )
        FilterResult(rejected, env.candidate.copy(cache = Some(updatedCache)))
      }
  }

  object CheckTmStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[PostgresClient, FilterResult] =
      for {
        cache <- ZIO.fromOption(env.candidate.cache)
          .orElseFail(new NoSuchElementException("cache not set — CheckDailyStats must run before CheckTmStats"))
        tmStats <- fetchTmStats(
          env.run.client,
          env.candidate.username,
          env.run.criteria,
          cache.dailyTimeoutPct.getOrElse(0.0),
          env.run.now,
          env.candidate.recentArchives
        )
        _ <- env.run.discoveredOpponents.update(_ ++ tmStats.opponentUsernames)
      } yield {
        val mergedTmTimeout = mergeOptionalInstants(tmStats.lastTimeoutAt, cache.lastTmTimeoutAt)
        val updatedCache = cache.copy(
          tmGamesFinished90d = Some(tmStats.gamesFinished),
          tmTimeoutPct90d = tmStats.timeoutPct,
          lastTmTimeoutAt = mergedTmTimeout
        )
        val criteria = env.run.criteria
        val rejected =
          criteria.dailyMinTmGamesFinished.exists(tmStats.gamesFinished < _)
            || criteria.dailyMaxTmTimeoutPercent.exists(max => tmStats.timeoutPct.exists(_ > max))
        FilterResult(rejected, env.candidate.copy(cache = Some(updatedCache)))
      }
  }
}
