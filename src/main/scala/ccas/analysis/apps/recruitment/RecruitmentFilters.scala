package ccas.analysis.apps.recruitment

import java.time.{Instant, LocalDate, YearMonth, ZoneOffset}
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.Transactor
import zio.{RIO, Ref, ZIO}
import zio.http.URL

import ccas.analysis.tables.*
import ccas.api.misc.enums.GameResultDetail
import ccas.api.misc.subtypes.Username
import ccas.api.player.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.safeMessage
import ccas.utils.sql.SqlZioTypes.withTransaction
import ccas.api.misc.enums.PlayerStatusCategory

private[recruitment] object RecruitmentFilters {

  // --- Public API ---

  def evaluateCandidate(
    runId: Long,
    username: Username,
    runCtx: RunContext,
    filters: List[RecruitmentFilter]
  ): RIO[Transactor, CandidateOutcome] = {
    val now          = Instant.now()
    val candidateCtx = CandidateContext.initial(username)
    val env          = FilterEnv(runCtx.copy(now = now), candidateCtx)
    def onEvaluationError(ctxRef: Ref[CandidateContext])(error: Throwable): RIO[Transactor, CandidateOutcome] =
      ctxRef.get.flatMap { latestCtx =>
        persistCandidateResults(runId, now, latestCtx, CandidateOutcome.Error, Some(error.safeMessage))
      }.as(CandidateOutcome.Error)

    for {
      ctxRef <- Ref.make(candidateCtx)
      result <- (for {
        (outcome, finalCandidate) <- runFilters(env, filters, ctxRef)
        _                         <- persistCandidateResults(runId, now, finalCandidate, outcome)
      } yield outcome).catchAll(onEvaluationError(ctxRef))
    } yield result
  }

  def buildFilterChain(criteria: RecruitmentCriteria): List[RecruitmentFilter] = {
    val base = List(
      FetchAndCheckPlayer,
      CheckInvitedTooRecently,
      CheckBlacklist
    )
    val formerMember = Option.when(criteria.excludeFormerMembers)(CheckFormerMember)
    val rest = List(
      CheckCacheCriteria,
      CheckOpponentMatch,
      CheckClubs,
      CheckDailyStats,
      CheckOngoingGames
    )
    val teamMatch = Option.when(
      criteria.dailyMinTmGamesFinished.isDefined || criteria.dailyMaxTmTimeoutPercent.isDefined
    )(CheckTmStats)
    base ++ formerMember ++ rest ++ teamMatch
  }

  // --- Pipeline runner ---

  private def runFilters(
    env: FilterEnv,
    filters: List[RecruitmentFilter],
    ctxRef: Ref[CandidateContext]
  ): RIO[Transactor, (CandidateOutcome, CandidateContext)] =
    ZIO.foldLeft(filters)(FilterResult(false, env.candidate)) {
      case (r @ FilterResult(true, _), _)     => ZIO.succeed(r)
      case (FilterResult(false, ctx), filter) => ctxRef.set(ctx) *> filter(env.copy(candidate = ctx))
    }.map(r => (if (r.rejected) CandidateOutcome.Rejected else CandidateOutcome.Invited, r.candidate))

  // --- Helpers ---

  private def requireApiPlayer(env: FilterEnv): zio.IO[NoSuchElementException, ApiPlayer] =
    ZIO.fromOption(env.candidate.apiPlayer)
      .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run first"))

  private def getOrUpdateCache(env: FilterEnv)
                              (update: PlayerRecruitmentCache => PlayerRecruitmentCache): PlayerRecruitmentCache = {
    val playerId = env.candidate.apiPlayer.get.playerId
    val base = env.candidate.cache.getOrElse(
      PlayerRecruitmentCache.empty(playerId, env.run.now, None)
    )
    update(base)
  }

  // --- Filter implementations ---

  private object CheckInvitedTooRecently extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
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

  private object FetchAndCheckPlayer extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
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
            val listed = criteria.nationalityCountries.contains(countryCode)
            criteria.nationalityExclude == listed
          })
        FilterResult(rejected, updatedCtx)
      }
  }

  private object CheckBlacklist extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        apiPlayer   <- requireApiPlayer(env)
        blacklisted <- RecruitmentBlacklist.isBlacklisted(env.run.clubId, apiPlayer.playerId, env.run.now)
      } yield FilterResult(blacklisted, env.candidate)
  }

  private object CheckFormerMember extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
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
    CacheCriterion(90L, (cache, criteria) =>
      criteria.dailyMaxTimeoutPercent.contains(0.0) && cache.lastDailyTimeoutAt.isDefined
    ),
    // Zero-tolerance TM timeout (90d — matches API stats window)
    CacheCriterion(90L, (cache, criteria) =>
      criteria.dailyMaxTmTimeoutPercent.contains(0.0) && cache.lastTmTimeoutAt.isDefined
    ),
    // Max clubs (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.maxClubs.exists(max => cache.clubCount.exists(_ > max))
    ),
    // Min daily ELO (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMinElo.exists(min => cache.dailyElo.exists(_ < min))
    ),
    // Max daily ELO (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMaxElo.exists(max => cache.dailyElo.exists(_ > max))
    ),
    // Max daily timeout % (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMaxTimeoutPercent.exists(max => cache.dailyTimeoutPct.exists(_ > max))
    ),
    // Min daily games finished (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMinGamesFinished.exists(min => cache.dailyGamesFinished.exists(_ < min))
    ),
    // Min ongoing games (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMinOngoingGames.exists(min => cache.ongoingGames.exists(_ < min))
    ),
    // Max ongoing games (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMaxOngoingGames.exists(max => cache.ongoingGames.exists(_ > max))
    ),
    // Min ongoing team matches (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMinOngoingTeamMatches.exists(min => cache.ongoingTeamMatches.exists(_ < min))
    ),
    // Min TM games finished 90d (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMinTmGamesFinished.exists(min => cache.tmGamesFinished90d.exists(_ < min))
    ),
    // Max TM timeout % 90d (30d)
    CacheCriterion(30L, (cache, criteria) =>
      criteria.dailyMaxTmTimeoutPercent.exists(max => cache.tmTimeoutPct90d.exists(_ > max))
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

  private object CheckCacheCriteria extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      ZIO.succeed {
        val rejected = env.candidate.cache.exists(runCacheCriteria(_, env.run.criteria, env.run.now))
        FilterResult(rejected, if (rejected) env.candidate.copy(cacheRejected = true) else env.candidate)
      }
  }

  private object CheckOpponentMatch extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      env.run.client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(env.candidate.username)).map { playerMatches =>
        val registeredIds = playerMatches.registered.map(_.`@id`).toSet ++
          playerMatches.inProgress.map(_.`@id`).toSet
        FilterResult(registeredIds.exists(env.run.clubMatchIds.contains), env.candidate)
      }
  }

  private object CheckClubs extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
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
          || criteria.excludeClubNames.exists(clubNames.contains)
        val updatedCache = getOrUpdateCache(env)(_.copy(clubCount = Some(clubCount)))
        FilterResult(rejected, env.candidate.copy(cache = Some(updatedCache)))
      }
  }

  private object CheckDailyStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      env.run.client.get[ApiPlayerStats](ApiPlayerStats.getUrl(env.candidate.username)).flatMap { playerStats =>
        playerStats.chessDaily match {
          case None             => ZIO.succeed(FilterResult(true, env.candidate))
          case Some(dailyStats) => applyDailyStats(env, dailyStats)
        }
      }

    private def applyDailyStats(
      env: FilterEnv,
      dailyStats: ApiPlayerStats.ApiPlayerDailyStats
    ): RIO[Transactor, FilterResult] = {
      def logAndReraise(url: URL)(e: Throwable): RIO[Transactor, Nothing] =
        ApiFetchFailure.insert(
          ApiFetchFailure(url.toString, e.getClass.getSimpleName, Option(e.getMessage), env.run.now)
        ).orDie *> ZIO.fail(e)

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
              env.run.client.get[ApiPlayerArchive](url).catchAll(logAndReraise(url))
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
          criteria.dailyMinElo.exists(dailyElo.value < _)
          || criteria.dailyMaxElo.exists(dailyElo.value > _)
          || criteria.dailyMaxTimeoutPercent.exists(dailyTimeoutPct > _)
          || criteria.dailyMinGamesFinished.exists(min => dailyGamesFinished90d.getOrElse(dailyGamesFinished) < min)
          || criteria.dailyMaxHoursPerMove.exists(maxHours => dailyTimePerMove > maxHours * 3600)
        val updatedCache = getOrUpdateCache(env)(
          _.copy(
            fetchedAt = env.run.now,
            dailyElo = Some(dailyElo.value),
            dailyTimeoutPct = Some(dailyTimeoutPct),
            dailyGamesFinished = Some(dailyGamesFinished90d.getOrElse(dailyGamesFinished)),
            lastDailyTimeoutAt = mergedDailyTimeout
          )
        )
        FilterResult(rejected, env.candidate.copy(cache = Some(updatedCache), recentArchives = archives))
      }
    }
  }

  private object CheckOngoingGames extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        currentGames <- env.run.client.get[ApiPlayerGamesCurrent](ApiPlayerGamesCurrent.getUrl(env.candidate.username))
        _ <- requireApiPlayer(env)
      } yield {
        val ongoingGames       = currentGames.games.size
        val ongoingTeamMatches = currentGames.games.count(_.`match`.isDefined)
        val criteria = env.run.criteria
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

  private object CheckTmStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        cache <- ZIO.fromOption(env.candidate.cache)
          .orElseFail(new NoSuchElementException("cache not set — CheckDailyStats must run before CheckTmStats"))
        tmStats <- fetchTmStats(
          env.run.client, env.candidate.username, env.run.criteria,
          cache.dailyTimeoutPct.getOrElse(0.0), env.run.now, env.candidate.recentArchives
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

  // --- Deferred DB writes ---

  private def persistCandidateResults(
    runId: Long,
    now: Instant,
    candidate: CandidateContext,
    outcome: CandidateOutcome,
    errorMessage: Option[String] = None
  ): RIO[Transactor, Unit] =
    // No player data (transient API error) — skip persistence, retry next run
    ZIO.foreachDiscard(candidate.apiPlayer) { ap =>
      withTransaction {
        for {
          _ <- ZIO.whenDiscard(candidate.isNewPlayer)(
            Player.insert(Player(ap.playerId, Instant.ofEpochSecond(ap.joined)))
          )
          _ <- {
            val snap = PlayerSnapshot(ap.playerId, now, candidate.username, ap.status.category, ap.title)
            PlayerSnapshot.selectIdLatest(ap.playerId).flatMap {
              case Some(latest)
                  if latest.username == snap.username && latest.status == snap.status && latest.title == snap.title =>
                ZIO.unit
              case _ =>
                PlayerSnapshot.insert(snap)
            }
          }
          _ <- ZIO.foreachDiscard(candidate.cache)(PlayerRecruitmentCache.upsert)
          // Skip candidate row for cache-only rejections so they aren't blocked by daysSinceRejected
          _ <- ZIO.unlessDiscard(candidate.cacheRejected)(
            RecruitmentCandidate.insert(RecruitmentCandidate(runId, ap.playerId, now, outcome, errorMessage))
          )
        } yield ()
      }
    }

  // --- TM stats helpers ---

  private def fetchTmStats(
    client: ChessComClient,
    username: Username,
    criteria: RecruitmentCriteria,
    overallTimeoutPct: Double,
    now: Instant,
    recentArchives: Option[List[ApiPlayerArchive]]
  ): RIO[Transactor, TmStatsResult] = {
    val needsTmStats = criteria.dailyMinTmGamesFinished.isDefined || criteria.dailyMaxTmTimeoutPercent.isDefined
    if (!needsTmStats) ZIO.succeed(TmStatsResult(0, None, None, Set.empty))
    else {
      // Fetch last ~90 days of archives
      val cutoff = now.minus(90, ChronoUnit.DAYS)
      val months = recentArchiveMonths(now, 90)

      (recentArchives match {
        case Some(cached) => ZIO.succeed(cached)
        case None =>
          ZIO.foreachPar(months) { ym =>
            val url = ApiPlayerArchive.getUrl(username, ym.getYear, ym.getMonthValue)
            client.get[ApiPlayerArchive](url).tapError { e =>
              ApiFetchFailure.insert(
                ApiFetchFailure(url.toString, e.getClass.getSimpleName, Option(e.getMessage), now)
              )
            }
          }
      }).map { archives =>
        val tmGames = archives.flatMap(
          _.games.filter(g => g.timeClass == "daily" && g.`match`.isDefined && g.endTime >= cutoff.getEpochSecond)
        )
        val tmGamesFinished = tmGames.size
        val tmTimeoutPct =
          if (tmGamesFinished == 0) None
          else if (criteria.dailyMaxTmTimeoutPercent.isDefined && overallTimeoutPct == 0.0) Some(0.0)
          else {
            val timeouts = tmGames.count(g => playerResult(g, username) == GameResultDetail.Timeout)
            Some(timeouts.toDouble / tmGamesFinished * 100.0)
          }
        val lastTmTimeoutAt = tmGames
          .filter(g => playerResult(g, username) == GameResultDetail.Timeout)
          .sortBy(_.endTime)(using Ordering[Long].reverse)
          .headOption
          .map(g => Instant.ofEpochSecond(g.endTime))
        val opponentUsernames = tmGames.flatMap(nonTimeoutOpponent(_, username)).toSet
        TmStatsResult(tmGamesFinished, tmTimeoutPct, lastTmTimeoutAt, opponentUsernames)
      }
    }
  }

  private[recruitment] def extractLastDailyTimeout(archives: List[ApiPlayerArchive], username: Username): Option[Instant] =
    archives.flatMap(_.games)
      .filter(g => g.timeClass == "daily" && g.`match`.isEmpty) // non-match daily games
      .filter(g => playerResult(g, username) == GameResultDetail.Timeout)
      .sortBy(_.endTime)(using Ordering[Long].reverse)
      .headOption
      .map(g => Instant.ofEpochSecond(g.endTime))

  private[recruitment] def mergeOptionalInstants(a: Option[Instant], b: Option[Instant]): Option[Instant] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(if (x.isAfter(y)) x else y)
      case (Some(x), None)    => Some(x)
      case (None, Some(y))    => Some(y)
      case (None, None)       => None
    }

  private[recruitment] def recentArchiveMonths(now: Instant, days: Int): List[YearMonth] = {
    val today      = LocalDate.ofInstant(now, ZoneOffset.UTC)
    val cutoff     = today.minusDays(days)
    val startMonth = YearMonth.from(cutoff)
    val endMonth   = YearMonth.from(today)
    Iterator.iterate(startMonth)(_.plusMonths(1)).takeWhile(!_.isAfter(endMonth)).toList
  }
}
