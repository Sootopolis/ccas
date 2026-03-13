package ccas.analysis.apps.recruitment

import java.time.{Instant, LocalDate, YearMonth, ZoneOffset}
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Chunk, Console, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Client, URL}

import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.misc.enums.{GameResultDetail, PlayerStatusCategory}
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.api.player.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.transactZIO

object RecruitmentApp extends ZIOAppDefault {

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- (args.toList match
        case "report" :: clubStr :: rest => showReport(ClubUrlName.wrap(clubStr), rest.headOption)
        case clubStr :: rest => recruit(ClubUrlName.wrap(clubStr), rest.headOption.getOrElse("default"))
        case _ =>
          ZIO.fail(
            ExternalException(
              "Usage: RecruitmentApp <club-url-name> [config-name]\n       RecruitmentApp report <club-url-name> [run-id]"
            )
          )
      ).provide(
        ChessComClient.live(),
        Client.default,
        DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
      )
    } yield ()

  // --- Phase 1: Initialize ---

  private[recruitment] def recruit(clubUrlName: ClubUrlName, configName: String)
      : ZIO[ChessComClient & Transactor, Throwable, RecruitmentRun] =
    for {
      client  <- ZIO.service[ChessComClient]
      apiClub <- ApiClub.get(client, clubUrlName)
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), clubUrlName)
      _ <- Club.upsert(club)
      config <- RecruitmentConfig.select(clubId, configName)
        .someOrFail(ExternalException(s"No recruitment config '$configName' found for club '$clubUrlName'"))
      now = Instant.now()
      runId <- RecruitmentRun.insert(clubId, configName, now)

      // --- Phase 2: Gather candidate usernames ---
      candidates <- gatherCandidates(client, clubId, clubUrlName, config)

      // --- Phase 3: Evaluate candidates ---
      invited <- evaluateCandidates(client, runId, clubUrlName, candidates, config)

      // --- Phase 4: Finalize ---
      completedAt = Instant.now()
      finalRun    = RecruitmentRun(runId, clubId, configName, now, Some(completedAt), invited.size)
      _ <- RecruitmentRun.update(finalRun)
      _ <- Console.printLine(s"=== Recruitment Complete ===").orDie
      _ <- Console.printLine(s"Candidates evaluated: ${candidates.size}").orDie
      _ <- Console.printLine(s"Invited: ${invited.size}").orDie
      _ <- ZIO.foreachDiscard(invited)(u => Console.printLine(s"  $u").orDie)
    } yield finalRun

  // --- Phase 2: Gather candidate usernames ---

  private[recruitment] def gatherCandidates(
      client: ChessComClient,
      clubId: ClubId,
      clubUrlName: ClubUrlName,
      config: RecruitmentConfig
    ): ZIO[Transactor, Throwable, List[Username]] =
    for {
      // Fetch target club members
      targetMembers <- ApiClubMembers.get(client, clubUrlName)
      existingUsernames = targetMembers.toMap.keySet

      // Fetch members from all source clubs
      sourceMembers <- ZIO.foreachPar(config.sourceClubNames) { sourceClubName =>
        ApiClubMembers.get(client, sourceClubName).map(_.toMap.keySet)
      }

      // Combine, deduplicate, and filter out existing members
      allSourceUsernames = sourceMembers.foldLeft(Set.empty[Username])(_ ++ _)
      candidatesBeforeAdminFilter = (allSourceUsernames -- existingUsernames)

      // Optionally exclude source club admins
      adminUsernames <- if (config.excludeSourceAdmins) {
        ZIO.foreachPar(config.sourceClubNames) { sourceClubName =>
          ApiClub.get(client, sourceClubName).map { apiClub =>
            apiClub.admin.map(url => Username.wrap(url.path.segments.last)).toSet
          }
        }.map(_.foldLeft(Set.empty[Username])(_ ++ _))
      } else ZIO.succeed(Set.empty[Username])

      candidates = (candidatesBeforeAdminFilter -- adminUsernames).toList
    } yield candidates

  // --- Phase 3: Evaluate candidates ---

  private[recruitment] def evaluateCandidates(
      client: ChessComClient,
      runId: Long,
      clubUrlName: ClubUrlName,
      candidates: List[Username],
      config: RecruitmentConfig
    ): ZIO[Transactor, Throwable, List[Username]] =
    for {
      // Pre-fetch target club's registered match IDs (for opponent check)
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubUrlName))
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet

      toEvaluate = candidates.take(config.maxCandidates)
      runCtx = RunContext(client, config, targetMatchIds, Instant.now())
      filters = buildFilterChain(config)
      revInvited <- ZIO.foldLeft(toEvaluate)(List.empty[Username]) { case (invited, username) =>
        val now = Instant.now()
        val candidateCtx = CandidateContext.initial(username)
        val env = FilterEnv(runCtx.copy(now = now), candidateCtx)
        (for {
          (outcome, finalCandidate) <- runFilters(env, filters)
          _ <- persistCandidateResults(runId, now, finalCandidate, outcome)
        } yield if (outcome == CandidateOutcome.Invited) username :: invited else invited).catchAll { error =>
          persistCandidateResults(
            runId,
            now,
            candidateCtx,
            CandidateOutcome.Error,
            Some(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
          ).as(invited)
        }
      }
    } yield revInvited.reverse

  // --- Filter pipeline types ---

  /** Immutable, shared across all candidates in a run. */
  private case class RunContext(
      client: ChessComClient,
      config: RecruitmentConfig,
      clubMatchIds: Set[URL],
      now: Instant
  )

  /** Accumulated per-candidate state — populated as filters run. */
  private case class CandidateContext(
      username: Username,
      apiPlayer: Option[ApiPlayer],
      isNewPlayer: Boolean,
      clubCount: Option[Int],
      cache: Option[PlayerRecruitmentCache]
  )
  private object CandidateContext {
    def initial(username: Username): CandidateContext =
      CandidateContext(username, apiPlayer = None, isNewPlayer = false, clubCount = None, cache = None)
  }

  /** Groups contexts passed to each filter. */
  private case class FilterEnv(run: RunContext, candidate: CandidateContext)

  private case class FilterResult(outcome: Option[CandidateOutcome], candidate: CandidateContext)

  private trait RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult]
  }

  // --- Pipeline runner ---

  private def runFilters(env: FilterEnv, filters: List[RecruitmentFilter])
      : ZIO[Transactor, Throwable, (CandidateOutcome, CandidateContext)] =
    ZIO.foldLeft(filters)(FilterResult(None, env.candidate)) {
      case (r @ FilterResult(Some(_), _), _) => ZIO.succeed(r)
      case (FilterResult(None, ctx), filter) => filter(env.copy(candidate = ctx))
    }.map(r => (r.outcome.getOrElse(CandidateOutcome.Invited), r.candidate))

  // --- Filter chain builder ---

  private def buildFilterChain(config: RecruitmentConfig): List[RecruitmentFilter] = {
    val base = List(
      CheckInvitedTooRecently,
      FetchAndCheckPlayer,
      CheckOpponentMatch,
      FetchAndCheckClubs,
      FetchDailyStatsAndCheck
    )
    val tm = Option.when(
      config.dailyMinTmGamesFinished.isDefined || config.dailyMaxTmTimeoutPercent.isDefined
    )(FetchTmStatsAndCheck)
    base ++ tm
  }

  // --- Filter implementations ---

  private object CheckInvitedTooRecently extends RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult] =
      for {
        recentInvite <- RecruitmentCandidate.selectLatestInvited(env.candidate.username)
        tooRecent = env.run.config.daysSinceLastInvited.exists { days =>
          recentInvite.exists(c => ChronoUnit.DAYS.between(c.evaluatedAt, env.run.now) < days)
        }
      } yield FilterResult(Option.when(tooRecent)(CandidateOutcome.Rejected), env.candidate)
  }

  private object FetchAndCheckPlayer extends RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult] =
      for {
        apiPlayer <- env.run.client.getWithPermit[ApiPlayer](ApiPlayer.getUrl(env.candidate.username))
        existingPlayer <- Player.selectId(apiPlayer.playerId)
        statusCat = apiPlayer.status.category
        config = env.run.config
        now = env.run.now

        updatedCtx = env.candidate.copy(
          apiPlayer = Some(apiPlayer),
          isNewPlayer = existingPlayer.isEmpty
        )

        outcome =
          if (statusCat != PlayerStatusCategory.Active) Some(CandidateOutcome.Rejected)
          else if (config.minDaysSinceRegistration.exists { days =>
            ChronoUnit.DAYS.between(Instant.ofEpochSecond(apiPlayer.joined), now) < days
          }) Some(CandidateOutcome.Rejected)
          else if (config.nationalityMode.exists { mode =>
            val countryCode = apiPlayer.country.path.segments.last
            mode match {
              case "include" => !config.nationalityCountries.contains(countryCode)
              case "exclude" => config.nationalityCountries.contains(countryCode)
              case _         => false
            }
          }) Some(CandidateOutcome.Rejected)
          else None
      } yield FilterResult(outcome, updatedCtx)
  }

  private val cacheFreshnessHours = 24L

  private object CheckOpponentMatch extends RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult] =
      for {
        playerMatches <- env.run.client.getWithPermit[ApiPlayerMatches](
          ApiPlayerMatches.getUrl(env.candidate.username)
        )
        playerRegisteredIds = playerMatches.registered.map(_.`@id`).toSet ++
          playerMatches.inProgress.map(_.`@id`).toSet
        hasOpponentMatch = playerRegisteredIds.exists(env.run.clubMatchIds.contains)

        // Also check cache freshness — if fresh, short-circuit with cached thresholds
        apiPlayer <- ZIO.fromOption(env.candidate.apiPlayer)
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckOpponentMatch"))
        cached <- PlayerRecruitmentCache.selectId(apiPlayer.playerId)
        cacheIsFresh = cached.exists(c =>
          ChronoUnit.HOURS.between(c.fetchedAt, env.run.now) < cacheFreshnessHours
        )

        outcome =
          if (hasOpponentMatch) Some(CandidateOutcome.Rejected)
          else if (cacheIsFresh) Some(checkThresholdsSync(cached.get, env.run.config))
          else None
      } yield FilterResult(outcome, env.candidate)
  }

  private object FetchAndCheckClubs extends RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult] =
      for {
        playerClubs <- env.run.client.getWithPermit[ApiPlayerClubs](
          ApiPlayerClubs.getUrl(env.candidate.username)
        )
        clubCount = playerClubs.clubs.size
        clubNames = playerClubs.clubs.map(_.clubName).toSet
        config = env.run.config

        updatedCtx = env.candidate.copy(clubCount = Some(clubCount))
        outcome =
          if (config.maxClubs.exists(clubCount > _)) Some(CandidateOutcome.Rejected)
          else if (config.excludeClubNames.exists(clubNames.contains)) Some(CandidateOutcome.Rejected)
          else None
      } yield FilterResult(outcome, updatedCtx)
  }

  private object FetchDailyStatsAndCheck extends RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult] =
      for {
        playerStats <- env.run.client.getWithPermit[ApiPlayerStats](
          ApiPlayerStats.getUrl(env.candidate.username)
        )
        dailyStats = playerStats.chessDaily
        dailyElo = dailyStats.last.rating
        dailyTimeoutPct = dailyStats.record.timeoutPercent
        dailyGamesFinished = dailyStats.record.nGames

        currentGames <- env.run.client.getWithPermit[ApiPlayerGamesCurrent](
          ApiPlayerGamesCurrent.getUrl(env.candidate.username)
        )
        ongoingGames = currentGames.games.size
        ongoingTeamMatches = currentGames.games.count(_.`match`.isDefined)

        apiPlayer <- ZIO.fromOption(env.candidate.apiPlayer)
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before FetchDailyStatsAndCheck"))
        cache = PlayerRecruitmentCache(
          playerId = apiPlayer.playerId,
          fetchedAt = env.run.now,
          dailyElo = Some(dailyElo),
          dailyTimeoutPct = Some(dailyTimeoutPct),
          dailyGamesFinished = Some(dailyGamesFinished),
          clubCount = env.candidate.clubCount,
          ongoingGames = ongoingGames,
          ongoingTeamMatches = ongoingTeamMatches,
          tmGamesFinished90d = 0,
          tmTimeoutPct90d = None
        )
        updatedCtx = env.candidate.copy(cache = Some(cache))
        outcome = checkThresholdsSync(cache, env.run.config)
      } yield FilterResult(Some(outcome).filter(_ == CandidateOutcome.Rejected), updatedCtx)
  }

  private object FetchTmStatsAndCheck extends RecruitmentFilter {
    def apply(env: FilterEnv): ZIO[Transactor, Throwable, FilterResult] = {
      val cache = env.candidate.cache
        .getOrElse(throw new NoSuchElementException("cache not set — FetchDailyStatsAndCheck must run before FetchTmStatsAndCheck"))
      for {
        tmResult <- fetchTmStats(
          env.run.client,
          env.candidate.username,
          env.run.config,
          cache.dailyTimeoutPct.getOrElse(0.0),
          env.run.now
        )
        (tmGamesFinished, tmTimeoutPct) = tmResult
        updatedCache = cache.copy(
          tmGamesFinished90d = tmGamesFinished,
          tmTimeoutPct90d = tmTimeoutPct
        )
        updatedCtx = env.candidate.copy(cache = Some(updatedCache))
        outcome = checkThresholdsSync(updatedCache, env.run.config)
      } yield FilterResult(Some(outcome).filter(_ == CandidateOutcome.Rejected), updatedCtx)
    }
  }

  // --- Threshold checks ---

  private def checkThresholdsSync(
      cache: PlayerRecruitmentCache,
      config: RecruitmentConfig
    ): CandidateOutcome =
    if (config.maxClubs.exists(max => cache.clubCount.exists(_ > max))) CandidateOutcome.Rejected
    else if (config.dailyMinElo.exists(min => cache.dailyElo.exists(_ < min))) CandidateOutcome.Rejected
    else if (config.dailyMaxElo.exists(max => cache.dailyElo.exists(_ > max))) CandidateOutcome.Rejected
    else if (config.dailyMaxTimeoutPercent.exists(max => cache.dailyTimeoutPct.exists(_ > max))) CandidateOutcome.Rejected
    else if (config.dailyMinGamesFinished.exists(min => cache.dailyGamesFinished.exists(_ < min))) CandidateOutcome.Rejected
    else if (config.dailyMinOngoingGames.exists(cache.ongoingGames < _)) CandidateOutcome.Rejected
    else if (config.dailyMaxOngoingGames.exists(cache.ongoingGames > _)) CandidateOutcome.Rejected
    else if (config.dailyMinOngoingTeamMatches.exists(cache.ongoingTeamMatches < _)) CandidateOutcome.Rejected
    else if (config.dailyMinTmGamesFinished.exists(cache.tmGamesFinished90d < _)) CandidateOutcome.Rejected
    else if (config.dailyMaxTmTimeoutPercent.exists(max => cache.tmTimeoutPct90d.exists(_ > max))) CandidateOutcome.Rejected
    else CandidateOutcome.Invited

  // --- Deferred DB writes ---

  private def persistCandidateResults(
      runId: Long,
      now: Instant,
      candidate: CandidateContext,
      outcome: CandidateOutcome,
      errorMessage: Option[String] = None
    ): ZIO[Transactor, Throwable, Unit] = transactZIO {
    // 1. Insert Player if new
    candidate.apiPlayer.filter(_ => candidate.isNewPlayer).foreach { ap =>
      sql"""INSERT INTO player (player_id, joined, board_url)
            VALUES (${ap.playerId}, ${Instant.ofEpochSecond(ap.joined)}, ${None: Option[String]})""".update.run()
    }
    // 2. Insert PlayerSnapshot (always, if apiPlayer was fetched)
    candidate.apiPlayer.foreach { ap =>
      val statusCat = ap.status.category
      sql"""INSERT INTO player_snapshot (player_id, since, username, status, title)
            VALUES (${ap.playerId}, $now, ${candidate.username}, ${statusCat.toString}, ${ap.title.map(_.toString)})""".update.run()
    }
    // 3. Upsert PlayerRecruitmentCache (if fresh data was gathered)
    candidate.cache.foreach(PlayerRecruitmentCache.upsertRaw)
    // 4. Insert RecruitmentCandidate
    val outcomeStr = outcome.toString
    val reason = errorMessage
    sql"""INSERT INTO recruitment_candidate (run_id, username, evaluated_at, outcome, rejection_reason)
          VALUES ($runId, ${candidate.username}, $now, $outcomeStr, $reason)""".update.run()
  }.unit

  // --- TM stats helpers ---

  private def fetchTmStats(
      client: ChessComClient,
      username: Username,
      config: RecruitmentConfig,
      overallTimeoutPct: Double,
      now: Instant
    ): ZIO[Any, Throwable, (Int, Option[Double])] = {
    val needsTmStats = config.dailyMinTmGamesFinished.isDefined || config.dailyMaxTmTimeoutPercent.isDefined
    if (!needsTmStats) ZIO.succeed((0, None))
    else {
      // Fetch last ~90 days of archives
      val cutoff = now.minus(90, ChronoUnit.DAYS)
      val months = recentArchiveMonths(now, 90)

      for {
        archives <- ZIO.foreachPar(months) { ym =>
          client.getWithPermit[ApiPlayerArchive](
            ApiPlayerArchive.getUrl(username, ym.getYear, ym.getMonthValue)
          ).catchAll(_ => ZIO.succeed(ApiPlayerArchive(Chunk.empty)))
        }
        tmGames = archives.flatMap(_.games.filter(g => g.`match`.isDefined && g.endTime >= cutoff.getEpochSecond))
        tmGamesFinished = tmGames.size

        // TM timeout rate
        tmTimeoutPct =
          if (tmGamesFinished == 0) None
          else if (config.dailyMaxTmTimeoutPercent.isDefined && overallTimeoutPct == 0.0) Some(0.0)
          else {
            val timeouts = tmGames.count { g =>
              val isWhite = g.white.username.equalsIgnoreCase(username)
              val playerResult = if (isWhite) g.white.result else g.black.result
              playerResult == GameResultDetail.Timeout
            }
            Some(timeouts.toDouble / tmGamesFinished * 100.0)
          }
      } yield (tmGamesFinished, tmTimeoutPct)
    }
  }

  private def recentArchiveMonths(now: Instant, days: Int): List[YearMonth] = {
    val today = LocalDate.ofInstant(now, ZoneOffset.UTC)
    val cutoff = today.minusDays(days)
    val startMonth = YearMonth.from(cutoff)
    val endMonth = YearMonth.from(today)
    Iterator.iterate(startMonth)(_.plusMonths(1)).takeWhile(!_.isAfter(endMonth)).toList
  }

  // --- Report mode ---

  private[recruitment] def showReport(clubUrlName: ClubUrlName, runIdOpt: Option[String])
      : ZIO[Transactor, Throwable, Unit] =
    for {
      clubs <- Club.selectAll
      club <- ZIO.fromOption(clubs.find(_.urlName == clubUrlName))
        .orElseFail(ExternalException(s"Club '$clubUrlName' not found in database"))
      clubId = club.clubId
      run <- runIdOpt match {
        case Some(id) =>
          ZIO.attempt(id.toLong)
            .orElseFail(ExternalException(s"Invalid run ID: '$id' (expected a number)"))
            .flatMap(RecruitmentRun.selectId)
            .someOrFail(ExternalException(s"Run $id not found"))
        case None =>
          RecruitmentRun.selectLatest(clubId)
            .flatMap(ZIO.fromOption(_).orElseFail(ExternalException(s"No runs found for club '$clubUrlName'")))
      }
      invited <- RecruitmentCandidate.selectInvitedByRun(run.runId)
      _       <- Console.printLine(s"=== Recruitment Report for $clubUrlName (run ${run.runId}) ===").orDie
      _       <- Console.printLine(s"Started: ${run.startedAt}").orDie
      _       <- Console.printLine(s"Completed: ${run.completedAt.getOrElse("in progress")}").orDie
      _       <- Console.printLine(s"Invited: ${invited.size}").orDie
      _       <- ZIO.foreachDiscard(invited)(c => Console.printLine(s"  ${c.username}").orDie)
    } yield ()
}
