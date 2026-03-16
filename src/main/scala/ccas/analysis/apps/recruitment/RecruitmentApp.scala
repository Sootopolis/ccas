package ccas.analysis.apps.recruitment

import java.time.{Instant, LocalDate, YearMonth, ZoneOffset}
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.Transactor
import zio.{Chunk, Console, RIO, Ref, Scope, Task, UIO, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Client, URL}

import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.misc.enums.{GameResultDetail, PlayerStatusCategory}
import ccas.api.misc.subtypes.{ClubUrlName, PlayerId, Username}
import ccas.api.player.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.SqlZioTypes.withTransaction

object RecruitmentApp extends ZIOAppDefault {

  private val DefaultInviteCap = 30
  private val DefaultExploreConcurrency = 1

  // --- Explore mode types ---

  private sealed trait SourceDescriptor { val id: String }
  private case class ClubSource(clubUrlName: ClubUrlName) extends SourceDescriptor {
    val id: String = ClubUrlName.unwrap(clubUrlName)
  }
  private case class UsernameSource(id: String, usernames: Set[Username]) extends SourceDescriptor

  private[recruitment] case class SourceState(
      remaining: List[Username],
      evaluated: Int,
      rejected: Int,
      consecutiveRejects: Int
  )

  // Grim constants (server-side, not user-configurable)
  private val GrimConsecutiveRejects = 40
  private val GrimMinSample = 40
  private val GrimRejectRatio = 39.0 / 40.0

  private[recruitment] def isGrim(s: SourceState): Boolean =
    s.consecutiveRejects >= GrimConsecutiveRejects ||
    (s.evaluated >= GrimMinSample && s.rejected.toDouble / s.evaluated >= GrimRejectRatio)

  // --- ExploreContext: bundles constant parameters across explore loop recursion ---

  private case class ExploreContext(
      runId: Long,
      clubUrlName: ClubUrlName,
      filters: List[RecruitmentFilter],
      runCtx: RunContext,
      invitedRef: Ref[List[Username]],
      evaluatedRef: Ref[Set[Username]],
      inviteCap: Int,
      existingUsernames: Set[Username],
      exploreConcurrency: Int,
      explore: Boolean,
      showProgress: Boolean
  )

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- (args.toList match
        case "report" :: clubStr :: rest => showReport(ClubUrlName.wrap(clubStr), rest.headOption)
        case clubStr :: rest =>
          val configName  = rest.headOption.getOrElse("default")
          val sourceClubs = rest.drop(1).map(ClubUrlName.wrap)
          recruit(ClubUrlName.wrap(clubStr), configName, sourceClubs = sourceClubs,
            explore = sourceClubs.isEmpty, showProgress = true)
        case _ =>
          ZIO.fail(
            ExternalException(
              "Usage: RecruitmentApp <club-url-name> [config-name] [source-clubs...]\n       RecruitmentApp report <club-url-name> [run-id]"
            )
          )
      ).provide(
        ChessComClient.live(),
        Client.default,
        DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
      )
    } yield ()

  // --- Phase 1: Initialize ---

  def recruit(
      clubUrlName: ClubUrlName,
      configName: String,
      inviteCap: Int = DefaultInviteCap,
      sourceClubs: List[ClubUrlName] = Nil,
      timeLimitMinutes: Option[Int] = None,
      explore: Boolean = true,
      showProgress: Boolean = false
    ): RIO[ChessComClient & Transactor, RecruitmentRun] =
    for {
      _       <- MembershipApp.reconcile(clubUrlName)
      client  <- ZIO.service[ChessComClient]
      apiClub <- ApiClub.get(client, clubUrlName)
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), clubUrlName)
      _ <- Club.upsert(club)
      config <- RecruitmentConfig.select(clubId, configName)
        .someOrFail(ExternalException(s"No recruitment config '$configName' found for club '$clubUrlName'"))
      now = Instant.now()
      runId <- RecruitmentRun.insert(clubId, configName, now)

      // --- Shared setup ---
      targetMembers <- ApiClubMembers.get(client, clubUrlName)
      membersMap = targetMembers.toMap
      existingUsernames = membersMap.keySet
      targetMemberNames = membersMap.keys.toList

      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubUrlName))
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet

      formerMemberIds <- if (config.excludeFormerMembers)
        ClubMember.selectClubFormer(config.clubId).map(_.map(_.playerId).toSet)
      else ZIO.succeed(Set.empty[PlayerId])

      discoveredClubs     <- Ref.make(Set.empty[ClubUrlName])
      discoveredOpponents <- Ref.make(Set.empty[Username])
      invitedRef          <- Ref.make(List.empty[Username])
      evaluatedRef        <- Ref.make(Set.empty[Username])

      runCtx = RunContext(client, config, targetMatchIds, formerMemberIds, Instant.now(), discoveredClubs, discoveredOpponents)
      filters = buildFilterChain(config)
      effectiveConcurrency = DefaultExploreConcurrency

      ctx = ExploreContext(
        runId = runId,
        clubUrlName = clubUrlName,
        filters = filters,
        runCtx = runCtx,
        invitedRef = invitedRef,
        evaluatedRef = evaluatedRef,
        inviteCap = inviteCap,
        existingUsernames = existingUsernames,
        exploreConcurrency = effectiveConcurrency,
        explore = explore,
        showProgress = showProgress
      )

      transactor <- ZIO.service[Transactor]

      finalizeRun = (label: String) => for {
        _ <- ZIO.when(showProgress)(Console.printLine("").orDie)
        invited     <- invitedRef.get.map(_.reverse)
        evaluated   <- evaluatedRef.get
        completedAt  = Instant.now()
        finalRun     = RecruitmentRun(runId, clubId, configName, now, Some(completedAt), invited.size)
        _ <- RecruitmentRun.update(finalRun)
        _ <- Console.printLine(s"=== $label ===").orDie
        _ <- Console.printLine(s"Candidates evaluated: ${evaluated.size}").orDie
        _ <- Console.printLine(s"Invited: ${invited.size}").orDie
        _ <- ZIO.foreachDiscard(invited)(u => Console.printLine(s"  $u").orDie)
      } yield finalRun

      _ <- ZIO.when(showProgress)(
        Console.printLine("[Hint] Press Ctrl+C to stop gracefully (candidates found so far will be listed)").orDie
      )

      // --- Build initial sources from provided source clubs ---
      initialSources = sourceClubs.map(ClubSource(_))

      // --- Build static strategy list (only used when explore == true) ---
      staticStrategies: List[() => RIO[Transactor, List[SourceDescriptor]]] =
        if (!explore) Nil
        else List(
          () => discoverOwnMemberClubs(client, clubUrlName, targetMemberNames),
          () => discoverDbClubs(clubUrlName),
          () => discoverMatchOpponents(clubMatches)
        )

      // --- Run the explore loop (with optional timeout) ---
      loopEffect = exploreLoop(
        ctx = ctx,
        activePool = Map.empty,
        pendingSources = initialSources,
        staticStrategies = staticStrategies,
        visitedClubs = sourceClubs.toSet,
        roundRobinKeys = Nil
      )
      _ <- (timeLimitMinutes match {
        case Some(minutes) => loopEffect.timeout(zio.durationLong(minutes.toLong).minutes).unit
        case None          => loopEffect.unit
      }).onInterrupt(
        finalizeRun("Recruitment Interrupted")
          .provideEnvironment(ZEnvironment(transactor))
          .orDie
      )

      // --- Finalize ---
      finalRun <- finalizeRun("Recruitment Complete")
    } yield finalRun

  // --- Explore loop ---

  private def exploreLoop(
      ctx: ExploreContext,
      activePool: Map[String, SourceState],
      pendingSources: List[SourceDescriptor],
      staticStrategies: List[() => RIO[Transactor, List[SourceDescriptor]]],
      visitedClubs: Set[ClubUrlName],
      roundRobinKeys: List[String]
    ): RIO[Transactor, Unit] =
    for {
      invited <- ctx.invitedRef.get
      _ <- if (invited.size >= ctx.inviteCap) ZIO.unit
      else if (activePool.isEmpty && pendingSources.isEmpty) {
        // Try replenishment
        for {
          evaluated <- ctx.evaluatedRef.get
          replenished <- replenish(ctx, evaluated, visitedClubs, staticStrategies)
          (newSources, remainingStrategies) = replenished
          _ <- ZIO.unlessDiscard(newSources.isEmpty && remainingStrategies.isEmpty)(exploreLoop(
            ctx, activePool, newSources, remainingStrategies,
            visitedClubs, roundRobinKeys
          ))
        } yield ()
      }
      else {
        // Activate pending sources if pool has capacity
        for {
          evaluated <- ctx.evaluatedRef.get
          activationResult <- activateSources(
            ctx, activePool, pendingSources, evaluated, visitedClubs
          )
          (pool2, pending2, visited2) = activationResult
          _ <- if (pool2.isEmpty) {
            // Activation yielded empty sources, try replenishment
            for {
              replenished <- replenish(ctx, evaluated, visited2, staticStrategies)
              (newSources, remainingStrategies) = replenished
              _ <- ZIO.unlessDiscard(newSources.isEmpty && remainingStrategies.isEmpty)(exploreLoop(
                ctx, pool2, newSources, remainingStrategies,
                visited2, roundRobinKeys
              ))
            } yield ()
          }
          else {
            // Pick next candidate via round-robin
            val keys = if (roundRobinKeys.exists(pool2.contains)) roundRobinKeys.filter(pool2.contains)
                        else pool2.keys.toList
            for {
              picked <- keys match {
                case head :: tail => ZIO.succeed((head, tail :+ head))
                case Nil => ZIO.die(new IllegalStateException("pool2 non-empty but no keys"))
              }
              (sourceId, nextKeys) = picked
              sourceState = pool2(sourceId)
              _ <- sourceState.remaining match {
                case Nil =>
                  // Source exhausted, remove and recurse
                  val pool3 = pool2 - sourceId
                  exploreLoop(
                    ctx, pool3, pending2, staticStrategies,
                    visited2, nextKeys.filter(_ != sourceId)
                  )
                case username :: rest =>
                  for {
                    alreadyEvaluated <- ctx.evaluatedRef.get.map(_.contains(username))
                    pool3 = pool2.updated(sourceId, sourceState.copy(remaining = rest))
                    _ <- if (alreadyEvaluated) {
                      // Skip, recurse with updated remaining
                      exploreLoop(
                        ctx, pool3, pending2, staticStrategies,
                        visited2, nextKeys
                      )
                    } else {
                      // Pre-filter: daysSinceRejected (silently skip recently-rejected candidates)
                      val checkRejected = ctx.runCtx.config.daysSinceRejected match {
                        case None => ZIO.succeed(false)
                        case Some(days) =>
                          PlayerSnapshot.selectNameLatest(username).flatMap {
                            case None => ZIO.succeed(false)
                            case Some(snap) =>
                              RecruitmentCandidate.selectLatestRejectedByConfig(
                                snap.playerId, ctx.runCtx.config.clubId, ctx.runCtx.config.configName
                              ).map(_.exists(c => ChronoUnit.DAYS.between(c.evaluatedAt, ctx.runCtx.now) < days))
                          }
                      }
                      checkRejected.flatMap { recentlyRejected =>
                        if (recentlyRejected) {
                          ctx.evaluatedRef.update(_ + username) *>
                            exploreLoop(ctx, pool3, pending2, staticStrategies, visited2, nextKeys)
                        } else for {
                          result <- evaluateCandidate(ctx.runId, username, ctx.runCtx, ctx.filters)
                          _ <- ctx.evaluatedRef.update(_ + username)
                          isInvited = result == CandidateOutcome.Invited
                          isRejected = result == CandidateOutcome.Rejected || result == CandidateOutcome.Error
                          _ <- ZIO.when(isInvited)(ctx.invitedRef.update(username :: _))
                          _ <- printProgress(ctx)

                          updatedSource = pool3(sourceId).copy(
                            evaluated = sourceState.evaluated + 1,
                            rejected = sourceState.rejected + (if (isRejected) 1 else 0),
                            consecutiveRejects = if (isInvited) 0 else sourceState.consecutiveRejects + 1
                          )
                          pool4 <- if (isGrim(updatedSource)) {
                            Console.printLine(s"[Explore] Abandoning grim source: $sourceId (eval=${updatedSource.evaluated}, rej=${updatedSource.rejected})").orDie.as(pool3 - sourceId)
                          } else ZIO.succeed(pool3.updated(sourceId, updatedSource))
                          _ <- exploreLoop(
                            ctx, pool4, pending2, staticStrategies,
                            visited2, nextKeys
                          )
                        } yield ()
                      }
                    }
                  } yield ()
              }
            } yield ()
          }
        } yield ()
      }
    } yield ()

  private def printProgress(ctx: ExploreContext): UIO[ Unit] =
    ZIO.whenDiscard(ctx.showProgress)(for {
      invited   <- ctx.invitedRef.get
      evaluated <- ctx.evaluatedRef.get
      cap        = ctx.inviteCap
      pct        = if (cap == 0) 100 else (invited.size * 100) / cap
      filled     = pct / 5
      bar        = "\u2588" * filled + "\u2591" * (20 - filled)
      line       = s"\r[Progress] Evaluated: ${evaluated.size} | Invited: ${invited.size}/$cap | $bar $pct%"
      _         <- Console.print(line).orDie
    } yield ())

  // --- Source activation ---

  private def activateSources(
      ctx: ExploreContext,
      activePool: Map[String, SourceState],
      pendingSources: List[SourceDescriptor],
      evaluatedUsernames: Set[Username],
      visitedClubs: Set[ClubUrlName]
    ): RIO[Transactor, (Map[String, SourceState], List[SourceDescriptor], Set[ClubUrlName])] = {
    val slotsAvailable = ctx.exploreConcurrency - activePool.size
    if (slotsAvailable <= 0 || pendingSources.isEmpty)
      ZIO.succeed((activePool, pendingSources, visitedClubs))
    else {
      val (toActivate, remaining) = pendingSources.splitAt(slotsAvailable)
      for {
        results <- ZIO.foreachPar(toActivate) { source =>
          activateSource(ctx, source, evaluatedUsernames).map(members => (source, members))
        }
        (pool2, visited2) = results.foldLeft((activePool, visitedClubs)) { case ((pool, visited), (source, members)) =>
          val newVisited = source match {
            case ClubSource(name) => visited + name
            case _                => visited
          }
          if (members.isEmpty) (pool, newVisited)
          else (pool + (source.id -> SourceState(members, 0, 0, 0)), newVisited)
        }
      } yield (pool2, remaining, visited2)
    }
  }

  private def activateSource(
      ctx: ExploreContext,
      source: SourceDescriptor,
      evaluatedUsernames: Set[Username]
    ): RIO[Transactor, List[Username]] =
    source match {
      case ClubSource(clubUrlName) =>
        for {
          (members, adminUsernames) <-
            if (ctx.runCtx.config.excludeSourceAdmins)
              ApiClubMembers.get(ctx.runCtx.client, clubUrlName).map(_.toMap.keySet)
                .zipPar(ApiClub.get(ctx.runCtx.client, clubUrlName).map(extractAdminUsernames))
            else
              ApiClubMembers.get(ctx.runCtx.client, clubUrlName).map(m => (m.toMap.keySet, Set.empty[Username]))
          filtered = (members -- ctx.existingUsernames -- evaluatedUsernames -- adminUsernames).toList
          _ <- Console.printLine(s"[Explore] Activated club source: ${ClubUrlName.unwrap(clubUrlName)} (${filtered.size} candidates)").orDie
        } yield filtered
      case UsernameSource(id, usernames) =>
        val filtered = (usernames -- ctx.existingUsernames -- evaluatedUsernames).toList
        Console.printLine(s"[Explore] Activated username source: $id (${filtered.size} candidates)").orDie
          .as(filtered)
    }

  // --- Replenishment ---

  private def replenish(
      ctx: ExploreContext,
      evaluatedUsernames: Set[Username],
      visitedClubs: Set[ClubUrlName],
      staticStrategies: List[() => RIO[Transactor, List[SourceDescriptor]]]
    ): RIO[Transactor, (List[SourceDescriptor], List[() => RIO[Transactor, List[SourceDescriptor]]])] = {
    if (!ctx.explore)
      ZIO.succeed((Nil, staticStrategies))
    else for {
      // Dynamic strategy 1: candidate opponents
      opponents <- ctx.runCtx.discoveredOpponents.get
      newOpponents = opponents -- evaluatedUsernames
      result <- if (newOpponents.nonEmpty) {
        Console.printLine(s"[Explore] Discovered ${newOpponents.size} candidate opponents").orDie
          .as((List(UsernameSource("candidate-opponents", newOpponents)), staticStrategies))
      } else {
        // Dynamic strategy 2: candidate clubs
        for {
          clubs <- ctx.runCtx.discoveredClubs.get
          newClubs = clubs.filterNot(visitedClubs.contains).filterNot(_ == ctx.clubUrlName)
            .filterNot(ctx.runCtx.config.excludeClubNames.contains)
          result <- if (newClubs.nonEmpty) {
            Console.printLine(s"[Explore] Discovered ${newClubs.size} candidate clubs").orDie
              .as((newClubs.toList.map(ClubSource(_)), staticStrategies))
          } else {
            // Static strategies: try next one
            staticStrategies match {
              case Nil => ZIO.succeed((Nil, Nil))
              case head :: tail =>
                for {
                  sources <- head()
                  filtered = sources.filter {
                    case ClubSource(name) => !visitedClubs.contains(name) && name != ctx.clubUrlName &&
                      !ctx.runCtx.config.excludeClubNames.contains(name)
                    case _ => true
                  }
                  _ <- Console.printLine(s"[Explore] Static strategy yielded ${filtered.size} sources").orDie
                } yield (filtered, tail)
            }
          }
        } yield result
      }
    } yield result
  }

  // --- Discovery strategies ---

  private def discoverOwnMemberClubs(
      client: ChessComClient,
      clubUrlName: ClubUrlName,
      targetMemberNames: List[Username]
    ): RIO[Transactor, List[SourceDescriptor]] = {
    val sample = targetMemberNames.take(20)
    for {
      clubSets <- ZIO.foreachPar(sample) { username =>
        client.getWithPermit[ApiPlayerClubs](ApiPlayerClubs.getUrl(username))
          .map(_.clubs.map(_.clubName).toSet)
          .catchAll(_ => ZIO.succeed(Set.empty[ClubUrlName]))
      }
      allClubs = clubSets.foldLeft(Set.empty[ClubUrlName])(_ ++ _) - clubUrlName
      _ <- Console.printLine(s"[Explore] Own member clubs strategy found ${allClubs.size} clubs").orDie
    } yield allClubs.toList.map(ClubSource(_))
  }

  private def discoverDbClubs(
      clubUrlName: ClubUrlName
    ): RIO[Transactor, List[SourceDescriptor]] =
    for {
      clubs <- Club.selectAll
      filtered = clubs.map(_.urlName).filter(_ != clubUrlName)
      _ <- Console.printLine(s"[Explore] DB clubs strategy found ${filtered.size} clubs").orDie
    } yield filtered.map(ClubSource(_))

  private def discoverMatchOpponents(
      clubMatches: ApiClubMatches
    ): RIO[Transactor, List[SourceDescriptor]] = {
    val opponentUrls = clubMatches.finished.map(_.opponent) ++
      clubMatches.inProgress.map(_.opponent) ++
      clubMatches.registered.map(_.opponent)
    val opponentClubNames = opponentUrls.map(url => ClubUrlName.wrap(url.path.segments.last)).toSet
    Console.printLine(s"[Explore] Match opponents strategy found ${opponentClubNames.size} clubs").orDie
      .as(opponentClubNames.toList.map(ClubSource(_)))
  }

  // --- Helpers ---

  private def extractAdminUsernames(apiClub: ApiClub): Set[Username] =
    apiClub.admin.map(url => Username.wrap(url.path.segments.last)).toSet

  private def evaluateCandidate(
      runId: Long,
      username: Username,
      runCtx: RunContext,
      filters: List[RecruitmentFilter]
    ): RIO[Transactor, CandidateOutcome] = {
    val now = Instant.now()
    val candidateCtx = CandidateContext.initial(username)
    val env = FilterEnv(runCtx.copy(now = now), candidateCtx)
    (for {
      (outcome, finalCandidate) <- runFilters(env, filters)
      _ <- persistCandidateResults(runId, now, finalCandidate, outcome)
    } yield outcome).catchAll { error =>
      persistCandidateResults(
        runId, now, candidateCtx, CandidateOutcome.Error,
        Some(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
      ).as(CandidateOutcome.Error)
    }
  }

  // --- Filter pipeline types ---

  /** Shared across all candidates in a run. */
  private case class RunContext(
      client: ChessComClient,
      config: RecruitmentConfig,
      clubMatchIds: Set[URL],
      formerMemberIds: Set[PlayerId],
      now: Instant,
      discoveredClubs: Ref[Set[ClubUrlName]],
      discoveredOpponents: Ref[Set[Username]]
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
    def apply(env: FilterEnv): RIO[Transactor, FilterResult]
  }

  // --- Pipeline runner ---

  private def runFilters(env: FilterEnv, filters: List[RecruitmentFilter])
      : RIO[Transactor, (CandidateOutcome, CandidateContext)] =
    ZIO.foldLeft(filters)(FilterResult(None, env.candidate)) {
      case (r @ FilterResult(Some(_), _), _) => ZIO.succeed(r)
      case (FilterResult(None, ctx), filter) => filter(env.copy(candidate = ctx))
    }.map(r => (r.outcome.getOrElse(CandidateOutcome.Invited), r.candidate))

  // --- Filter chain builder ---

  private def buildFilterChain(config: RecruitmentConfig): List[RecruitmentFilter] = {
    val base = List(
      FetchAndCheckPlayer,
      CheckInvitedTooRecently,
      CheckBlacklist
    )
    val formerMember = Option.when(config.excludeFormerMembers)(CheckFormerMember)
    val rest = List(
      CheckCacheCriteria,
      CheckOpponentMatch,
      CheckClubs,
      CheckDailyStats,
      CheckOngoingGames
    )
    val tm = Option.when(
      config.dailyMinTmGamesFinished.isDefined || config.dailyMaxTmTimeoutPercent.isDefined
    )(CheckTmStats)
    base ++ formerMember ++ rest ++ tm
  }

  // --- Filter implementations ---

  private object CheckInvitedTooRecently extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        playerId <- ZIO.fromOption(env.candidate.apiPlayer.map(_.playerId))
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckInvitedTooRecently"))
        recentInvite <- RecruitmentCandidate.selectLatestInvited(playerId)
        tooRecent = env.run.config.daysSinceLastInvited.exists { days =>
          recentInvite.exists(c => ChronoUnit.DAYS.between(c.evaluatedAt, env.run.now) < days)
        }
      } yield FilterResult(Option.when(tooRecent)(CandidateOutcome.Rejected), env.candidate)
  }

  private object FetchAndCheckPlayer extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        apiPlayer <- env.run.client.getWithPermit[ApiPlayer](ApiPlayer.getUrl(env.candidate.username))
        existingPlayer <- Player.selectId(apiPlayer.playerId)
        statusCat = apiPlayer.status.category
        config = env.run.config
        now = env.run.now

        // Load existing cache
        cached <- PlayerRecruitmentCache.selectId(apiPlayer.playerId)

        updatedCtx = env.candidate.copy(
          apiPlayer = Some(apiPlayer),
          isNewPlayer = existingPlayer.isEmpty,
          cache = cached
        )

        outcome =
          if (statusCat != PlayerStatusCategory.Active) Some(CandidateOutcome.Rejected)
          else if (config.minDaysSinceRegistration.exists { days =>
            ChronoUnit.DAYS.between(Instant.ofEpochSecond(apiPlayer.joined), now) < days
          }) Some(CandidateOutcome.Rejected)
          else if (config.nationalityCountries.nonEmpty) {
            val countryCode = apiPlayer.country.path.segments.last
            val listed = config.nationalityCountries.contains(countryCode)
            if (config.nationalityExclude == listed) Some(CandidateOutcome.Rejected) else None
          }
          else None
      } yield FilterResult(outcome, updatedCtx)
  }

  private object CheckBlacklist extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        playerId <- ZIO.fromOption(env.candidate.apiPlayer.map(_.playerId))
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckBlacklist"))
        blacklisted <- RecruitmentBlacklist.isBlacklisted(env.run.config.clubId, playerId, env.run.now)
      } yield FilterResult(Option.when(blacklisted)(CandidateOutcome.Rejected), env.candidate)
  }

  private object CheckFormerMember extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        playerId <- ZIO.fromOption(env.candidate.apiPlayer.map(_.playerId))
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckFormerMember"))
        isFormer = env.run.formerMemberIds.contains(playerId)
      } yield FilterResult(Option.when(isFormer)(CandidateOutcome.Rejected), env.candidate)
  }

  // --- Per-criterion cache checks ---

  private case class CacheCriterion(
      stalenessHours: Long,
      check: (PlayerRecruitmentCache, RecruitmentConfig) => Option[CandidateOutcome]
  )

  private val cacheCriteria: List[CacheCriterion] = List(
    // Zero-tolerance daily timeout (unlimited staleness)
    CacheCriterion(
      Long.MaxValue,
      (cache, config) =>
        Option.when(
          config.dailyMaxTimeoutPercent.contains(0.0) && cache.lastDailyTimeoutAt.isDefined
        )(CandidateOutcome.Rejected)
    ),
    // Zero-tolerance TM timeout (unlimited staleness)
    CacheCriterion(
      Long.MaxValue,
      (cache, config) =>
        Option.when(
          config.dailyMaxTmTimeoutPercent.contains(0.0) && cache.lastTmTimeoutAt.isDefined
        )(CandidateOutcome.Rejected)
    ),
    // Max clubs (72h)
    CacheCriterion(
      72L,
      (cache, config) =>
        Option.when(
          config.maxClubs.exists(max => cache.clubCount.exists(_ > max))
        )(CandidateOutcome.Rejected)
    ),
    // Min daily ELO (24h)
    CacheCriterion(
      24L,
      (cache, config) =>
        Option.when(
          config.dailyMinElo.exists(min => cache.dailyElo.exists(_ < min))
        )(CandidateOutcome.Rejected)
    ),
    // Max daily ELO (24h)
    CacheCriterion(
      24L,
      (cache, config) =>
        Option.when(
          config.dailyMaxElo.exists(max => cache.dailyElo.exists(_ > max))
        )(CandidateOutcome.Rejected)
    ),
    // Max daily timeout % (24h)
    CacheCriterion(
      24L,
      (cache, config) =>
        Option.when(
          config.dailyMaxTimeoutPercent.exists(max => cache.dailyTimeoutPct.exists(_ > max))
        )(CandidateOutcome.Rejected)
    ),
    // Min daily games finished (24h)
    CacheCriterion(
      24L,
      (cache, config) =>
        Option.when(
          config.dailyMinGamesFinished.exists(min => cache.dailyGamesFinished.exists(_ < min))
        )(CandidateOutcome.Rejected)
    ),
    // Min ongoing games (4h)
    CacheCriterion(
      4L,
      (cache, config) =>
        Option.when(
          config.dailyMinOngoingGames.exists(min => cache.ongoingGames.exists(_ < min))
        )(CandidateOutcome.Rejected)
    ),
    // Max ongoing games (4h)
    CacheCriterion(
      4L,
      (cache, config) =>
        Option.when(
          config.dailyMaxOngoingGames.exists(max => cache.ongoingGames.exists(_ > max))
        )(CandidateOutcome.Rejected)
    ),
    // Min ongoing team matches (4h)
    CacheCriterion(
      4L,
      (cache, config) =>
        Option.when(
          config.dailyMinOngoingTeamMatches.exists(min => cache.ongoingTeamMatches.exists(_ < min))
        )(CandidateOutcome.Rejected)
    ),
    // Min TM games finished 90d (24h)
    CacheCriterion(
      24L,
      (cache, config) =>
        Option.when(
          config.dailyMinTmGamesFinished.exists(min => cache.tmGamesFinished90d.exists(_ < min))
        )(CandidateOutcome.Rejected)
    ),
    // Max TM timeout % 90d (24h)
    CacheCriterion(
      24L,
      (cache, config) =>
        Option.when(
          config.dailyMaxTmTimeoutPercent.exists(max => cache.tmTimeoutPct90d.exists(_ > max))
        )(CandidateOutcome.Rejected)
    )
  )

  private def runCacheCriteria(
      cache: PlayerRecruitmentCache,
      config: RecruitmentConfig,
      now: Instant
    ): Option[CandidateOutcome] = {
    val ageHours = ChronoUnit.HOURS.between(cache.fetchedAt, now)
    cacheCriteria.view
      .filter(_.stalenessHours > ageHours)
      .flatMap(_.check(cache, config))
      .headOption
  }

  private object CheckCacheCriteria extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      ZIO.succeed {
        val outcome = env.candidate.cache.flatMap(runCacheCriteria(_, env.run.config, env.run.now))
        FilterResult(outcome, env.candidate)
      }
  }

  private object CheckOpponentMatch extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        playerMatches <- env.run.client.getWithPermit[ApiPlayerMatches](
          ApiPlayerMatches.getUrl(env.candidate.username)
        )
        playerRegisteredIds = playerMatches.registered.map(_.`@id`).toSet ++
          playerMatches.inProgress.map(_.`@id`).toSet
        hasOpponentMatch = playerRegisteredIds.exists(env.run.clubMatchIds.contains)
      } yield FilterResult(Option.when(hasOpponentMatch)(CandidateOutcome.Rejected), env.candidate)
  }

  private object CheckClubs extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        playerClubs <- env.run.client.getWithPermit[ApiPlayerClubs](
          ApiPlayerClubs.getUrl(env.candidate.username)
        )
        clubCount = playerClubs.clubs.size
        clubNames = playerClubs.clubs.map(_.clubName).toSet
        config = env.run.config

        // Update cache with clubCount
        apiPlayer <- ZIO.fromOption(env.candidate.apiPlayer)
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckClubs"))
        updatedCache = env.candidate.cache match {
          case Some(c) => c.copy(clubCount = Some(clubCount))
          case None    => PlayerRecruitmentCache.empty(apiPlayer.playerId, env.run.now, Some(clubCount))
        }
        updatedCtx = env.candidate.copy(clubCount = Some(clubCount), cache = Some(updatedCache))

        outcome =
          if (config.maxClubs.exists(clubCount > _)) Some(CandidateOutcome.Rejected)
          else if (config.excludeClubNames.exists(clubNames.contains)) Some(CandidateOutcome.Rejected)
          else None

        // Harvest clubs for explore mode (all candidates that reach this filter)
        _ <- env.run.discoveredClubs.update(_ ++ clubNames)
      } yield FilterResult(outcome, updatedCtx)
  }

  private object CheckDailyStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        playerStats <- env.run.client.getWithPermit[ApiPlayerStats](
          ApiPlayerStats.getUrl(env.candidate.username)
        )
        result <- playerStats.chessDaily match {
          case None => ZIO.succeed(FilterResult(Some(CandidateOutcome.Rejected), env.candidate))
          case Some(dailyStats) => applyDailyStats(env, dailyStats)
        }
      } yield result

    private def applyDailyStats(env: FilterEnv, dailyStats: ApiPlayerStats.ApiPlayerDailyStats)
        : RIO[Transactor, FilterResult] = {
      val dailyElo = dailyStats.last.rating
      val dailyTimeoutPct = dailyStats.record.timeoutPercent
      val dailyGamesFinished = dailyStats.record.nGames
      for {
        // Fetch current month archive to find most recent daily timeout
        lastDailyTimeoutAt <- findLastDailyTimeout(env.run.client, env.candidate.username, env.run.now)

        // Merge with existing cache's lastDailyTimeoutAt (keep more recent)
        mergedDailyTimeout = mergeOptionalInstants(
          lastDailyTimeoutAt,
          env.candidate.cache.flatMap(_.lastDailyTimeoutAt)
        )

        dailyTimePerMove = dailyStats.record.timePerMove
        config = env.run.config
        outcome =
          if (config.dailyMinElo.exists(dailyElo < _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMaxElo.exists(dailyElo > _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMaxTimeoutPercent.exists(dailyTimeoutPct > _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMinGamesFinished.exists(dailyGamesFinished < _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMaxHoursPerMove.exists(maxHours => dailyTimePerMove > maxHours * 3600)) Some(CandidateOutcome.Rejected)
          else None

        // Build/update cache with daily stats
        apiPlayer <- ZIO.fromOption(env.candidate.apiPlayer)
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckDailyStats"))
        existingCache = env.candidate.cache
        updatedCache = existingCache match {
          case Some(c) => c.copy(
            fetchedAt = env.run.now,
            dailyElo = Some(dailyElo),
            dailyTimeoutPct = Some(dailyTimeoutPct),
            dailyGamesFinished = Some(dailyGamesFinished),
            clubCount = env.candidate.clubCount.orElse(c.clubCount),
            lastDailyTimeoutAt = mergedDailyTimeout
          )
          case None => PlayerRecruitmentCache.empty(apiPlayer.playerId, env.run.now, env.candidate.clubCount).copy(
            dailyElo = Some(dailyElo),
            dailyTimeoutPct = Some(dailyTimeoutPct),
            dailyGamesFinished = Some(dailyGamesFinished),
            lastDailyTimeoutAt = mergedDailyTimeout
          )
        }
        updatedCtx = env.candidate.copy(cache = Some(updatedCache))
      } yield FilterResult(outcome, updatedCtx)
    }
  }

  private object CheckOngoingGames extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        currentGames <- env.run.client.getWithPermit[ApiPlayerGamesCurrent](
          ApiPlayerGamesCurrent.getUrl(env.candidate.username)
        )
        ongoingGames = currentGames.games.size
        ongoingTeamMatches = currentGames.games.count(_.`match`.isDefined)

        config = env.run.config
        outcome =
          if (config.dailyMinOngoingGames.exists(ongoingGames < _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMaxOngoingGames.exists(ongoingGames > _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMinOngoingTeamMatches.exists(ongoingTeamMatches < _)) Some(CandidateOutcome.Rejected)
          else None

        // Update cache with ongoing fields
        existingCache = env.candidate.cache
        apiPlayer <- ZIO.fromOption(env.candidate.apiPlayer)
          .orElseFail(new NoSuchElementException("apiPlayer not set — FetchAndCheckPlayer must run before CheckOngoingGames"))
        updatedCache = existingCache match {
          case Some(c) => c.copy(
            ongoingGames = Some(ongoingGames),
            ongoingTeamMatches = Some(ongoingTeamMatches)
          )
          case None => PlayerRecruitmentCache.empty(apiPlayer.playerId, env.run.now, env.candidate.clubCount).copy(
            ongoingGames = Some(ongoingGames),
            ongoingTeamMatches = Some(ongoingTeamMatches)
          )
        }
        updatedCtx = env.candidate.copy(cache = Some(updatedCache))
      } yield FilterResult(outcome, updatedCtx)
  }

  private object CheckTmStats extends RecruitmentFilter {
    def apply(env: FilterEnv): RIO[Transactor, FilterResult] =
      for {
        cache <- ZIO.fromOption(env.candidate.cache)
          .orElseFail(new NoSuchElementException("cache not set — CheckDailyStats must run before CheckTmStats"))
        tmResult <- fetchTmStats(
          env.run.client,
          env.candidate.username,
          env.run.config,
          cache.dailyTimeoutPct.getOrElse(0.0),
          env.run.now
        )
        (tmGamesFinished, tmTimeoutPct, lastTmTimeoutAt, opponentUsernames) = tmResult

        // Harvest opponents for explore mode
        _ <- env.run.discoveredOpponents.update(_ ++ opponentUsernames)

        // Merge with existing cache's lastTmTimeoutAt (keep more recent)
        mergedTmTimeout = mergeOptionalInstants(lastTmTimeoutAt, cache.lastTmTimeoutAt)

        updatedCache = cache.copy(
          tmGamesFinished90d = Some(tmGamesFinished),
          tmTimeoutPct90d = tmTimeoutPct,
          lastTmTimeoutAt = mergedTmTimeout
        )
        updatedCtx = env.candidate.copy(cache = Some(updatedCache))
        config = env.run.config
        outcome =
          if (config.dailyMinTmGamesFinished.exists(tmGamesFinished < _)) Some(CandidateOutcome.Rejected)
          else if (config.dailyMaxTmTimeoutPercent.exists(max => tmTimeoutPct.exists(_ > max))) Some(CandidateOutcome.Rejected)
          else None
      } yield FilterResult(outcome, updatedCtx)
  }

  // --- Deferred DB writes ---

  private def persistCandidateResults(
      runId: Long,
      now: Instant,
      candidate: CandidateContext,
      outcome: CandidateOutcome,
      errorMessage: Option[String] = None
    ): RIO[Transactor, Unit] =
    candidate.apiPlayer match {
      case None => ZIO.unit // No player data (transient API error) — skip persistence, retry next run
      case Some(ap) =>
        withTransaction {
          for {
            _ <- ZIO.when(candidate.isNewPlayer)(
                   Player.insert(Player(ap.playerId, Instant.ofEpochSecond(ap.joined)))
                 )
            _ <- PlayerSnapshot.insert(PlayerSnapshot(ap.playerId, now, candidate.username, ap.status.category, ap.title))
            _ <- ZIO.foreachDiscard(candidate.cache)(PlayerRecruitmentCache.upsert)
            _ <- RecruitmentCandidate.insert(RecruitmentCandidate(runId, ap.playerId, now, outcome, errorMessage))
          } yield ()
        }
    }

  // --- TM stats helpers ---

  private def fetchTmStats(
      client: ChessComClient,
      username: Username,
      config: RecruitmentConfig,
      overallTimeoutPct: Double,
      now: Instant
    ): Task[ (Int, Option[Double], Option[Instant], Set[Username])] = {
    val needsTmStats = config.dailyMinTmGamesFinished.isDefined || config.dailyMaxTmTimeoutPercent.isDefined
    if (!needsTmStats) ZIO.succeed((0, None, None, Set.empty))
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
            val timeouts = tmGames.count(g => playerResult(g, username) == GameResultDetail.Timeout)
            Some(timeouts.toDouble / tmGamesFinished * 100.0)
          }

        // Find most recent TM game with Timeout result
        lastTmTimeoutAt = tmGames
          .filter(g => playerResult(g, username) == GameResultDetail.Timeout)
          .sortBy(_.endTime)(using Ordering[Long].reverse)
          .headOption
          .map(g => Instant.ofEpochSecond(g.endTime))

        // Harvest opponents who didn't lose by timeout
        opponentUsernames = tmGames.flatMap { g =>
          val isWhite = g.white.username.equalsIgnoreCase(username)
          val opponentResult = if (isWhite) g.black.result else g.white.result
          val opponentName = if (isWhite) g.black.username else g.white.username
          Option.when(opponentResult != GameResultDetail.Timeout)(Username.wrap(opponentName))
        }.toSet
      } yield (tmGamesFinished, tmTimeoutPct, lastTmTimeoutAt, opponentUsernames)
    }
  }

  private def findLastDailyTimeout(
      client: ChessComClient,
      username: Username,
      now: Instant
    ): Task[ Option[Instant]] = {
    val currentMonth = YearMonth.from(LocalDate.ofInstant(now, ZoneOffset.UTC))
    client.getWithPermit[ApiPlayerArchive](
      ApiPlayerArchive.getUrl(username, currentMonth.getYear, currentMonth.getMonthValue)
    ).map { archive =>
      archive.games
        .filter(g => g.`match`.isEmpty) // non-match daily games
        .filter(g => playerResult(g, username) == GameResultDetail.Timeout)
        .sortBy(_.endTime)(using Ordering[Long].reverse)
        .headOption
        .map(g => Instant.ofEpochSecond(g.endTime))
    }.catchAll(_ => ZIO.succeed(None))
  }

  private def playerResult(g: ApiPlayerArchive.ApiPlayerArchiveGame, username: Username): GameResultDetail =
    if (g.white.username.equalsIgnoreCase(username)) g.white.result else g.black.result

  private def mergeOptionalInstants(a: Option[Instant], b: Option[Instant]): Option[Instant] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(if (x.isAfter(y)) x else y)
      case (Some(x), None)    => Some(x)
      case (None, Some(y))    => Some(y)
      case (None, None)       => None
    }

  private def recentArchiveMonths(now: Instant, days: Int): List[YearMonth] = {
    val today = LocalDate.ofInstant(now, ZoneOffset.UTC)
    val cutoff = today.minusDays(days)
    val startMonth = YearMonth.from(cutoff)
    val endMonth = YearMonth.from(today)
    Iterator.iterate(startMonth)(_.plusMonths(1)).takeWhile(!_.isAfter(endMonth)).toList
  }

  // --- Test-facing helpers (preserve original API for existing tests) ---

  private[recruitment] def gatherCandidates(
      client: ChessComClient,
      clubUrlName: ClubUrlName,
      config: RecruitmentConfig,
      sourceClubs: List[ClubUrlName]
    ): RIO[Transactor, List[Username]] =
    for {
      targetMembers <- ApiClubMembers.get(client, clubUrlName)
      existingUsernames = targetMembers.toMap.keySet
      sourceMembers <- ZIO.foreachPar(sourceClubs) { sourceClubName =>
        ApiClubMembers.get(client, sourceClubName).map(_.toMap.keySet)
      }
      allSourceUsernames = sourceMembers.foldLeft(Set.empty[Username])(_ ++ _)
      candidatesBeforeAdminFilter = allSourceUsernames -- existingUsernames
      adminUsernames <- if (config.excludeSourceAdmins) {
        ZIO.foreachPar(sourceClubs) { sourceClubName =>
          ApiClub.get(client, sourceClubName).map(extractAdminUsernames)
        }.map(_.foldLeft(Set.empty[Username])(_ ++ _))
      } else ZIO.succeed(Set.empty[Username])
    } yield (candidatesBeforeAdminFilter -- adminUsernames).toList

  private[recruitment] def evaluateCandidates(
      client: ChessComClient,
      runId: Long,
      clubUrlName: ClubUrlName,
      candidates: List[Username],
      config: RecruitmentConfig,
      inviteCap: Int = DefaultInviteCap
    ): RIO[Transactor, List[Username]] =
    for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubUrlName))
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet
      formerMemberIds <- if (config.excludeFormerMembers)
        ClubMember.selectClubFormer(config.clubId).map(_.map(_.playerId).toSet)
      else ZIO.succeed(Set.empty[PlayerId])
      discoveredClubs     <- Ref.make(Set.empty[ClubUrlName])
      discoveredOpponents <- Ref.make(Set.empty[Username])
      runCtx  = RunContext(client, config, targetMatchIds, formerMemberIds, Instant.now(), discoveredClubs, discoveredOpponents)
      filters = buildFilterChain(config)
      revInvited <- ZIO.foldLeft(candidates)(List.empty[Username]) { case (invited, username) =>
        if (invited.size >= inviteCap) ZIO.succeed(invited)
        else evaluateCandidate(runId, username, runCtx, filters).map { outcome =>
          if (outcome == CandidateOutcome.Invited) username :: invited else invited
        }
      }
    } yield revInvited.reverse

  // --- Report mode ---

  def showReport(clubUrlName: ClubUrlName, runIdOpt: Option[String])
      : RIO[Transactor, Unit] =
    for {
      club <- Club.selectByUrlName(clubUrlName)
        .someOrFail(ExternalException(s"Club '$clubUrlName' not found in database"))
      clubId = club.clubId
      run <- runIdOpt match {
        case Some(id) =>
          ZIO.attempt(id.toLong)
            .orElseFail(ExternalException(s"Invalid run ID: '$id' (expected a number)"))
            .flatMap(RecruitmentRun.selectId)
            .someOrFail(ExternalException(s"Run $id not found"))
        case None =>
          RecruitmentRun.selectLatest(clubId)
            .someOrFail(ExternalException(s"No runs found for club '$clubUrlName'"))
      }
      invited <- RecruitmentCandidate.selectInvitedByRun(run.runId)
      _       <- Console.printLine(s"=== Recruitment Report for $clubUrlName (run ${run.runId}) ===").orDie
      _       <- Console.printLine(s"Started: ${run.startedAt}").orDie
      _       <- Console.printLine(s"Completed: ${run.completedAt.getOrElse("in progress")}").orDie
      _       <- Console.printLine(s"Invited: ${invited.size}").orDie
      _       <- ZIO.foreachDiscard(invited) { c =>
                   PlayerSnapshot.selectIdLatest(c.playerId).map(_.map(_.username).getOrElse(Username.wrap(s"[pid=${c.playerId}]")))
                     .flatMap(name => Console.printLine(s"  $name").orDie)
                 }
    } yield ()
}
