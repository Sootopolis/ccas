package ccas.analysis.apps.recruitment

import java.time.{Duration as JDuration, Instant}

import zio.{Clock, RIO, Ref, Scope, Task, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.apps.withClubSlugRenameRecovery
import ccas.analysis.tables.*
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.{display, OutputFile, ProgressDisplay}
import ccas.utils.client.{ChessComClient, HttpClientLayer}
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

object RecruitmentApp extends ZIOAppDefault {

  private val DefaultTarget             = 30
  private val DefaultExploreConcurrency = 1
  private val DefaultEvalChunkSize      = 32 // candidates per checkpoint (source-switch interval)

  private val help =
    """Usage: RecruitmentApp <club-slug> [alias] [source-clubs...] [--target N] [--cumulative] [--focus]
      |       RecruitmentApp report <club-slug> [run-id]
      |
      |  --target N      Number of candidates to find (default: 30)
      |  --cumulative    Count today's earlier finds toward the target
      |  --focus         Only recruit from the given source clubs (no exploration)""".stripMargin

  /** Parsed CLI arguments for the recruit command. */
  private[recruitment] case class RecruitArgs(
    alias: String,
    sourceClubs: List[ClubSlug],
    target: Option[Int],
    cumulative: Boolean,
    focus: Boolean
  )

  /** Parses the arguments after the club-slug token.
    *
    * Positional args (alias, source clubs) must appear before any flags. Flags start at the first `--`-prefixed token;
    * `--target` consumes the next token as its value.
    */
  private[recruitment] def parseRecruitArgs(rest: List[String]): RecruitArgs = {
    val flagStart                = rest.indexWhere(_.startsWith("--"))
    val (positional, flagTokens) = if (flagStart < 0) (rest, Nil) else rest.splitAt(flagStart)
    val target = flagTokens.indexOf("--target") match {
      case i if i >= 0 && i + 1 < flagTokens.size => flagTokens(i + 1).toIntOption
      case _                                      => None
    }
    RecruitArgs(
      alias = positional.headOption.getOrElse("default"),
      sourceClubs = positional.drop(1).map(ClubSlug.wrap),
      target = target,
      cumulative = flagTokens.contains("--cumulative"),
      focus = flagTokens.contains("--focus")
    )
  }

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- (args.toList match {
        case "report" :: clubStr :: rest =>
          val clubSlug = ClubSlug.wrap(clubStr)
          for {
            report <- showReport(clubSlug, rest.headOption)
            now    <- Clock.instant
            completedAt = report.run.completedAt.getOrElse(now)
            _ <- OutputFile.writeAndLog(
              "recruitment",
              clubSlug,
              formatRecruitmentOutput(report.usernames, report.evaluatedCount, report.run.startedAt, completedAt)
            )
          } yield ()
        case clubStr :: rest =>
          val parsed   = parseRecruitArgs(rest)
          val clubSlug = ClubSlug.wrap(clubStr)
          recruit(
            clubSlug,
            parsed.alias,
            target = parsed.target,
            cumulative = parsed.cumulative,
            sourceClubs = parsed.sourceClubs,
            explore = parsed.sourceClubs.isEmpty || !parsed.focus,
            showHints = true
          ).flatMap { run =>
            for {
              candidates <-
                if (parsed.cumulative) RecruitmentCandidate.selectInvitedToday(run.clubId, parsed.alias)
                else RecruitmentCandidate.selectInvitedByRun(run.runId)
              resolvedMap <- Player.resolveUsernames(candidates.map(_.playerId))
              usernames = candidates.map(c => resolvedMap.getOrElse(c.playerId, Username.wrap(s"[pid=${c.playerId}]")))
              evaluatedCount <- RecruitmentCandidate.selectCountByRun(run.runId)
              now            <- Clock.instant
              output = formatRecruitmentOutput(
                usernames,
                evaluatedCount,
                run.startedAt,
                run.completedAt.getOrElse(now)
              )
              _ <- OutputFile.writeAndLog("recruitment", clubSlug, output)
            } yield ()
          }
        case _ => ZIO.fail(BadRequestException(help))
      }).provideSome[Scope](
        ProgressDisplay.live(showProgress = true),
        ChessComClient.live("recruitment"),
        HttpClientLayer.live,
        PostgresClient.live(onInit = Tables.ensureTables)
      )
    } yield ()

  // --- Phase 1: Initialize ---

  def recruit(
    clubSlug: ClubSlug,
    alias: String,
    target: Option[Int] = None,
    cumulative: Boolean = false,
    sourceClubs: List[ClubSlug] = Nil,
    timeLimitMinutes: Option[Int] = None,
    explore: Boolean = true,
    showHints: Boolean = false,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunId: Option[JobRunId] = None
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, RecruitmentRun] = ZIO.scoped {
    for {
      _      <- MembershipApp.reconcile(clubSlug, trackRun = false)
      client <- ZIO.service[ChessComClient]
      // The club fetch is the FIRST 404-prone hit in the run. With no clubIdHint, the resolver derives the hint from
      // the `club` table (deriveHint) — matching how MembershipApp's earlier reconcile resolves the slug.
      apiClub <- ApiClub.get(client, clubSlug)
        .withClubSlugRenameRecovery(client, clubSlug, clubIdHint = None)(fresh => ApiClub.get(client, fresh))
      clubId = apiClub.clubId
      // Read the canonical slug from the API response — recovery may have rewritten it. On the happy path
      // (no recovery) this is just the input slug echoed back, and the local upsert below is the source of truth.
      // On the recovery path the resolver already upserted under the canonical slug, so the local upsert becomes
      // an idempotent reaffirmation.
      effectiveSlug = ClubSlug.wrap(apiClub.`@id`.path.segments.last)
      club          = Club.fromApi(apiClub, effectiveSlug)
      _ <- Club.upsertResolvingSlugConflict(club, client)
      aliasRow <- RecruitmentAlias.selectLatest(clubId, alias)
        .someOrFail(NotFoundException(s"No recruitment alias '$alias' found for club '$clubSlug'"))
      criteria <- RecruitmentCriteria.selectId(aliasRow.criteriaId)
        .someOrFail(IllegalStateException(s"Criteria ${aliasRow.criteriaId} referenced by alias '$alias' not found"))
      resolvedTarget = target.getOrElse(DefaultTarget)
      alreadyFound <-
        if (cumulative) RecruitmentRun.sumCandidatesFoundToday(clubId, alias)
        else ZIO.succeed(0)
      effectiveTarget = (resolvedTarget - alreadyFound) max 0
      now <- Clock.instant
      runId <- RecruitmentRun.insert(clubId, criteria.criteriaId, trigger, now, jobRunId)

      // --- Shared setup ---
      targetMembers <- ApiClubMembers.get(client, effectiveSlug)
        .withClubSlugRenameRecovery(client, effectiveSlug, Some(clubId))(fresh => ApiClubMembers.get(client, fresh))
      existingUsernames = targetMembers.toMap.keySet

      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(effectiveSlug))
        .withClubSlugRenameRecovery(client, effectiveSlug, Some(clubId))(fresh =>
          client.get[ApiClubMatches](ApiClubMatches.getUrl(fresh))
        )
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet
      _ <- writeClubMatchRef(client, clubId, effectiveSlug, clubMatches).ignore

      formerMemberIds <-
        if (criteria.excludeFormerMembers)
          ClubMember.selectClubFormer(clubId).map(_.map(_.playerId).toSet)
        else ZIO.succeed(Set.empty[PlayerId])

      adminExcludedPlayerIds <-
        criteria.avoidAdminMinClubSize.fold(ZIO.succeed(Set.empty[PlayerId]))(
          ClubAdmin.selectPlayerIdsForSizableClubs
        )

      excludedSlugs       <- ZIO.foreach(criteria.excludeClubs)(Club.selectId).map(_.flatten.map(_.slug).toSet)
      discoveredOpponents <- Ref.make(Set.empty[Username])
      failedAdminSlugs    <- Ref.make(Set.empty[ClubSlug])
      invitedRef          <- Ref.make(List.empty[Username])
      evaluatedRef        <- Ref.make(Set.empty[Username])
      evalCountRef        <- Ref.make(0)

      runCtx = RunContext(
        client = client,
        criteria = criteria,
        clubId = clubId,
        alias = alias,
        clubMatchIds = targetMatchIds,
        formerMemberIds = formerMemberIds,
        adminExcludedPlayerIds = adminExcludedPlayerIds,
        excludedSlugs = excludedSlugs,
        now = now,
        discoveredOpponents = discoveredOpponents,
        failedAdminSlugs = failedAdminSlugs
      )
      filters              = RecruitmentFilters.buildFilterChain(criteria)
      effectiveConcurrency = DefaultExploreConcurrency

      progressBar <- ProgressDisplay.progressBar
      ctx = ExploreContext(
        runId = runId,
        clubSlug = effectiveSlug,
        filters = filters,
        runCtx = runCtx,
        invitedRef = invitedRef,
        evaluatedRef = evaluatedRef,
        evalCountRef = evalCountRef,
        target = effectiveTarget,
        existingUsernames = existingUsernames,
        exploreConcurrency = effectiveConcurrency,
        evalChunkSize = DefaultEvalChunkSize,
        explore = explore,
        showHints = showHints,
        progressBar = progressBar
      )

      pgClient <- ZIO.service[PostgresClient]

      _ <- ZIO.whenDiscard(cumulative && alreadyFound > 0)(
        ZIO.logInfo(s"[Cumulative] Already found $alreadyFound today, effective target: $effectiveTarget")
      )

      finalRun <-
        if (effectiveTarget == 0) {
          ZIO.logInfo("[Cumulative] Target already met, skipping explore") *>
            finalizeRun(
              ctx,
              trigger,
              now,
              cumulative,
              alreadyFound,
              "Recruitment Complete (target already met)",
              jobRunId = jobRunId
            )
        } else {
          runExplorePhase(
            ctx,
            client,
            pgClient,
            sourceClubs,
            timeLimitMinutes,
            trigger,
            now,
            cumulative,
            alreadyFound,
            jobRunId
          )
        }
    } yield finalRun
  }

  private def runExplorePhase(
    ctx: ExploreContext,
    client: ChessComClient,
    pgClient: PostgresClient,
    sourceClubs: List[ClubSlug],
    timeLimitMinutes: Option[Int],
    trigger: RunTrigger,
    startedAt: Instant,
    cumulative: Boolean,
    alreadyFound: Int,
    jobRunId: Option[JobRunId]
  ): RIO[ChessComClient & PostgresClient, RecruitmentRun] =
    for {
      _ <- ZIO.whenDiscard(ctx.showHints)(
        ZIO.logInfo("[Hint] Press Ctrl+C to stop gracefully (candidates found so far will be listed)")
      )

      // --- Load deferred candidates from prior runs as a priority source ---
      deferredCandidates  <- RecruitmentCandidate.selectDeferredByClub(ctx.runCtx.clubId)
      deferredResolvedMap <- Player.resolveUsernames(deferredCandidates.map(_.playerId))
      deferredUsernames = deferredResolvedMap.values.filterNot(ctx.existingUsernames).toList.distinct
      _ <- ZIO.whenDiscard(deferredUsernames.nonEmpty)(
        ZIO.logInfo(s"[Deferred] Found ${deferredUsernames.size} deferred candidates from prior runs")
      )

      // --- Build initial sources from provided source clubs ---
      deferredSource = Option.when(deferredUsernames.nonEmpty)(
        UsernameSource("deferred-priority", deferredUsernames)
      )
      initialSources = deferredSource.toList ++ sourceClubs.map(ClubSource(_))

      // --- Build static strategy list (only used when explore == true) ---
      staticStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]] =
        if (!ctx.explore) Nil
        else
          List(
            RecruitmentExplore.discoverCandidateOpponents(client, startedAt),
            RecruitmentExplore.discoverMatchBoardOpponents(ctx.runCtx.clubId)
          )

      // --- Run the explore loop (with optional timeout) ---
      loopEffect = RecruitmentExplore.exploreLoop(
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
        finalizeRun(
          ctx,
          trigger,
          startedAt,
          cumulative,
          alreadyFound,
          "Recruitment Interrupted",
          interrupted = true,
          jobRunId = jobRunId
        )
          .provideEnvironment(ZEnvironment(pgClient))
          .orDie
      )

      // --- Finalize ---
      finalRun <- finalizeRun(
        ctx,
        trigger,
        startedAt,
        cumulative,
        alreadyFound,
        "Recruitment Complete",
        jobRunId = jobRunId
      )
    } yield finalRun

  private def finalizeRun(
    ctx: ExploreContext,
    trigger: RunTrigger,
    startedAt: Instant,
    cumulative: Boolean,
    alreadyFound: Int,
    label: String,
    interrupted: Boolean = false,
    jobRunId: Option[JobRunId]
  ): RIO[PostgresClient, RecruitmentRun] =
    for {
      _     <- ctx.progressBar.finish
      _     <- RecruitmentExplore.reclassifyExcessInvited(ctx)
      found <- ctx.invitedRef.get.map(_.reverse)

      // --- Confirmation step: Deferred → Invited ---
      confirmed <-
        if (interrupted || found.isEmpty) ZIO.succeed(List.empty[Username])
        else if (trigger == RunTrigger.Cli) promptConfirmation(found)
        else ZIO.succeed(found) // Api, Scheduled, FollowUp: auto-confirm

      evalCount <- ctx.evalCountRef.get
      clubId    = ctx.runCtx.clubId
      alias     = ctx.runCtx.alias
      (finalRun, deferredCount) <- withTransaction {
        for {
          _ <- ZIO.foreachDiscard(confirmed) { u =>
            Player.selectByUsername(u)
              .someOrFail(new java.sql.SQLException(s"No player found for confirmed candidate $u"))
              .flatMap(p => RecruitmentCandidate.updateOutcome(ctx.runId, p.playerId, CandidateOutcome.Invited))
          }
          deferredCount <- RecruitmentCandidate.selectDeferredCountByRun(ctx.runId)
          completedAt <- Clock.instant
          finalRun = RecruitmentRun(
            ctx.runId,
            clubId,
            ctx.runCtx.criteria.criteriaId,
            trigger,
            startedAt,
            Some(completedAt),
            confirmed.size,
            jobRunId
          )
          _ <- RecruitmentRun.update(finalRun)
        } yield (finalRun, deferredCount)
      }
      duration = JDuration.between(startedAt, finalRun.completedAt.get)
      _ <- ZIO.logInfo(s"=== $label ===")
      _ <- ZIO.logInfo(s"Duration: ${duration.display}")
      _ <- ZIO.logInfo(s"Candidates evaluated: $evalCount")
      _ <- ZIO.logInfo(s"Invited: ${confirmed.size}")
      _ <- ZIO.whenDiscard(found.nonEmpty && confirmed.isEmpty)(
        ZIO.logInfo(s"Found (not confirmed): ${found.size}")
      )
      _ <- ZIO.whenDiscard(deferredCount > 0)(ZIO.logInfo(s"Deferred: $deferredCount"))
      // CLI prompt already displayed candidates; skip redundant listing
      _ <- ZIO.whenDiscard(trigger != RunTrigger.Cli)(
        ZIO.foreachDiscard(confirmed)(u => ZIO.logInfo(s"  $u"))
      )
      // Cumulative summary: show today's total across all runs
      _ <- ZIO.whenDiscard(cumulative && alreadyFound > 0) {
        for {
          earlierCandidates  <- RecruitmentCandidate.selectInvitedToday(clubId, alias)
          earlierResolvedMap <- Player.resolveUsernames(earlierCandidates.map(_.playerId))
          earlierUsernames = earlierCandidates.map(c =>
            earlierResolvedMap.getOrElse(c.playerId, Username.wrap(s"[pid=${c.playerId}]"))
          )
          allToday = earlierUsernames ++ confirmed
          _ <- ZIO.logInfo(s"=== Today's Total: ${allToday.size} ===")
          _ <- ZIO.foreachDiscard(allToday)(u => ZIO.logInfo(s"  $u"))
        } yield ()
      }
    } yield finalRun

  private def promptConfirmation(found: List[Username]): Task[List[Username]] =
    for {
      _ <- ZIO.logInfo(s"\nFound ${found.size} candidates:")
      _ <- ZIO.foreachDiscard(found)(u => ZIO.logInfo(s"  $u"))
      answer <- ZIO.attemptBlocking(
        scala.io.StdIn.readLine(s"\nMark all ${found.size} candidates as Invited? [Y/n] ")
      ).orElse(ZIO.succeed("n"))
    } yield
      if (answer == null || answer.trim.isEmpty || answer.trim.toLowerCase.startsWith("y")) found
      else Nil

  // --- Report mode ---

  private def formatRecruitmentOutput(
    usernames: List[Username],
    evaluatedCount: Int,
    startedAt: Instant,
    completedAt: Instant
  ): String = {
    val duration = JDuration.between(startedAt, completedAt)
    val timing   = s"Started:   $startedAt\nCompleted: $completedAt\nDuration:  ${duration.display}\n\n"
    val stats    = s"Evaluated: $evaluatedCount | Invited: ${usernames.size}"
    val header   = usernames.mkString(" ")
    val detail   = usernames.map(name => s"$name ${ApiPlayer.getProfileUrl(name)}").mkString("\n")
    s"$timing$stats\n\n$header\n\n$detail\n"
  }

  final case class RecruitmentReportResult(usernames: List[Username], evaluatedCount: Int, run: RecruitmentRun)

  def showReport(
    clubSlug: ClubSlug,
    runIdOpt: Option[String]
  ): RIO[PostgresClient, RecruitmentReportResult] =
    for {
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(NotFoundException(s"Club '$clubSlug' not found in database"))
      clubId = club.clubId
      run <- runIdOpt match {
        case Some(id) =>
          ZIO.attempt(id.toLong)
            .orElseFail(BadRequestException(s"Invalid run ID: '$id' (expected a number)"))
            .map(RecruitmentRunId.wrap)
            .flatMap(RecruitmentRun.selectId)
            .someOrFail(NotFoundException(s"Run $id not found"))
        case None =>
          RecruitmentRun.selectLatest(clubId)
            .someOrFail(NotFoundException(s"No runs found for club '$clubSlug'"))
      }
      invited        <- RecruitmentCandidate.selectInvitedByRun(run.runId)
      evaluatedCount <- RecruitmentCandidate.selectCountByRun(run.runId)
      _              <- ZIO.logInfo(s"=== Recruitment Report for $clubSlug (run ${run.runId}) ===")
      _              <- ZIO.logInfo(s"Started: ${run.startedAt}")
      _              <- ZIO.logInfo(s"Completed: ${run.completedAt.getOrElse("in progress")}")
      _              <- ZIO.logInfo(s"Evaluated: $evaluatedCount | Invited: ${invited.size}")
      resolvedMap    <- Player.resolveUsernames(invited.map(_.playerId))
      usernames = invited.map(c => resolvedMap.getOrElse(c.playerId, Username.wrap(s"[pid=${c.playerId}]")))
      _ <- ZIO.logInfo(usernames.mkString(" "))
      _ <- ZIO.logInfo("")
      _ <- ZIO.foreachDiscard(usernames) { name =>
        ZIO.logInfo(ApiPlayer.getProfileUrl(name).toString)
      }
    } yield RecruitmentReportResult(usernames, evaluatedCount, run)

  // --- Match ref writing ---

  private def writeClubMatchRef(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    clubMatches: ApiClubMatches
  ): RIO[PostgresClient, Unit] =
    ZIO.whenZIODiscard(ClubMatchRef.findOrInfer(clubId).map(_.isEmpty)) {
      ZIO.foreachDiscard(clubMatches.finished.headOption) { m =>
        val parsed = RefHelpers.parseMatchUrl(m.`@id`)
        RefHelpers.fetchTeamMatchTeams(client, parsed.matchId, parsed.isLive).flatMap { teams =>
          ZIO.foreachDiscard(RefHelpers.findClubIsTeam1(teams, clubSlug)) { t1 =>
            ClubMatchRef.upsert(ClubMatchRef(clubId, parsed.matchId, parsed.isLive, t1)).unit
          }
        }
      }
    }
}
