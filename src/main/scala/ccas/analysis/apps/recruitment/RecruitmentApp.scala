package ccas.analysis.apps.recruitment

import java.time.{Duration as JDuration, Instant}

import com.augustnagro.magnum.Transactor
import zio.{RIO, Ref, Scope, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.{CcasLogger, OutputFile, display}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.DataSourceLayer

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
    * Positional args (alias, source clubs) must appear before any flags.
    * Flags start at the first `--`-prefixed token; `--target` consumes the next token as its value.
    */
  private[recruitment] def parseRecruitArgs(rest: List[String]): RecruitArgs = {
    val flagStart                  = rest.indexWhere(_.startsWith("--"))
    val (positional, flagTokens)   = if (flagStart < 0) (rest, Nil) else rest.splitAt(flagStart)
    val target = flagTokens.indexOf("--target") match {
      case i if i >= 0 && i + 1 < flagTokens.size => flagTokens(i + 1).toIntOption
      case _                                       => None
    }
    RecruitArgs(
      alias       = positional.headOption.getOrElse("default"),
      sourceClubs = positional.drop(1).map(ClubSlug.wrap),
      target      = target,
      cumulative  = flagTokens.contains("--cumulative"),
      focus       = flagTokens.contains("--focus")
    )
  }

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- (args.toList match {
        case "report" :: clubStr :: rest =>
          val clubSlug = ClubSlug.wrap(clubStr)
          showReport(clubSlug, rest.headOption).flatMap { case (usernames, evaluatedCount, reportRun) =>
            val startedAt   = reportRun.startedAt
            val completedAt = reportRun.completedAt.getOrElse(Instant.now())
            OutputFile.writeAndLog("recruitment", clubSlug, formatRecruitmentOutput(usernames, evaluatedCount, startedAt, completedAt))
          }
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
              usernames <- ZIO.foreach(candidates)(c =>
                PlayerSnapshot.selectIdLatest(c.playerId)
                  .map(_.fold(Username.wrap(s"[pid=${c.playerId}]"))(_.username))
              )
              evaluatedCount <- RecruitmentCandidate.selectCountByRun(run.runId)
              _ <- OutputFile.writeAndLog("recruitment", clubSlug, formatRecruitmentOutput(usernames, evaluatedCount, run.startedAt, run.completedAt.getOrElse(Instant.now())))
            } yield ()
          }
        case _ => ZIO.fail(BadRequestException(help))
      }).provideSome[Scope](
        CcasLogger.live(showProgress = true),
        ChessComClient.live,
        Client.default,
        DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
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
    trigger: RunTrigger = RunTrigger.Cli
  ): RIO[CcasLogger & ChessComClient & Transactor, RecruitmentRun] = ZIO.scoped {
    for {
      _       <- MembershipApp.reconcile(clubSlug, trackRun = false)
      client  <- ZIO.service[ChessComClient]
      logger  <- ZIO.service[CcasLogger]
      apiClub <- ApiClub.get(client, clubSlug)
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), clubSlug, apiClub.name)
      _ <- Club.upsert(club)
      aliasRow <- RecruitmentAlias.selectLatest(clubId, alias)
        .someOrFail(NotFoundException(s"No recruitment alias '$alias' found for club '$clubSlug'"))
      criteria <- RecruitmentCriteria.selectId(aliasRow.criteriaId)
        .someOrFail(IllegalStateException(s"Criteria ${aliasRow.criteriaId} referenced by alias '$alias' not found"))
      resolvedTarget = target.getOrElse(DefaultTarget)
      alreadyFound <-
        if (cumulative) RecruitmentRun.sumCandidatesFoundToday(clubId, alias)
        else ZIO.succeed(0)
      effectiveTarget = (resolvedTarget - alreadyFound) max 0
      now             = Instant.now()
      runId <- RecruitmentRun.insert(clubId, criteria.criteriaId, trigger, now)

      // --- Shared setup ---
      targetMembers <- ApiClubMembers.get(client, clubSlug)
      membersMap        = targetMembers.toMap
      existingUsernames = membersMap.keySet
      targetMemberNames = membersMap.keys.toList

      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))
      targetMatchIds = (clubMatches.registered.map(_.`@id`) ++ clubMatches.inProgress.map(_.`@id`)).toSet
      _ <- writeClubMatchRef(client, clubId, clubSlug, clubMatches).catchAll(_ => ZIO.unit)

      formerMemberIds <-
        if (criteria.excludeFormerMembers)
          ClubMember.selectClubFormer(clubId).map(_.map(_.playerId).toSet)
        else ZIO.succeed(Set.empty[PlayerId])

      excludedSlugs <- ZIO.foreach(criteria.excludeClubs)(Club.selectId).map(_.flatten.map(_.slug).toSet)
      discoveredClubs     <- Ref.make(Set.empty[ClubSlug])
      discoveredOpponents <- Ref.make(Set.empty[Username])
      invitedRef          <- Ref.make(List.empty[Username])
      evaluatedRef        <- Ref.make(Set.empty[Username])
      evalCountRef        <- Ref.make(0)

      runCtx = RunContext(
        client,
        criteria,
        clubId,
        alias,
        targetMatchIds,
        formerMemberIds,
        excludedSlugs,
        Instant.now(),
        discoveredClubs,
        discoveredOpponents
      )
      filters              = RecruitmentFilters.buildFilterChain(criteria)
      effectiveConcurrency = DefaultExploreConcurrency

      progressBar <- CcasLogger.progressBar
      ctx = ExploreContext(
        runId = runId,
        clubSlug = clubSlug,
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

      transactor <- ZIO.service[Transactor]

      _ <- ZIO.whenDiscard(cumulative && alreadyFound > 0)(
        CcasLogger.info(s"[Cumulative] Already found $alreadyFound today, effective target: $effectiveTarget")
      )

      finalRun <-
        if (effectiveTarget == 0) {
          CcasLogger.info("[Cumulative] Target already met, skipping explore") *>
            finalizeRun(ctx, trigger, now, cumulative, alreadyFound, "Recruitment Complete (target already met)")
        } else {
          runExplorePhase(
            ctx, client, logger, transactor, clubMatches,
            targetMemberNames, sourceClubs, timeLimitMinutes, trigger, now, cumulative, alreadyFound
          )
        }
    } yield finalRun
  }

  private def runExplorePhase(
    ctx: ExploreContext,
    client: ChessComClient,
    logger: CcasLogger,
    transactor: Transactor,
    clubMatches: ApiClubMatches,
    targetMemberNames: List[Username],
    sourceClubs: List[ClubSlug],
    timeLimitMinutes: Option[Int],
    trigger: RunTrigger,
    startedAt: Instant,
    cumulative: Boolean,
    alreadyFound: Int
  ): RIO[CcasLogger & ChessComClient & Transactor, RecruitmentRun] =
    for {
      _ <- ZIO.whenDiscard(ctx.showHints)(
        CcasLogger.info("[Hint] Press Ctrl+C to stop gracefully (candidates found so far will be listed)")
      )

      // --- Load deferred candidates from prior runs as a priority source ---
      deferredCandidates <- RecruitmentCandidate.selectDeferredByClub(ctx.runCtx.clubId)
      deferredUsernames <- ZIO.foreach(deferredCandidates)(c =>
        PlayerSnapshot.selectIdLatest(c.playerId).map(_.map(_.username))
      ).map(_.flatten.filterNot(ctx.existingUsernames))
      _ <- ZIO.whenDiscard(deferredUsernames.nonEmpty)(
        CcasLogger.info(s"[Deferred] Found ${deferredUsernames.size} deferred candidates from prior runs")
      )

      // --- Build initial sources from provided source clubs ---
      deferredSource = Option.when(deferredUsernames.nonEmpty)(
        UsernameSource("deferred-priority", deferredUsernames)
      )
      initialSources = deferredSource.toList ++ sourceClubs.map(ClubSource(_))

      // --- Build static strategy list (only used when explore == true) ---
      staticStrategies: List[RIO[CcasLogger & Transactor, List[SourceDescriptor]]] =
        if (!ctx.explore) Nil
        else
          List(
            RecruitmentExplore.discoverOwnMemberClubs(client, ctx.clubSlug, targetMemberNames),
            RecruitmentExplore.discoverDbClubs(ctx.clubSlug),
            RecruitmentExplore.discoverMatchOpponents(clubMatches),
            RecruitmentExplore.discoverCandidateOpponents(client, startedAt)
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
        finalizeRun(ctx, trigger, startedAt, cumulative, alreadyFound, "Recruitment Interrupted", interrupted = true)
          .provideEnvironment(ZEnvironment(logger, transactor))
          .orDie
      )

      // --- Finalize ---
      finalRun <- finalizeRun(ctx, trigger, startedAt, cumulative, alreadyFound, "Recruitment Complete")
    } yield finalRun

  private def finalizeRun(
    ctx: ExploreContext,
    trigger: RunTrigger,
    startedAt: Instant,
    cumulative: Boolean,
    alreadyFound: Int,
    label: String,
    interrupted: Boolean = false
  ): RIO[CcasLogger & Transactor, RecruitmentRun] =
    for {
      _     <- ctx.progressBar.finish
      _     <- RecruitmentExplore.reclassifyExcessInvited(ctx)
      found <- ctx.invitedRef.get.map(_.reverse)

      // --- Confirmation step: Deferred → Invited ---
      confirmed <-
        if (interrupted || found.isEmpty) ZIO.succeed(List.empty[Username])
        else if (trigger == RunTrigger.Cli) promptConfirmation(found)
        else ZIO.succeed(found) // Api, Scheduled, FollowUp: auto-confirm

      _ <- ZIO.foreachDiscard(confirmed) { u =>
        PlayerSnapshot.selectNameLatest(u)
          .someOrFail(new java.sql.SQLException(s"No snapshot for confirmed candidate $u"))
          .flatMap(snap => RecruitmentCandidate.updateOutcome(ctx.runId, snap.playerId, CandidateOutcome.Invited))
      }

      evalCount     <- ctx.evalCountRef.get
      deferredCount <- RecruitmentCandidate.selectDeferredCountByRun(ctx.runId)
      completedAt = Instant.now()
      duration    = JDuration.between(startedAt, completedAt)
      clubId      = ctx.runCtx.clubId
      alias       = ctx.runCtx.alias
      finalRun    = RecruitmentRun(ctx.runId, clubId, ctx.runCtx.criteria.criteriaId, trigger, startedAt, Some(completedAt), confirmed.size)
      _ <- RecruitmentRun.update(finalRun)
      _ <- CcasLogger.info(s"=== $label ===")
      _ <- CcasLogger.info(s"Duration: ${duration.display}")
      _ <- CcasLogger.info(s"Candidates evaluated: $evalCount")
      _ <- CcasLogger.info(s"Invited: ${confirmed.size}")
      _ <- ZIO.whenDiscard(found.nonEmpty && confirmed.isEmpty)(
        CcasLogger.info(s"Found (not confirmed): ${found.size}")
      )
      _ <- ZIO.whenDiscard(deferredCount > 0)(CcasLogger.info(s"Deferred: $deferredCount"))
      // CLI prompt already displayed candidates; skip redundant listing
      _ <- ZIO.whenDiscard(trigger != RunTrigger.Cli)(
        ZIO.foreachDiscard(confirmed)(u => CcasLogger.info(s"  $u"))
      )
      // Cumulative summary: show today's total across all runs
      _ <- ZIO.whenDiscard(cumulative && alreadyFound > 0) {
        for {
          earlierCandidates <- RecruitmentCandidate.selectInvitedToday(clubId, alias)
          earlierUsernames <- ZIO.foreach(earlierCandidates)(c =>
            PlayerSnapshot.selectIdLatest(c.playerId)
              .map(_.fold(Username.wrap(s"[pid=${c.playerId}]"))(_.username))
          )
          allToday = earlierUsernames ++ confirmed
          _ <- CcasLogger.info(s"=== Today's Total: ${allToday.size} ===")
          _ <- ZIO.foreachDiscard(allToday)(u => CcasLogger.info(s"  $u"))
        } yield ()
      }
    } yield finalRun

  private def promptConfirmation(found: List[Username]): RIO[CcasLogger, List[Username]] =
    for {
      _ <- CcasLogger.info(s"\nFound ${found.size} candidates:")
      _ <- ZIO.foreachDiscard(found)(u => CcasLogger.info(s"  $u"))
      answer <- ZIO.attemptBlocking(
        scala.io.StdIn.readLine(s"Mark all ${found.size} candidates as Invited? [Y/n] ")
      ).orElse(ZIO.succeed("n"))
    } yield {
      if (answer == null || answer.trim.isEmpty || answer.trim.toLowerCase.startsWith("y")) found
      else Nil
    }

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

  def showReport(clubSlug: ClubSlug, runIdOpt: Option[String]): RIO[CcasLogger & Transactor, (List[Username], Int, RecruitmentRun)] =
    for {
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(NotFoundException(s"Club '$clubSlug' not found in database"))
      clubId = club.clubId
      run <- runIdOpt match {
        case Some(id) =>
          ZIO.attempt(id.toLong)
            .orElseFail(BadRequestException(s"Invalid run ID: '$id' (expected a number)"))
            .flatMap(RecruitmentRun.selectId)
            .someOrFail(NotFoundException(s"Run $id not found"))
        case None =>
          RecruitmentRun.selectLatest(clubId)
            .someOrFail(NotFoundException(s"No runs found for club '$clubSlug'"))
      }
      invited        <- RecruitmentCandidate.selectInvitedByRun(run.runId)
      evaluatedCount <- RecruitmentCandidate.selectCountByRun(run.runId)
      _              <- CcasLogger.info(s"=== Recruitment Report for $clubSlug (run ${run.runId}) ===")
      _              <- CcasLogger.info(s"Started: ${run.startedAt}")
      _              <- CcasLogger.info(s"Completed: ${run.completedAt.getOrElse("in progress")}")
      _              <- CcasLogger.info(s"Evaluated: $evaluatedCount | Invited: ${invited.size}")
      usernames <- ZIO.foreach(invited) { c =>
        PlayerSnapshot.selectIdLatest(c.playerId)
          .map(_.fold(Username.wrap(s"[pid=${c.playerId}]"))(_.username))
      }
      _ <- CcasLogger.info(usernames.mkString(" "))
      _ <- CcasLogger.info("")
      _ <- ZIO.foreachDiscard(usernames) { name =>
        CcasLogger.info(ApiPlayer.getProfileUrl(name).toString)
      }
    } yield (usernames, evaluatedCount, run)

  // --- Match ref writing ---

  private def writeClubMatchRef(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    clubMatches: ApiClubMatches
  ): RIO[Transactor, Unit] =
    ClubMatchRef.selectId(clubId).flatMap {
      case Some(_) => ZIO.unit
      case None =>
        ClubMatch.selectClubMatchRef(clubId).flatMap {
          case Some(ref) => ClubMatchRef.insert(ref).unit
          case None =>
            ZIO.foreachDiscard(clubMatches.finished.headOption) { m =>
              val parsed = RefHelpers.parseMatchUrl(m.`@id`)
              RefHelpers.fetchTeamMatchTeams(client, parsed.matchId, parsed.isLive).flatMap { teams =>
                ZIO.foreachDiscard(RefHelpers.findClubIsTeam1(teams, clubSlug)) { t1 =>
                  ClubMatchRef.insert(ClubMatchRef(clubId, parsed.matchId, parsed.isLive, t1)).unit
                }
              }
            }
        }
    }
}
