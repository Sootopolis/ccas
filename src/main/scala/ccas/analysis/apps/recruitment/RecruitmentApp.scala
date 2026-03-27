package ccas.analysis.apps.recruitment

import java.time.{Duration as JDuration, Instant}

import com.augustnagro.magnum.Transactor
import zio.{RIO, Ref, Scope, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubSlug, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.DataSourceLayer
import ccas.utils.OutputFile

object RecruitmentApp extends ZIOAppDefault {

  private val DefaultTarget             = 30
  private val DefaultExploreConcurrency = 1
  private val DefaultEvalChunkSize      = 32 // candidates per checkpoint (source-switch interval)

  private val help =
    """Usage: RecruitmentApp <club-slug> [alias] [source-clubs...] [--target N] [--cumulative] [--focus]
      |       RecruitmentApp report <club-slug> [run-id]
      |
      |  --focus    Only recruit from the given source clubs (no exploration)""".stripMargin

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
          val (flags, positional) = rest.partition(_.startsWith("--"))
          val flagMap = flags.sliding(2, 2).collect { case Seq(k, v) => k -> v }.toMap ++
            flags.filterNot(f => flags.sliding(2, 2).exists(_.headOption.contains(f)) && !f.startsWith("--c")).collect {
              case f if f == "--cumulative" => f -> "true"
            }.toMap
          val targetOpt       = flagMap.get("--target").flatMap(_.toIntOption)
          val cumulative      = flags.contains("--cumulative")
          val focus           = flags.contains("--focus")
          val positionalClean = positional.filterNot(_ == "--cumulative")
          val alias           = positionalClean.headOption.getOrElse("default")
          val sourceClubs     = positionalClean.drop(1).map(ClubSlug.wrap)
          val clubSlug     = ClubSlug.wrap(clubStr)
          recruit(
            clubSlug,
            alias,
            target = targetOpt,
            cumulative = cumulative,
            sourceClubs = sourceClubs,
            explore = sourceClubs.isEmpty || !focus,
            showHints = true
          ).flatMap { run =>
            for {
              candidates <-
                if (cumulative) RecruitmentCandidate.selectInvitedToday(run.clubId, alias)
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

      finalizeRun = (label: String) =>
        for {
          _             <- progressBar.finish
          _             <- RecruitmentExplore.reclassifyExcessInvited(ctx)
          invited       <- invitedRef.get.map(_.reverse)
          evalCount     <- evalCountRef.get
          deferredCount <- RecruitmentCandidate.selectDeferredCountByRun(runId)
          completedAt = Instant.now()
          duration    = JDuration.between(now, completedAt)
          finalRun    = RecruitmentRun(runId, clubId, criteria.criteriaId, trigger, now, Some(completedAt), invited.size)
          _ <- RecruitmentRun.update(finalRun)
          _ <- CcasLogger.info(s"=== $label ===")
          _ <- CcasLogger.info(s"Duration: ${duration.toMinutes}m ${duration.toSecondsPart}s")
          _ <- CcasLogger.info(s"Candidates evaluated: $evalCount")
          _ <- CcasLogger.info(s"Invited: ${invited.size}")
          _ <- ZIO.whenDiscard(deferredCount > 0)(CcasLogger.info(s"Deferred: $deferredCount"))
          _ <- ZIO.foreachDiscard(invited)(u => CcasLogger.info(s"  $u"))
          // Cumulative summary: show today's total across all runs
          _ <- ZIO.whenDiscard(cumulative && alreadyFound > 0) {
            for {
              earlierCandidates <- RecruitmentCandidate.selectInvitedToday(clubId, alias)
              earlierUsernames <- ZIO.foreach(earlierCandidates)(c =>
                PlayerSnapshot.selectIdLatest(c.playerId)
                  .map(_.fold(Username.wrap(s"[pid=${c.playerId}]"))(_.username))
              )
              allToday = earlierUsernames ++ invited
              _ <- CcasLogger.info(s"=== Today's Total: ${allToday.size} ===")
              _ <- ZIO.foreachDiscard(allToday)(u => CcasLogger.info(s"  $u"))
            } yield ()
          }
        } yield finalRun

      _ <- ZIO.whenDiscard(cumulative && alreadyFound > 0)(
        CcasLogger.info(s"[Cumulative] Already found $alreadyFound today, effective target: $effectiveTarget")
      )

      finalRun <-
        if (effectiveTarget == 0) {
          CcasLogger.info("[Cumulative] Target already met, skipping explore") *>
            finalizeRun("Recruitment Complete (target already met)")
        } else {
          for {
            _ <- ZIO.whenDiscard(showHints)(
              CcasLogger.info("[Hint] Press Ctrl+C to stop gracefully (candidates found so far will be listed)")
            )

            // --- Load deferred candidates from prior runs as a priority source ---
            deferredCandidates <- RecruitmentCandidate.selectDeferredByClub(clubId)
            deferredUsernames <- ZIO.foreach(deferredCandidates)(c =>
              PlayerSnapshot.selectIdLatest(c.playerId).map(_.map(_.username))
            ).map(_.flatten.filterNot(existingUsernames))
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
              if (!explore) Nil
              else
                List(
                  RecruitmentExplore.discoverOwnMemberClubs(client, clubSlug, targetMemberNames),
                  RecruitmentExplore.discoverDbClubs(clubSlug),
                  RecruitmentExplore.discoverMatchOpponents(clubMatches),
                  RecruitmentExplore.discoverCandidateOpponents(client, now)
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
              finalizeRun("Recruitment Interrupted")
                .provideEnvironment(ZEnvironment(logger, transactor))
                .orDie
            )

            // --- Finalize ---
            finalRun <- finalizeRun("Recruitment Complete")
          } yield finalRun
        }
    } yield finalRun
  }

  // --- Report mode ---

  private def formatRecruitmentOutput(
    usernames: List[Username],
    evaluatedCount: Int,
    startedAt: Instant,
    completedAt: Instant
  ): String = {
    val duration = JDuration.between(startedAt, completedAt)
    val timing   = s"Started:   $startedAt\nCompleted: $completedAt\nDuration:  ${duration.toMinutes}m ${duration.toSecondsPart}s\n\n"
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
}
