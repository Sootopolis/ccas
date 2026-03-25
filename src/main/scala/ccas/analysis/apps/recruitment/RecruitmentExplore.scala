package ccas.analysis.apps.recruitment

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.Transactor
import zio.{RIO, Task, UIO, ZIO}

import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.api.player.*
import ccas.utils.client.ChessComClient

private[recruitment] object RecruitmentExplore {

  // --- Explore loop ---

  def exploreLoop(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    pendingSources: List[SourceDescriptor],
    staticStrategies: List[RIO[Transactor, List[SourceDescriptor]]],
    visitedClubs: Set[ClubSlug],
    roundRobinKeys: List[String]
  ): RIO[Transactor, Unit] =
    for {
      invited <- ctx.invitedRef.get
      _ <- ZIO.unlessDiscard(invited.size >= ctx.target) {
        if (activePool.isEmpty && pendingSources.isEmpty)
          tryReplenishAndContinue(ctx, activePool, visitedClubs, staticStrategies, roundRobinKeys)
        else
          activateAndPickCandidate(ctx, activePool, pendingSources, staticStrategies, visitedClubs, roundRobinKeys)
      }
    } yield ()

  private def tryReplenishAndContinue(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    visitedClubs: Set[ClubSlug],
    staticStrategies: List[RIO[Transactor, List[SourceDescriptor]]],
    roundRobinKeys: List[String]
  ): RIO[Transactor, Unit] =
    for {
      evaluated   <- ctx.evaluatedRef.get
      replenished <- replenish(ctx, evaluated, visitedClubs, staticStrategies)
      (newSources, remainingStrategies) = replenished
      _ <- ZIO.unlessDiscard(newSources.isEmpty && remainingStrategies.isEmpty)(
        exploreLoop(ctx, activePool, newSources, remainingStrategies, visitedClubs, roundRobinKeys)
      )
    } yield ()

  private def activateAndPickCandidate(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    pendingSources: List[SourceDescriptor],
    staticStrategies: List[RIO[Transactor, List[SourceDescriptor]]],
    visitedClubs: Set[ClubSlug],
    roundRobinKeys: List[String]
  ): RIO[Transactor, Unit] =
    for {
      evaluated  <- ctx.evaluatedRef.get
      activation <- activateSources(ctx, activePool, pendingSources, evaluated, visitedClubs)
      _ <-
        if (activation.pool.isEmpty)
          tryReplenishAndContinue(ctx, activation.pool, activation.visited, staticStrategies, roundRobinKeys)
        else {
          val keys =
            if (roundRobinKeys.exists(activation.pool.contains)) roundRobinKeys.filter(activation.pool.contains)
            else activation.pool.keys.toList
          for {
            picked <- keys match {
              case head :: tail => ZIO.succeed((head, tail :+ head))
              case Nil          => ZIO.die(new IllegalStateException("pool non-empty but no keys"))
            }
            (sourceId, nextKeys) = picked
            sourceState          = activation.pool(sourceId)
            _ <-
              if (sourceState.remaining.isEmpty) {
                val pool3 = activation.pool - sourceId
                exploreLoop(
                  ctx,
                  pool3,
                  activation.pending,
                  staticStrategies,
                  activation.visited,
                  nextKeys.filter(_ != sourceId)
                )
              } else {
                evaluateBatchFromSource(
                  ctx,
                  activation.pool,
                  sourceState,
                  sourceId,
                  activation.pending,
                  staticStrategies,
                  activation.visited,
                  nextKeys
                )
              }
          } yield ()
        }
    } yield ()

  private def checkRecentlyRejected(ctx: ExploreContext, username: Username): RIO[Transactor, Boolean] =
    ctx.runCtx.criteria.daysSinceRejected.fold(ZIO.succeed(false)) { days =>
      for {
        snapOpt <- PlayerSnapshot.selectNameLatest(username)
        rejectOpt <- ZIO.foreach(snapOpt)(snap =>
          RecruitmentCandidate.selectLatestRejectedByAlias(
            snap.playerId,
            ctx.runCtx.clubId,
            ctx.runCtx.alias
          )
        ).map(_.flatten)
      } yield rejectOpt.exists(c => ChronoUnit.DAYS.between(c.evaluatedAt, ctx.runCtx.now) < days)
    }

  private def evaluateBatchFromSource(
    ctx: ExploreContext,
    pool: Map[String, SourceState],
    sourceState: SourceState,
    sourceId: String,
    pendingSources: List[SourceDescriptor],
    staticStrategies: List[RIO[Transactor, List[SourceDescriptor]]],
    visitedClubs: Set[ClubSlug],
    nextKeys: List[String]
  ): RIO[Transactor, Unit] = {
    val (chunk, rest) = sourceState.remaining.splitAt(ctx.evalChunkSize)
    val pool3         = pool.updated(sourceId, sourceState.copy(remaining = rest))
    for {
      // Filter out already-evaluated and recently-rejected candidates
      alreadyEvaluated <- ctx.evaluatedRef.get
      freshChunk = chunk.filterNot(alreadyEvaluated)
      filteredChunk <- ZIO.filter(freshChunk)(u => checkRecentlyRejected(ctx, u).map(!_))
      recentlyRejected = freshChunk.filterNot(filteredChunk.contains)
      _ <- ctx.evaluatedRef.update(_ ++ recentlyRejected.toSet)

      // Evaluate chunk with bounded parallelism (continuous throughput)
      results <- ZIO.foreachPar(filteredChunk)(u =>
        RecruitmentFilters.evaluateCandidate(ctx.runId, u, ctx.runCtx, ctx.filters).map(u -> _)
      )

      // Update refs
      invitedInBatch  = results.collect { case (u, CandidateOutcome.Invited) => u }
      rejectedInBatch = results.count { case (_, o) => o == CandidateOutcome.Rejected || o == CandidateOutcome.Error }
      _ <- ctx.evaluatedRef.update(_ ++ filteredChunk.toSet)
      _ <- ctx.evalCountRef.update(_ + filteredChunk.size)
      _ <- ZIO.foreachDiscard(invitedInBatch)(u => ctx.invitedRef.update(u :: _))
      _ <- reclassifyExcessInvited(ctx)
      _ <- printProgress(ctx)

      // Compute consecutive rejects: trailing rejects after last invite in chunk
      chunkConsecutiveRejects = {
        val orderedResults = results.map(_._2)
        val lastInviteIdx  = orderedResults.lastIndexWhere(_ == CandidateOutcome.Invited)
        if (lastInviteIdx >= 0) orderedResults.drop(lastInviteIdx + 1).size
        else sourceState.consecutiveRejects + orderedResults.size
      }

      updatedSource = pool3(sourceId).copy(
        evaluated = sourceState.evaluated + filteredChunk.size,
        rejected = sourceState.rejected + rejectedInBatch,
        consecutiveRejects =
          if (invitedInBatch.nonEmpty) chunkConsecutiveRejects
          else sourceState.consecutiveRejects + filteredChunk.size
      )
      pool4 <-
        if (ctx.explore && isGrim(updatedSource)) {
          ZIO.logInfo(
            s"[Explore] Abandoning grim source: $sourceId (eval=${updatedSource.evaluated}, rej=${updatedSource.rejected})"
          ).as(pool3 - sourceId)
        } else ZIO.succeed(pool3.updated(sourceId, updatedSource))
      _ <- exploreLoop(ctx, pool4, pendingSources, staticStrategies, visitedClubs, nextKeys)
    } yield ()
  }

  private def printProgress(ctx: ExploreContext): UIO[Unit] =
    ZIO.whenDiscard(ctx.showProgress)(for {
      invited   <- ctx.invitedRef.get
      evalCount <- ctx.evalCountRef.get
      _ <- ctx.progressBar.print(invited.size, ctx.target,
        s"[Progress] Evaluated: $evalCount | Invited: ${invited.size}/${ctx.target}")
    } yield ())

  /** When invited count exceeds the target, reclassify the newest excess from Invited to Deferred. */
  def reclassifyExcessInvited(ctx: ExploreContext): RIO[Transactor, Unit] =
    for {
      invited <- ctx.invitedRef.get
      excess = invited.size - ctx.target
      _ <- ZIO.whenDiscard(excess > 0) {
        // invitedRef is prepend-ordered (newest first), so take excess from the head
        val toDefer = invited.take(excess)
        for {
          playerIds <- ZIO.foreach(toDefer)(u =>
            PlayerSnapshot.selectNameLatest(u)
              .someOrFail(new java.sql.SQLException(s"No snapshot for deferred candidate $u"))
              .map(_.playerId)
          )
          _ <- ZIO.foreachDiscard(playerIds)(pid =>
            RecruitmentCandidate.updateOutcome(ctx.runId, pid, CandidateOutcome.Deferred)
          )
          _ <- ctx.invitedRef.set(invited.drop(excess))
        } yield ()
      }
    } yield ()

  // --- Source activation ---

  private def activateSources(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    pendingSources: List[SourceDescriptor],
    evaluatedUsernames: Set[Username],
    visitedClubs: Set[ClubSlug]
  ): RIO[Transactor, ActivationResult] = {
    val slotsAvailable = ctx.exploreConcurrency - activePool.size
    if (slotsAvailable <= 0 || pendingSources.isEmpty)
      ZIO.succeed(ActivationResult(activePool, pendingSources, visitedClubs))
    else {
      val (toActivate, remaining) = pendingSources.splitAt(slotsAvailable)
      ZIO.foreachPar(toActivate) { source =>
        activateSource(ctx, source, evaluatedUsernames).map(members => (source, members))
      }.map { results =>
        val (pool2, visited2) = results.foldLeft((activePool, visitedClubs)) { case ((pool, visited), (source, members)) =>
          val newVisited = source match {
            case ClubSource(name) => visited + name
            case _                => visited
          }
          if (members.isEmpty) (pool, newVisited)
          else (pool + (source.id -> SourceState(members, 0, 0, 0)), newVisited)
        }
        ActivationResult(pool2, remaining, visited2)
      }
    }
  }

  def gatherClubCandidates(
    client: ChessComClient,
    clubSlug: ClubSlug,
    excludeSourceAdmins: Boolean,
    existingUsernames: Set[Username],
    evaluatedUsernames: Set[Username]
  ): Task[List[Username]] = {
    val getMembersAndAdmins = if (excludeSourceAdmins) {
      ApiClubMembers.get(client, clubSlug).map(_.all.map(_.username).toList)
        .zipPar(ApiClub.get(client, clubSlug).map(extractAdminUsernames))
    } else ApiClubMembers.get(client, clubSlug).map(m => (m.all.map(_.username).toList, Set.empty[Username]))
    getMembersAndAdmins.map { (orderedMembers, adminUsernames) =>
      val exclude = existingUsernames ++ evaluatedUsernames ++ adminUsernames
      orderedMembers.filterNot(exclude).distinct
    }
  }

  private def activateSource(
    ctx: ExploreContext,
    source: SourceDescriptor,
    evaluatedUsernames: Set[Username]
  ): RIO[Transactor, List[Username]] =
    source match {
      case ClubSource(clubSlug) =>
        for {
          filtered <- gatherClubCandidates(
            ctx.runCtx.client,
            clubSlug,
            ctx.runCtx.criteria.excludeSourceAdmins,
            ctx.existingUsernames,
            evaluatedUsernames
          )
          _ <- ZIO.logInfo(
            s"[Explore] Activated club source: ${ClubSlug.unwrap(clubSlug)} (${filtered.size} candidates)"
          )
        } yield filtered
      case UsernameSource(id, usernames) =>
        val exclude  = ctx.existingUsernames ++ evaluatedUsernames
        val filtered = usernames.filterNot(exclude)
        ZIO.logInfo(s"[Explore] Activated username source: $id (${filtered.size} candidates)").as(filtered)
    }

  // --- Replenishment ---

  private def replenish(
    ctx: ExploreContext,
    evaluatedUsernames: Set[Username],
    visitedClubs: Set[ClubSlug],
    staticStrategies: List[RIO[Transactor, List[SourceDescriptor]]]
  ): RIO[Transactor, (List[SourceDescriptor], List[RIO[Transactor, List[SourceDescriptor]]])] =
    if (!ctx.explore) ZIO.succeed((Nil, staticStrategies))
    else
      for {
        // Dynamic strategy 1: candidate opponents
        opponents <- ctx.runCtx.discoveredOpponents.get
        newOpponents = opponents -- evaluatedUsernames
        result <-
          if (newOpponents.nonEmpty) {
            ZIO.logInfo(s"[Explore] Discovered ${newOpponents.size} candidate opponents")
              .as((List(UsernameSource("candidate-opponents", newOpponents.toList)), staticStrategies))
          } else {
            // Dynamic strategy 2: candidate clubs
            for {
              clubs <- ctx.runCtx.discoveredClubs.get
              newClubs = clubs.diff(visitedClubs).filterNot(_ == ctx.clubSlug)
                .filterNot(ctx.runCtx.criteria.excludeClubNames.contains)
              result <-
                if (newClubs.nonEmpty) {
                  ZIO.logInfo(s"[Explore] Discovered ${newClubs.size} candidate clubs")
                    .as((newClubs.toList.map(ClubSource(_)), staticStrategies))
                } else {
                  // Static strategies: try next one
                  staticStrategies match {
                    case Nil => ZIO.succeed((Nil, Nil))
                    case head :: tail =>
                      for {
                        sources <- head
                        filtered = sources.filter {
                          case ClubSource(name) =>
                            !visitedClubs.contains(name) && name != ctx.clubSlug &&
                            !ctx.runCtx.criteria.excludeClubNames.contains(name)
                          case _ => true
                        }
                        _ <- ZIO.logInfo(s"[Explore] Static strategy yielded ${filtered.size} sources")
                      } yield (filtered, tail)
                  }
                }
            } yield result
          }
      } yield result

  // --- Discovery strategies ---

  def discoverOwnMemberClubs(
    client: ChessComClient,
    clubSlug: ClubSlug,
    targetMemberNames: List[Username]
  ): RIO[Transactor, List[SourceDescriptor]] = {
    val sample = targetMemberNames.take(20)
    for {
      clubSets <- ZIO.foreachPar(sample) { username =>
        client.get[ApiPlayerClubs](ApiPlayerClubs.getUrl(username))
          .map(_.clubs.map(_.clubName).toSet)
          .catchAll(_ => ZIO.succeed(Set.empty[ClubSlug]))
      }
      allClubs = clubSets.foldLeft(Set.empty[ClubSlug])(_ ++ _) - clubSlug
      _ <- ZIO.logInfo(s"[Explore] Own member clubs strategy found ${allClubs.size} clubs")
    } yield allClubs.toList.map(ClubSource(_))
  }

  def discoverDbClubs(clubSlug: ClubSlug): RIO[Transactor, List[SourceDescriptor]] =
    for {
      clubs <- Club.selectAll
      filtered = scala.util.Random.shuffle(clubs.map(_.slug).filter(_ != clubSlug))
      _ <- ZIO.logInfo(s"[Explore] DB clubs strategy found ${filtered.size} clubs")
    } yield filtered.map(ClubSource(_))

  def discoverMatchOpponents(clubMatches: ApiClubMatches): RIO[Transactor, List[SourceDescriptor]] = {
    val opponentUrls = clubMatches.finished.map(_.opponent)
      ++ clubMatches.inProgress.map(_.opponent)
      ++ clubMatches.registered.map(_.opponent)
    val opponentClubNames = opponentUrls.map(url => ClubSlug.wrap(url.path.segments.last)).toSet
    ZIO.logInfo(s"[Explore] Match opponents strategy found ${opponentClubNames.size} clubs")
      .as(opponentClubNames.toList.map(ClubSource(_)))
  }

  def discoverCandidateOpponents(client: ChessComClient, now: Instant): RIO[Transactor, List[SourceDescriptor]] = {
    val cutoff = now.minus(90, ChronoUnit.DAYS)
    val months = RecruitmentFilters.recentArchiveMonths(now, 90)
    for {
      tmPlayers <- PlayerRecruitmentCache.selectTmActive(20)
      snapshots <- ZIO.foreach(tmPlayers)(c => PlayerSnapshot.selectIdLatest(c.playerId))
      usernames = snapshots.flatten.map(_.username)
      opponentSets <- ZIO.foreachPar(usernames) { username =>
        ZIO.foreachPar(months) { ym =>
          client.get[ApiPlayerArchive](ApiPlayerArchive.getUrl(username, ym.getYear, ym.getMonthValue))
        }.map { archives =>
          archives.flatMap(
            _.games.filter(g => g.timeClass == "daily" && g.`match`.isDefined && g.endTime >= cutoff.getEpochSecond)
          ).flatMap(nonTimeoutOpponent(_, username)).toSet
        }.catchAll(_ => ZIO.succeed(Set.empty[Username]))
      }
      allOpponents = opponentSets.foldLeft(Set.empty[Username])(_ ++ _)
      _ <- ZIO.logInfo(s"[Explore] Candidate opponents strategy found ${allOpponents.size} opponents")
    } yield
      if (allOpponents.isEmpty) Nil
      else List(UsernameSource("db-candidate-opponents", allOpponents.toList))
  }

  private def extractAdminUsernames(apiClub: ApiClub): Set[Username] =
    apiClub.admin.map(url => Username.wrap(url.path.segments.last)).toSet
}
