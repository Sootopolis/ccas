package ccas.analysis.apps.recruitment

import java.time.temporal.ChronoUnit
import java.time.{Instant, YearMonth}

import ccas.utils.sql.PostgresClient
import zio.{RIO, Task, UIO, ZIO}

import ccas.analysis.apps.{ClubSlugRenameResolver, withPlayerRenameRecovery}
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.misc.enums.ClubMatchStatus
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Username}
import ccas.api.player.*
import ccas.utils.client.{ChessComClient, onNotFound}
import ccas.utils.ApiConcurrency

private[recruitment] object RecruitmentExplore {

  private case class EvaluatedCandidate(username: Username, outcome: CandidateOutcome)

  private case class ReplenishResult(
    newSources: List[SourceDescriptor],
    remainingStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]]
  )

  // --- Explore loop ---

  def exploreLoop(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    pendingSources: List[SourceDescriptor],
    staticStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]],
    visitedClubs: Set[ClubSlug],
    roundRobinKeys: List[String]
  ): RIO[PostgresClient, Unit] =
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
    staticStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]],
    roundRobinKeys: List[String]
  ): RIO[PostgresClient, Unit] =
    for {
      evaluated   <- ctx.evaluatedRef.get
      replenished <- replenish(ctx, evaluated, staticStrategies)
      _ <- ZIO.unlessDiscard(replenished.newSources.isEmpty && replenished.remainingStrategies.isEmpty) {
        exploreLoop(
          ctx,
          activePool,
          replenished.newSources,
          replenished.remainingStrategies,
          visitedClubs,
          roundRobinKeys
        )
      }
    } yield ()

  private def activateAndPickCandidate(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    pendingSources: List[SourceDescriptor],
    staticStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]],
    visitedClubs: Set[ClubSlug],
    roundRobinKeys: List[String]
  ): RIO[PostgresClient, Unit] =
    for {
      evaluated  <- ctx.evaluatedRef.get
      activation <- activateSources(ctx, activePool, pendingSources, evaluated, visitedClubs)
      _ <-
        if (activation.pool.isEmpty)
          // Don't drop activation.pending: when every source activated this round yields zero candidates the pool is
          // empty but un-activated source clubs may still remain (with exploreConcurrency=1, splitAt activates one
          // source per round). The sibling arms (the empty-`remaining` source branch below and
          // `evaluateBatchFromSource`) thread pending through; route back via exploreLoop so the remaining sources are
          // drained. exploreLoop's own (pool empty && pending empty) guard still reaches tryReplenishAndContinue when
          // pending is genuinely empty, so the no-sources case is unchanged.
          exploreLoop(ctx, activation.pool, activation.pending, staticStrategies, activation.visited, roundRobinKeys)
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

  private def checkRecentlyRejected(ctx: ExploreContext, username: Username): RIO[PostgresClient, Boolean] =
    ctx.runCtx.criteria.daysSinceRejected.fold(ZIO.succeed(false)) { days =>
      for {
        playerOpt <- Player.selectByUsername(username)
        rejectOpt <- ZIO.foreach(playerOpt)(p =>
          RecruitmentCandidate.selectLatestRejectedByAlias(
            p.playerId,
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
    staticStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]],
    visitedClubs: Set[ClubSlug],
    nextKeys: List[String]
  ): RIO[PostgresClient, Unit] = {
    val (chunk, rest) = sourceState.remaining.splitAt(ctx.evalChunkSize)
    val pool3         = pool.updated(sourceId, sourceState.copy(remaining = rest))
    for {
      // Filter out already-evaluated and recently-rejected candidates
      alreadyEvaluated <- ctx.evaluatedRef.get
      freshChunk = chunk.filterNot(alreadyEvaluated)
      filteredChunk <- ZIO.filter(freshChunk)(u => checkRecentlyRejected(ctx, u).map(!_))
      recentlyRejected = freshChunk.filterNot(filteredChunk.contains)
      _ <- ctx.evaluatedRef.update(_ ++ recentlyRejected.toSet)

      // Evaluate chunk with bounded parallelism — update refs per-candidate for lively progress
      results <- ZIO.foreachPar(filteredChunk) { u =>
        for {
          outcome <- RecruitmentFilters.evaluateCandidate(ctx.runId, u, ctx.runCtx, ctx.filters)
          count   <- ctx.evalCountRef.updateAndGet(_ + 1)
          _       <- ctx.evaluatedRef.update(_ + u)
          _       <- ZIO.whenDiscard(outcome == CandidateOutcome.Invited)(ctx.invitedRef.update(u :: _))
          _       <- ZIO.whenDiscard(count % 4 == 0)(printProgress(ctx, sourceId))
        } yield EvaluatedCandidate(u, outcome)
      }.withParallelism(ApiConcurrency.fiberCap(ctx.runCtx.client))
      _ <- printProgress(ctx, sourceId) // ensure final state is rendered

      // Batch-level cleanup
      rejectedInBatch =
        results.count(r => r.outcome == CandidateOutcome.Rejected || r.outcome == CandidateOutcome.Error)
      hadInvite = results.exists(_.outcome == CandidateOutcome.Invited)
      _ <- reclassifyExcessInvited(ctx)

      // Compute consecutive rejects: trailing rejects after last invite in chunk
      chunkConsecutiveRejects = {
        val orderedResults = results.map(_.outcome)
        val lastInviteIdx  = orderedResults.lastIndexWhere(_ == CandidateOutcome.Invited)
        if (lastInviteIdx >= 0) orderedResults.drop(lastInviteIdx + 1).size
        else sourceState.consecutiveRejects + orderedResults.size
      }

      updatedSource = pool3(sourceId).copy(
        evaluated = sourceState.evaluated + filteredChunk.size,
        rejected = sourceState.rejected + rejectedInBatch,
        consecutiveRejects =
          if (hadInvite) chunkConsecutiveRejects
          else sourceState.consecutiveRejects + filteredChunk.size
      )
      pool4 <-
        if (ctx.explore && isGrim(updatedSource)) {
          // Record the un-evaluated tail so replenish won't rediscover and resurrect this abandoned source via
          // discoveredOpponents (a monotonic accumulator that is never cleared). Without this, a grim
          // candidate-opponents source is rebuilt every replenish cycle with consecutiveRejects reset to 0,
          // defeating the abandonment and re-evaluating the whole bad tail in GrimConsecutiveRejects-sized batches.
          ctx.abandonedOpponents.update(_ ++ updatedSource.remaining) *>
            ZIO.logInfo(
              s"[Explore] Abandoning grim source: $sourceId (eval=${updatedSource.evaluated}, rej=${updatedSource.rejected})"
            ).as(pool3 - sourceId)
        } else ZIO.succeed(pool3.updated(sourceId, updatedSource))
      _ <- exploreLoop(ctx, pool4, pendingSources, staticStrategies, visitedClubs, nextKeys)
    } yield ()
  }

  private def printProgress(ctx: ExploreContext, sourceId: String): UIO[Unit] =
    for {
      invited   <- ctx.invitedRef.get
      evalCount <- ctx.evalCountRef.get
      _ <- ctx.progressBar.print(
        invited.size,
        ctx.target,
        s"[Progress] $sourceId | Evaluated: $evalCount | Invited: ${invited.size}/${ctx.target}"
      )
    } yield ()

  /** When found count exceeds the target, trim the newest excess from invitedRef. All candidates are already Deferred
    * in the DB; no status flip needed here.
    */
  def reclassifyExcessInvited(ctx: ExploreContext): RIO[PostgresClient, Unit] =
    // invitedRef is prepend-ordered (newest first), so drop excess from the head
    ctx.invitedRef.update { invited =>
      val excess = invited.size - ctx.target
      if (excess > 0) invited.drop(excess) else invited
    }

  // --- Source activation ---

  private def activateSources(
    ctx: ExploreContext,
    activePool: Map[String, SourceState],
    pendingSources: List[SourceDescriptor],
    evaluatedUsernames: Set[Username],
    visitedClubs: Set[ClubSlug]
  ): RIO[PostgresClient, ActivationResult] = {
    val slotsAvailable = ctx.exploreConcurrency - activePool.size
    if (slotsAvailable <= 0 || pendingSources.isEmpty)
      ZIO.succeed(ActivationResult(activePool, pendingSources, visitedClubs))
    else {
      val (toActivate, remaining) = pendingSources.splitAt(slotsAvailable)
      ZIO.foreachPar(toActivate) { source =>
        activateSource(ctx, source, evaluatedUsernames).map(members => (source, members))
      }.map { results =>
        val (pool2, visited2) = results.foldLeft((activePool, visitedClubs)) {
          case ((pool, visited), (source, members)) =>
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

  /** Loads the source club's members (and admins, when `excludeSourceAdmins`) and trims the candidate list against
    * existing memberships and already-evaluated usernames. Resolves any rename ONCE up front via
    * `ClubSlugRenameResolver.resolveAndPersist`, then runs the two endpoint fetches under the canonical slug. Doing
    * the resolver call inside the parallel `zipPar` would race: whichever leg's wrap won would update the Club row,
    * causing the loser's `deriveHint` lookup to miss and leaving its 404 unrecovered.
    *
    * On the recovery path the resolver returns a verified `ApiClub` for free; we extract admins from it directly
    * instead of issuing a redundant `ApiClub.get(fresh)` in the retry.
    */
  def gatherClubCandidates(
    client: ChessComClient,
    clubSlug: ClubSlug,
    excludeSourceAdmins: Boolean,
    existingUsernames: Set[Username],
    evaluatedUsernames: Set[Username]
  ): RIO[PostgresClient, List[Username]] = {
    def fetchMembers(slug: ClubSlug): Task[List[Username]] =
      ApiClubMembers.get(client, slug).map(_.all.map(_.username).toList)
    def fetchAdmins(slug: ClubSlug): Task[Set[Username]] =
      ApiClub.get(client, slug).map(ClubAdmin.extractAdminUsernames)
    def fetchBoth(slug: ClubSlug): Task[(List[Username], Set[Username])] =
      if (excludeSourceAdmins) { fetchMembers(slug).zipPar(fetchAdmins(slug)) }
      else { fetchMembers(slug).map(m => (m, Set.empty[Username])) }

    val combined: RIO[PostgresClient, (List[Username], Set[Username])] =
      fetchBoth(clubSlug).onNotFound { e =>
        ClubSlugRenameResolver.resolveAndPersist(client, clubSlug, clubIdHint = None).flatMap {
          case Some((fresh, apiClub)) =>
            // Reuse the resolver's verified ApiClub to skip a duplicate `ApiClub.get(fresh)` on the admins leg.
            // Members still need a fresh fetch (different endpoint).
            val adminsCached = ZIO.succeed(ClubAdmin.extractAdminUsernames(apiClub))
            val membersFresh = fetchMembers(fresh)
            val retried =
              if (excludeSourceAdmins) { membersFresh.zipPar(adminsCached) }
              else { membersFresh.map(m => (m, Set.empty[Username])) }
            ZIO.logInfo(s"  Slug rename recovered: $clubSlug → $fresh; retrying gatherClubCandidates") *>
              retried
          case None => ZIO.fail(e)
        }
      }

    combined.map { (orderedMembers, adminUsernames) =>
      val exclude = existingUsernames ++ evaluatedUsernames ++ adminUsernames
      orderedMembers.filterNot(exclude).distinct
    }
  }

  private def activateSource(
    ctx: ExploreContext,
    source: SourceDescriptor,
    evaluatedUsernames: Set[Username]
  ): RIO[PostgresClient, List[Username]] =
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
    staticStrategies: List[RIO[PostgresClient, List[SourceDescriptor]]]
  ): RIO[PostgresClient, ReplenishResult] =
    if (!ctx.explore) ZIO.succeed(ReplenishResult(Nil, staticStrategies))
    else
      for {
        // Dynamic: candidate opponents discovered during evaluation
        opponents <- ctx.runCtx.discoveredOpponents.get
        abandoned <- ctx.abandonedOpponents.get
        // Exclude existingUsernames too, matching `activateSource`'s UsernameSource filter. Otherwise a
        // discovered opponent who is already a club member is re-yielded here every round but filtered to 0 at
        // activation, so evaluatedRef never grows and the loop spins forever (Discovered N → 0 candidates → repeat).
        // `abandoned` holds the un-evaluated tails of grim-abandoned sources so they are not rediscovered.
        newOpponents = opponents -- evaluatedUsernames -- ctx.existingUsernames -- abandoned
        result <-
          if (newOpponents.nonEmpty) {
            ZIO.logInfo(s"[Explore] Discovered ${newOpponents.size} candidate opponents").as(
              ReplenishResult(List(UsernameSource("candidate-opponents", newOpponents.toList)), staticStrategies)
            )
          } else {
            // Static strategies: try next one
            staticStrategies match {
              case Nil => ZIO.succeed(ReplenishResult(Nil, Nil))
              case head :: tail =>
                for {
                  sources <- head
                  _       <- ZIO.logInfo(s"[Explore] Static strategy yielded ${sources.size} sources")
                } yield ReplenishResult(sources, tail)
            }
          }
      } yield result

  // --- Discovery strategies ---

  def discoverCandidateOpponents(
    client: ChessComClient,
    now: Instant
  ): RIO[PostgresClient, List[SourceDescriptor]] = {
    val cutoff = now.minus(90, ChronoUnit.DAYS)
    val months = RecruitmentStatsHelpers.recentArchiveMonths(now, 90)

    def fetchMonth(uname: Username, ym: YearMonth): RIO[PostgresClient, ApiPlayerArchive] =
      client.get[ApiPlayerArchive](ApiPlayerArchive.getUrl(uname, ym.getYear, ym.getMonthValue))

    for {
      tmPlayers <- PlayerRecruitmentCache.selectTmActive(20)
      players   <- Player.selectByIds(tmPlayers.map(_.playerId))
      activePlayers = players.filterNot(_.isTombstoned).map(p => (p.playerId, p.username))
      opponentSets <- ZIO.foreachPar(activePlayers) { case (playerId, username) =>
        val gather = for {
          archives <- ZIO.foreachPar(months) { ym =>
            fetchMonth(username, ym)
              .withPlayerRenameRecovery(client, username, Some(playerId))(uname => fetchMonth(uname, ym))
          }
          // Post-recovery the games' username field reflects the canonical handle; using the original `username`
          // would treat the renamed player AS their own opponent. Re-read off `Player` after the fan-out (rather
          // than reusing the pre-fetched `players` list) because the wrap may have just reconciled this row with a
          // fresh name. Tombstoned rows fall back to the input username so a `_stale_<id>` placeholder doesn't
          // leak into the opponent predicate. N+1 lookup is bounded by `selectTmActive(20)`.
          effectiveUname <- Player.selectId(playerId).map(_.filterNot(_.isTombstoned).fold(username)(_.username))
        } yield archives.flatMap(
          _.games.filter(g => g.timeClass == "daily" && g.`match`.isDefined && g.endTime >= cutoff.getEpochSecond)
        ).flatMap(nonTimeoutOpponent(_, effectiveUname)).toSet
        gather.catchAll(_ => ZIO.succeed(Set.empty[Username]))
      }
      allOpponents = opponentSets.foldLeft(Set.empty[Username])(_ ++ _)
      _ <- ZIO.logInfo(s"[Explore] Candidate opponents strategy found ${allOpponents.size} opponents")
    } yield
      if (allOpponents.isEmpty) Nil
      else List(UsernameSource("db-candidate-opponents", allOpponents.toList))
  }

  def discoverMatchBoardOpponents(clubId: ClubId): RIO[PostgresClient, List[SourceDescriptor]] =
    for {
      matchIds <- ClubMatch.selectMatchIdsForClub(clubId)
      matches <- ZIO.foreach(matchIds.toList)(ClubMatch.selectId).map(
        _.flatten.filter(_.status == ClubMatchStatus.Finished)
      )
      boards <- ZIO.foreach(matches)(m => ClubMatchBoard.selectMatch(m.matchId))
      opponentIds = matches.zip(boards).flatMap { (m, bs) =>
        val isTeam1 = m.team1ClubId.contains(clubId)
        bs.flatMap(b => if (isTeam1) b.team2PlayerId else b.team1PlayerId)
      }.toSet
      players <- Player.selectByIds(opponentIds)
      usernames = players.filterNot(_.isTombstoned).map(_.username)
      _ <- ZIO.logInfo(s"[Explore] Match board opponents strategy found ${usernames.size} players")
    } yield
      if (usernames.isEmpty) Nil
      else List(UsernameSource("match-board-opponents", usernames))

}
