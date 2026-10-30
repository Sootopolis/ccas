package ccas.analysis.apps.membership

import java.time.Instant

import scala.annotation.tailrec

import zio.{Chunk, Clock, ExitCode, IO, NonEmptyChunk, RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.{withClubSlugRenameRecovery, ClubQuery, ClubResolution, ClubSlugRenameResolver, NamedClub}
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.JoinedClub
import ccas.analysis.tables.*
import ccas.analysis.tables.subtypes.MembershipRunId
import ccas.api.club.ApiClubMembers
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId, PlayerId}
import ccas.utils.{OutputFile, ProgressDisplay, TimeParser}
import ccas.utils.client.{BodyStore, ChessComClient, HttpClientLayer, NetworkUnavailableException}
import ccas.utils.errors.{safeMessage, BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

object MembershipApp extends ZIOAppDefault {
  private val SINCE      = "--since"
  private val UNTIL      = "--until"
  private val help       = s"Usage: MembershipApp <club-slug> [club-slug ...] [$SINCE <date>] [$UNTIL <date>]"
  private val MEMBERSHIP = "membership"

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    ProgressDisplay.installed(showProgress = true) {
      (for {
        args   <- ZIOAppArgs.getArgs
        parsed <- parseArgs(args)
        mode   <- parseRunMode(parsed.flags)
        _ <- ZIO.foreachDiscard(parsed.slugs) { clubName =>
          mode match {
            case ReconcileOnly =>
              for {
                (result, invitations) <- reconcileAndReport(
                  clubSlug = clubName,
                  expectedClubIdOption = None,
                  trustUsernames = true,
                  trigger = RunTrigger.Cli,
                  jobRunIdOption = None
                )
                _ <- OutputFile.writeAndLog(
                  MEMBERSHIP,
                  clubName,
                  MembershipReport.formatReconciliation(result, invitations)
                )
              } yield ()
            case SinceNow(since) =>
              for {
                result <- reconcile(clubSlug = clubName, expectedClubIdOption = None)
                club   <- reconciledClub(result)
                until  <- Clock.instant
                rr     <- MembershipReport.report(club, since, until)
                _      <- OutputFile.writeAndLog(MEMBERSHIP, club.slug, MembershipReport.formatReport(rr))
              } yield ()
            case SinceUntil(since, until) =>
              for {
                club <- reconcileIfStale(clubName, until)
                rr   <- MembershipReport.report(club, since, until)
                _    <- OutputFile.writeAndLog(MEMBERSHIP, club.slug, MembershipReport.formatReport(rr))
              } yield ()
          }
        }
      } yield ())
        // A systemic outage aborts: an in-flight reconcile persists nothing its roster implied (one final transaction),
        // so the residue is an incomplete `membership_run` row `selectLatestCompleted` ignores, and any club lists kept.
        .catchSome(NetworkUnavailableException.abortRun("MembershipApp", "run left incomplete")(exit(ExitCode.failure)))
        .provideSomeAuto(
          ChessComClient.live(MEMBERSHIP),
          HttpClientLayer.live,
          BodyStore.live,
          PostgresClient.live(onInit = Tables.ensureTablesOnInit)
        )
    }

  private sealed trait RunMode
  private case object ReconcileOnly                             extends RunMode
  private case class SinceNow(since: Instant)                   extends RunMode
  private case class SinceUntil(since: Instant, until: Instant) extends RunMode

  private case class MembershipAppArgs(slugs: NonEmptyChunk[ClubSlug], flags: Map[String, String])

  private def parseArgs(args: Chunk[String]): Task[MembershipAppArgs] = {
    @tailrec
    def loop(
      remaining: List[String],
      slugs: List[ClubSlug],
      flags: Map[String, String]
    ): Either[String, (List[ClubSlug], Map[String, String])] = remaining match {
      case Nil                                => Right((slugs.reverse, flags))
      case (SINCE | UNTIL) :: value :: rest   => loop(rest, slugs, flags + (remaining.head -> value))
      case (SINCE | UNTIL) :: Nil             => Left(s"${remaining.head} requires a value")
      case flag :: _ if flag.startsWith("--") => Left(s"Unknown flag: $flag")
      case slug :: rest                       => loop(rest, ClubSlug.wrap(slug) :: slugs, flags)
    }
    ZIO.fromEither(loop(args.toList, Nil, Map.empty))
      .mapError(BadRequestException(_))
      .flatMap { case (slugs, flags) =>
        NonEmptyChunk.fromChunk(Chunk.from(slugs)) match {
          case None      => ZIO.fail(BadRequestException(help))
          case Some(nec) => ZIO.succeed(MembershipAppArgs(nec, flags))
        }
      }
  }

  private def parseRunMode(flags: Map[String, String]): Task[RunMode] =
    (flags.get(SINCE), flags.get(UNTIL)) match {
      case (None, None)                     => ZIO.succeed(ReconcileOnly)
      case (None, Some(_))                  => ZIO.fail(BadRequestException("--until requires --since"))
      case (Some(sinceStr), None)           => parseDateArg(sinceStr).map(SinceNow(_))
      case (Some(sinceStr), Some(untilStr)) => parseDateArg(sinceStr).zip(parseDateArg(untilStr)).map(SinceUntil(_, _))
    }

  private def parseDateArg(string: String): IO[BadRequestException, Instant] =
    TimeParser.parseInstantZIO(string).mapError(BadRequestException(_))

  /** The club to report on, reconciled first unless a reconcile that completed, whichever job ran it, already covers
    * `until`. A name no club here has held is reconciled by name, which is how a club never ingested gets its first
    * run; any other name that reaches no club fails with why, since Chess.com has already been asked about it.
    */
  private[membership] def reconcileIfStale(
    clubSlug: ClubSlug,
    until: Instant
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, NamedClub] =
    ZIO.serviceWithZIO[ChessComClient](ClubResolution.resolveAndAdjudicate(_, ClubQuery.BySlug(clubSlug))).flatMap {
      case ClubResolution.NotLocal(_) =>
        reconcile(clubSlug = clubSlug, expectedClubIdOption = None).flatMap(reconciledClub)
      case resolution =>
        ZIO.fromEither(resolution.runnable).mapError(NotFoundException(_)).flatMap { club =>
          MembershipRun.selectLatestCompleted(club.clubId).flatMap {
            case Some(run) if !until.isAfter(run.startedAt) => ZIO.succeed(club)
            case _ =>
              reconcile(clubSlug = club.slug, expectedClubIdOption = Some(club.clubId)).flatMap(reconciledClub)
          }
        }
    }

  // Reconciling persists the club under the name Chess.com answered with, which is the one to report under.
  private def reconciledClub(result: ReconciliationResult): RIO[PostgresClient, NamedClub] =
    ClubName
      .selectCurrentName(result.clubId)
      .someOrFail(NotFoundException(s"Club holds no name: #${ClubId.unwrap(result.clubId)}"))
      .map(NamedClub(result.clubId, _))

  // --- Phase A: Gather data ---

  /** `expectedClubIdOption` guards against `clubSlug` now answering as another club: see
    * [[ClubSlugRenameResolver.fetchExpecting]]. Every reconcile is recorded as a run; another job reconciling as a
    * step of its own passes [[RunTrigger.Dependency]], so Membership's next report still shows what it saw.
    */
  def reconcile(
    clubSlug: ClubSlug,
    expectedClubIdOption: Option[ClubId],
    trustUsernames: Boolean = true,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunIdOption: Option[JobRunId] = None
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, ReconciliationResult] =
    for {
      startedAt <- Clock.instant
      client    <- ZIO.service[ChessComClient]
      resolved  <- ClubSlugRenameResolver.fetchExpecting(client, clubSlug, expectedClubIdOption)
      apiClub = resolved.api
      clubId  = apiClub.clubId
      club    = Club.fromApi(apiClub)
      _     <- Club.upsert(club)
      runId <- MembershipRun.insert(clubId, trigger, startedAt, jobRunIdOption)
      saved <- saveRoster(
        client = client,
        slug = resolved.slug,
        clubId = clubId,
        runId = runId,
        trustUsernames = trustUsernames,
        attempt = 1
      )
      // Active accounts only, like the previous count: a closed account stays a member but is not counted.
      currentMemberCount <- ClubMember.selectClubActive(clubId).map(_.size)
      // Members present in the DB but absent from the previous reported run joined at a reconcile another job ran as
      // a step of its own. Report them so joins aren't silently absorbed.
      phaseBJoinIds   = saved.phaseB.newMemberships.map(_.playerId).toSet
      storedMembers   = saved.storedRoster.membersByPlayerId
      externallyAdded = storedMembers.keySet -- saved.prevMemberIds -- phaseBJoinIds
      externalChanges = Chunk.from(externallyAdded.flatMap { pid =>
        storedMembers.get(pid).map { state =>
          MemberChangeSummary(pid, state.player.username, Chunk(JoinedClub(state.member.since)))
        }
      })
      externalMemberships = Chunk.from(externallyAdded.flatMap(pid => storedMembers.get(pid).map(_.member)))
    } yield mergeResults(
      clubId = clubId,
      b = saved.phaseB,
      c = saved.phaseC,
      settlement = saved.settlement,
      currentMemberCount = currentMemberCount,
      previousMemberCount = saved.previousMemberCount,
      startedAt = startedAt,
      completedAt = saved.completedAt,
      externalChanges = externalChanges,
      externalMemberships = externalMemberships
    )

  // What a saved roster was classified against and implied.
  private final case class SavedRoster(
    storedRoster: StoredRoster,
    prevMemberIds: Set[PlayerId],
    previousMemberCount: Int,
    phaseB: MembershipClassify.PhaseBResult,
    phaseC: MembershipClassify.PhaseCResult,
    settlement: ClosedAccountClubs.Settlement,
    completedAt: Instant
  )

  // Each retry follows a save by another reconcile of the club, so running out means they start faster than one ends.
  private val MaxSaveAttempts = 5

  /** Fetches `clubId`'s roster, classifies it and saves what it implied, starting again from the fetch whenever another
    * reconcile of the club saved after the attempt began. Classified against the rows that save replaced, the roster
    * would repeat that save's changes, and a roster fetched before that save's would undo them (#312).
    */
  private def saveRoster(
    client: ChessComClient,
    slug: ClubSlug,
    clubId: ClubId,
    runId: MembershipRunId,
    trustUsernames: Boolean,
    attempt: Int
  ): RIO[ProgressDisplay & PostgresClient, SavedRoster] =
    attemptSave(client, slug, clubId, runId, trustUsernames).flatMap {
      case Some(saved) => ZIO.succeed(saved)
      case None if attempt < MaxSaveAttempts =>
        ZIO.logInfo("Another reconcile saved this club's roster while this one ran; fetching the roster again") *>
          saveRoster(
            client = client,
            slug = slug,
            clubId = clubId,
            runId = runId,
            trustUsernames = trustUsernames,
            attempt = attempt + 1
          )
      case None =>
        ZIO.fail(
          IllegalStateException(
            s"Another reconcile of club #${ClubId.unwrap(clubId)} saved during each of $MaxSaveAttempts attempts"
          )
        )
    }

  private def attemptSave(
    client: ChessComClient,
    slug: ClubSlug,
    clubId: ClubId,
    runId: MembershipRunId,
    trustUsernames: Boolean
  ): RIO[ProgressDisplay & PostgresClient, Option[SavedRoster]] =
    for {
      // Read before the fetch and the load: a save before this read is in the rows loaded and came from an older
      // roster, and a save after it stops this attempt saving.
      version <- ClubRosterCoverage.selectVersion(clubId)
      (apiMembers, observedAt, storedRoster) <-
        fetchRoster(client, slug, clubId).zipPar(loadStoredRoster(clubId))
      // Read before this run completes, or it would be its own baseline.
      (prevMemberIds, previousMemberCount) <- loadPreviousRoster(clubId)
      apiMap = apiMembers.toMap
      phaseB <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, storedRoster, observedAt, trustUsernames)
      phaseC <- MembershipClassify.classifyDisappeared(client, storedRoster, phaseB.resolvedIds, apiMap, observedAt)
      // Closed accounts neither roster lists: their club lists settle their rows (ADR 0022).
      rostered = storedRoster.membersByPlayerId.keySet ++ phaseB.resolvedIds
      settlement <- ClosedAccountClubs.settle(
        client = client,
        clubId = clubId,
        storedRoster = storedRoster,
        exclude = rostered
      )
      completedAt <- Clock.instant
      saved <- saveUnlessSuperseded(
        clubId = clubId,
        version = version,
        runId = runId,
        observedAt = observedAt,
        completedAt = completedAt,
        b = phaseB,
        c = phaseC,
        settlement = settlement
      )
    } yield Option.when(saved)(
      SavedRoster(storedRoster, prevMemberIds, previousMemberCount, phaseB, phaseC, settlement, completedAt)
    )

  // A run is complete exactly when what its roster implied is saved, so the stale check and the report's baseline
  // agree with the rows and `club_roster_coverage`. Nothing is saved if another save came after `version`.
  private def saveUnlessSuperseded(
    clubId: ClubId,
    version: Option[Long],
    runId: MembershipRunId,
    observedAt: Instant,
    completedAt: Instant,
    b: MembershipClassify.PhaseBResult,
    c: MembershipClassify.PhaseCResult,
    settlement: ClosedAccountClubs.Settlement
  ): RIO[PostgresClient, Boolean] =
    withTransaction {
      for {
        current <- ClubRosterCoverage.lockVersion(clubId)
        saving = current == version
        _ <- ZIO.whenDiscard(saving) {
          persist(clubId, observedAt, b, c, settlement) *> MembershipRun.complete(runId, completedAt)
        }
      } yield saving
    }

  // Shared by the CLI ReconcileOnly path and the server membership job (JobRoutes): reconcile, then emit the
  // reconciliation summary via reportReconciliation so it lands in the per-job log / streams to the CLI. Returns the
  // result and join invitations so the CLI can additionally write its report file (the server discards the tuple — its
  // summary is already logged). Uses result.clubId rather than re-selecting by clubSlug, which would 404 a club whose
  // slug reconcile resolved through a rename. The invitation lookup is best-effort: a DB hiccup there must not flip an
  // already-committed reconcile to a failed job, so it logs and falls back to no invite annotations.
  def reconcileAndReport(
    clubSlug: ClubSlug,
    expectedClubIdOption: Option[ClubId],
    trustUsernames: Boolean,
    trigger: RunTrigger,
    jobRunIdOption: Option[JobRunId]
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, (ReconciliationResult, Map[PlayerId, Instant])] =
    for {
      result <- reconcile(
        clubSlug = clubSlug,
        expectedClubIdOption = expectedClubIdOption,
        trustUsernames = trustUsernames,
        trigger = trigger,
        jobRunIdOption = jobRunIdOption
      )
      invitations <- MembershipReport.lookupJoinInvitations(result.clubId, result.changes.toList)
        .catchAll { e =>
          ZIO
            .logWarning(s"Join-invitation lookup failed for club ${result.clubId}: ${e.safeMessage}")
            .as(Map.empty[PlayerId, Instant])
        }
      _ <- MembershipReport.reportReconciliation(result, invitations)
    } yield (result, invitations)

  // Belt-and-suspenders against a second rename since `fetchExpecting` resolved `slug`. A roster is built for each
  // request, so it describes the moment it arrived (docs/chess-com-api.md, "A member list is built for each request").
  private def fetchRoster(
    client: ChessComClient,
    slug: ClubSlug,
    clubId: ClubId
  ): RIO[PostgresClient, (ApiClubMembers, Instant)] =
    for {
      members <- ApiClubMembers.get(client, slug)
        .withClubSlugRenameRecovery(client, slug, Some(clubId))(fresh => ApiClubMembers.get(client, fresh))
      observedAt <- Clock.instant
    } yield (members, observedAt)

  // The roster names players by what they hold now, so it is matched against `player_name`: a player holding none is
  // reachable only by id, and the display cache could name whoever took its name.
  private[membership] def loadStoredRoster(clubId: ClubId): RIO[PostgresClient, StoredRoster] =
    for {
      listedMembers  <- ClubMember.selectClubListed(clubId)
      allClubMembers <- ClubMember.selectClub(clubId)
      playerIds = allClubMembers.map(_.playerId).distinct
      players      <- Player.selectByIds(playerIds)
      currentNames <- PlayerName.selectCurrentNames(playerIds)
      coverage     <- ClubRosterCoverage.select(clubId)
      clubNames    <- ClubName.selectClub(clubId)
    } yield {
      val playerMap = players.map(p => p.playerId -> p).toMap
      val listed    = listedMembers.toSet
      val states    = listedMembers.flatMap(m => playerMap.get(m.playerId).map(p => MemberState(p, m)))
      StoredRoster(
        membersByPlayerId = states.map(s => s.player.playerId -> s).toMap,
        membersByUsername = states.flatMap(s => currentNames.get(s.player.playerId).map(_ -> s)).toMap,
        unlistedByPlayerId = allClubMembers.filterNot(listed.contains).groupBy(_.playerId),
        lastObservedAt = coverage.map(_.lastObservedAt),
        slugs = clubNames.map(_.slug).toSet,
        knownPlayersByUsername = players.flatMap(p => currentNames.get(p.playerId).map(_ -> p)).toMap
      )
    }

  // The roster as the previous reported run left it: who was in, and how many of them had active accounts.
  private def loadPreviousRoster(clubId: ClubId): RIO[PostgresClient, (Set[PlayerId], Int)] =
    MembershipRun.selectLatestReported(clubId).map(_.flatMap(_.completedAt)).flatMap {
      case Some(at) => ClubMember.selectPlayerIdsCurrentAt(clubId, at) <*> ClubMember.countActiveCurrentAt(clubId, at)
      case None     => ZIO.succeed((Set.empty[PlayerId], 0))
    }

  // --- Merge & Persist ---

  private[membership] def mergeResults(
    clubId: ClubId,
    b: MembershipClassify.PhaseBResult,
    c: MembershipClassify.PhaseCResult,
    settlement: ClosedAccountClubs.Settlement,
    currentMemberCount: Int,
    previousMemberCount: Int,
    startedAt: Instant,
    completedAt: Instant,
    externalChanges: Chunk[MemberChangeSummary] = Chunk.empty,
    externalMemberships: Chunk[ClubMember] = Chunk.empty
  ): ReconciliationResult =
    ReconciliationResult(
      clubId = clubId,
      changes = b.changes ++ c.changes ++ externalChanges,
      newPlayers = b.newPlayers,
      updatedPlayers = b.updatedPlayers ++ c.updatedPlayers,
      archivedSnapshots = b.archivedSnapshots ++ c.archivedSnapshots,
      newMemberships = b.newMemberships ++ externalMemberships,
      closedMemberships = b.closedMemberships ++ c.closedMemberships,
      hiddenMemberships = c.hiddenMemberships,
      reopenedMemberships = b.reopenedMemberships,
      settlement = settlement,
      currentMemberCount = currentMemberCount,
      previousMemberCount = previousMemberCount,
      startedAt = startedAt,
      completedAt = completedAt
    )

  /** What the roster and the club lists implied, `club_roster_coverage` included, for the caller to commit as one. */
  private def persist(
    clubId: ClubId,
    observedAt: Instant,
    b: MembershipClassify.PhaseBResult,
    c: MembershipClassify.PhaseCResult,
    settlement: ClosedAccountClubs.Settlement
  ): RIO[PostgresClient, Unit] = {
    val allUpdated  = b.updatedPlayers ++ c.updatedPlayers
    val allArchived = b.archivedSnapshots ++ c.archivedSnapshots
    val narrowed    = b.closedMemberships ++ c.closedMemberships ++ c.hiddenMemberships
    val widened     = b.reopenedMemberships ++ settlement.hidden.map(_.row)
    val opened      = b.newMemberships ++ settlement.opened.map(_.row)
    for {
      _ <- Player.writeBatch(inserted = b.newPlayers, updated = allUpdated)
      _ <- ZIO.whenDiscard(allArchived.nonEmpty)(PlayerSnapshot.insertBatch(allArchived))
      // Closes first: a rejoin opens a row the old one would overlap while still open.
      _ <- ZIO.whenDiscard(narrowed.nonEmpty)(ClubMember.updateBatch(narrowed))
      _ <- ZIO.whenDiscard(widened.nonEmpty)(ClubMember.updateBatch(widened))
      _ <- ZIO.whenDiscard(opened.nonEmpty)(ClubMember.insertBatch(opened))
      _ <- ClubRosterCoverage.observe(clubId, observedAt)
    } yield ()
  }
}
