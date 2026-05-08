package ccas.analysis.apps.membership

import java.time.Instant
import scala.annotation.tailrec

import zio.{Chunk, Clock, IO, NonEmptyChunk, RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.analysis.apps.{ClubSlugRenameResolver, withClubSlugRenameRecovery}
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.JoinedClub
import ccas.analysis.tables.*
import ccas.api.club.ApiClubMembers
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId, PlayerId}
import ccas.utils.{OutputFile, ProgressDisplay, TimeParser}
import ccas.utils.client.{ChessComClient, HttpClientLayer}
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

object MembershipApp extends ZIOAppDefault {
  private val SINCE = "--since"
  private val UNTIL = "--until"
  private val help = s"Usage: MembershipApp <club-slug> [club-slug ...] [$SINCE <date>] [$UNTIL <date>]"
  private val MEMBERSHIP = "membership"

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args   <- ZIOAppArgs.getArgs
      parsed <- parseArgs(args)
      mode   <- parseRunMode(parsed.flags)
      _ <- ZIO.foreachDiscard(parsed.slugs) { clubName =>
        mode match {
          case ReconcileOnly =>
            reconcile(clubName).flatMap { result =>
              Club.selectBySlug(clubName)
                .someOrFail(new IllegalStateException(s"Club '$clubName' not found after reconcile"))
                .flatMap(club => MembershipReport.lookupJoinInvitations(club.clubId, result.changes.toList))
                .flatMap { invitations =>
                  MembershipReport.reportReconciliation(result, invitations) *>
                    OutputFile.writeAndLog(MEMBERSHIP, clubName, MembershipReport.formatReconciliation(result, invitations))
                }
            }
          case SinceNow(since) =>
            reconcile(clubName) *> MembershipReport.report(clubName, since, Instant.now()).flatMap { rr =>
              OutputFile.writeAndLog(MEMBERSHIP, clubName, MembershipReport.formatReport(rr))
            }
          case SinceUntil(since, until) =>
            reconcileIfStale(clubName, until) *> MembershipReport.report(clubName, since, until).flatMap { rr =>
              OutputFile.writeAndLog(MEMBERSHIP, clubName, MembershipReport.formatReport(rr))
            }
        }
      }
    } yield ()).provideSomeAuto(
      ProgressDisplay.live(showProgress = true),
      ChessComClient.live(MEMBERSHIP),
      HttpClientLayer.live,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

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
      case Nil                                      => Right((slugs.reverse, flags))
      case (SINCE | UNTIL) :: value :: rest => loop(rest, slugs, flags + (remaining.head -> value))
      case (SINCE | UNTIL) :: Nil           => Left(s"${remaining.head} requires a value")
      case flag :: _ if flag.startsWith("--")       => Left(s"Unknown flag: $flag")
      case slug :: rest                             => loop(rest, ClubSlug.wrap(slug) :: slugs, flags)
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
      case (None, None) => ZIO.succeed(ReconcileOnly)
      case (None, Some(_)) => ZIO.fail(BadRequestException("--until requires --since"))
      case (Some(sinceStr), None) => parseDateArg(sinceStr).map(SinceNow(_))
      case (Some(sinceStr), Some(untilStr)) => parseDateArg(sinceStr).zip(parseDateArg(untilStr)).map(SinceUntil(_, _))
    }

  private def parseDateArg(string: String): IO[BadRequestException, Instant] =
    TimeParser.parseInstantZIO(string).mapError(BadRequestException(_))

  private def reconcileIfStale(
    clubSlug: ClubSlug,
    until: Instant
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, Unit] =
    Club.selectBySlug(clubSlug).flatMap {
      case Some(club) =>
        MembershipRun.selectLatest(club.clubId).flatMap {
          case Some(run) if !until.isAfter(run.startedAt) => ZIO.unit
          case _                                          => reconcile(clubSlug).unit
        }
      case None => reconcile(clubSlug).unit
    }

  // --- Phase A: Gather data ---

  def reconcile(
    clubSlug: ClubSlug,
    trustUsernames: Boolean = true,
    trackRun: Boolean = true,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunId: Option[JobRunId] = None
  ): RIO[ProgressDisplay & ChessComClient & PostgresClient, ReconciliationResult] =
    for {
      startedAt <- Clock.instant
      client    <- ZIO.service[ChessComClient]
      (apiClub, resolvedSlug) <- ClubSlugRenameResolver.fetchOrRecover(client, clubSlug)
      clubId = apiClub.clubId
      club   = Club.fromApi(apiClub, resolvedSlug)
      _                     <- Club.upsertResolvingSlugConflict(club, client)
      runId <- ZIO.when(trackRun)(MembershipRun.insert(clubId, trigger, startedAt, jobRunId))
      // Wrap belt-and-suspenders against a second rename between the `ApiClub.get` recovery above and now.
      (apiMembers, dbState) <- ApiClubMembers.get(client, resolvedSlug)
        .withClubSlugRenameRecovery(client, resolvedSlug, Some(clubId))(fresh => ApiClubMembers.get(client, fresh))
        .zipPar(buildDbState(clubId))
      prevMemberIds <- loadPreviousMemberIds(clubId)
      // Both counts filter on player.status=Active to exclude Closed-but-still-member phantoms.
      // previousMemberCount MUST be sampled before persist/complete, or selectLatestCompleted returns THIS run.
      previousMemberCount <- loadPreviousActiveMemberCount(clubId)
      apiMap = apiMembers.toMap
      now    = Instant.now()
      phaseB <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, now, trustUsernames)
      phaseC <- MembershipClassify.classifyDisappeared(client, dbState, phaseB.resolvedIds, apiMap, resolvedSlug, now)
      _ <- persist(phaseB, phaseC)
      completedAt = Instant.now()
      _ <- ZIO.foreachDiscard(runId)(id => MembershipRun.complete(id, completedAt))
      currentMemberCount <- ClubMember.selectClubActive(clubId).map(_.size)
      // Members present in the DB but absent from the previous membership run were added externally
      // (e.g. by HistoryApp). Report them so joins aren't silently absorbed.
      phaseBJoinIds = phaseB.newMemberships.map(_.playerId).toSet
      externallyAdded = dbState.membersByPlayerId.keySet -- prevMemberIds -- phaseBJoinIds
      externalChanges = Chunk.from(externallyAdded.flatMap { pid =>
        dbState.membersByPlayerId.get(pid).map { state =>
          MemberChangeSummary(pid, state.player.username, Chunk(JoinedClub(state.member.since)))
        }
      })
      externalMemberships = Chunk.from(externallyAdded.flatMap { pid =>
        dbState.membersByPlayerId.get(pid).map(_.member)
      })
    } yield mergeResults(
      phaseB, phaseC, currentMemberCount, previousMemberCount, startedAt, completedAt,
      externalChanges, externalMemberships
    )

  private[membership] def buildDbState(clubId: ClubId): RIO[PostgresClient, DbState] =
    for {
      currentMembers <- ClubMember.selectClubCurrent(clubId)
      allClubMembers <- ClubMember.selectClub(clubId)
      players        <- Player.selectByIds(allClubMembers.map(_.playerId).distinct)
    } yield {
      val playerMap = players.map(p => p.playerId -> p).toMap
      val states    = currentMembers.flatMap(m => playerMap.get(m.playerId).map(p => MemberState(p, m)))
      DbState(
        membersByPlayerId = states.map(s => s.player.playerId -> s).toMap,
        membersByUsername = states.map(s => s.player.username -> s).toMap,
        knownPlayersByUsername = players.map(p => p.username -> p).toMap
      )
    }

  private def loadPreviousMemberIds(clubId: ClubId): RIO[PostgresClient, Set[PlayerId]] =
    MembershipRun.selectLatestCompleted(clubId).flatMap {
      case Some(run) if run.completedAt.isDefined =>
        ClubMember.selectPlayerIdsCurrentAt(clubId, run.completedAt.get)
      case _ => ZIO.succeed(Set.empty)
    }

  private def loadPreviousActiveMemberCount(clubId: ClubId): RIO[PostgresClient, Int] =
    MembershipRun.selectLatestCompleted(clubId).flatMap {
      case Some(run) if run.completedAt.isDefined =>
        ClubMember.countActiveCurrentAt(clubId, run.completedAt.get)
      case _ => ZIO.succeed(0)
    }

  // --- Merge & Persist ---

  private[membership] def mergeResults(
    b: MembershipClassify.PhaseBResult,
    c: MembershipClassify.PhaseCResult,
    currentMemberCount: Int,
    previousMemberCount: Int,
    startedAt: Instant,
    completedAt: Instant,
    externalChanges: Chunk[MemberChangeSummary] = Chunk.empty,
    externalMemberships: Chunk[ClubMember] = Chunk.empty
  ): ReconciliationResult =
    ReconciliationResult(
      changes = b.changes ++ c.changes ++ externalChanges,
      newPlayers = b.newPlayers,
      updatedPlayers = b.updatedPlayers ++ c.updatedPlayers,
      archivedSnapshots = b.archivedSnapshots ++ c.archivedSnapshots,
      newMemberships = b.newMemberships ++ externalMemberships,
      closedMemberships = b.closedMemberships ++ c.closedMemberships,
      currentMemberCount = currentMemberCount,
      previousMemberCount = previousMemberCount,
      startedAt = startedAt,
      completedAt = completedAt
    )

  private def persist(
    b: MembershipClassify.PhaseBResult,
    c: MembershipClassify.PhaseCResult
  ): RIO[PostgresClient, Unit] = {
    val allUpdated      = b.updatedPlayers ++ c.updatedPlayers
    val allArchived     = b.archivedSnapshots ++ c.archivedSnapshots
    val allClosedMships = b.closedMemberships ++ c.closedMemberships
    // Username swaps within the batch are handled by the DEFERRABLE INITIALLY DEFERRED
    // constraint, which checks uniqueness at commit time rather than per-statement.
    withTransaction {
      for {
        _ <- ZIO.whenDiscard(b.newPlayers.nonEmpty)(Player.insertBatch(b.newPlayers))
        _ <- ZIO.whenDiscard(allArchived.nonEmpty)(PlayerSnapshot.insertBatch(allArchived))
        _ <- ZIO.whenDiscard(allUpdated.nonEmpty)(Player.updateCurrentStateBatch(allUpdated))
        _ <- ZIO.whenDiscard(b.newMemberships.nonEmpty)(ClubMember.insertBatch(b.newMemberships))
        _ <- ZIO.whenDiscard(allClosedMships.nonEmpty)(ClubMember.updateBatch(allClosedMships))
      } yield ()
    }
  }

}
