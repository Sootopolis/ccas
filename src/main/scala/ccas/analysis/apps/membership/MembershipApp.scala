package ccas.analysis.apps.membership

import java.time.Instant
import scala.annotation.tailrec

import zio.{Chunk, Clock, IO, NonEmptyChunk, RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.utils.{CcasLogger, OutputFile, TimeParser}
import ccas.utils.client.ChessComClient
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
      args           <- ZIOAppArgs.getArgs
      (slugs, flags) <- parseArgs(args)
      mode           <- parseRunMode(flags)
      _ <- ZIO.foreachDiscard(slugs) { clubName =>
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
      CcasLogger.live(showProgress = true),
      ChessComClient.live(MEMBERSHIP),
      Client.default,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  private sealed trait RunMode
  private case object ReconcileOnly                             extends RunMode
  private case class SinceNow(since: Instant)                   extends RunMode
  private case class SinceUntil(since: Instant, until: Instant) extends RunMode

  private def parseArgs(args: Chunk[String]): Task[(NonEmptyChunk[ClubSlug], Map[String, String])] = {
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
          case Some(nec) => ZIO.succeed((nec, flags))
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
    ZIO.fromEither(TimeParser.parseInstant(string)).mapError(BadRequestException(_))

  private def reconcileIfStale(
    clubSlug: ClubSlug,
    until: Instant
  ): RIO[CcasLogger & ChessComClient & PostgresClient, Unit] =
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
  ): RIO[CcasLogger & ChessComClient & PostgresClient, ReconciliationResult] =
    for {
      startedAt <- Clock.instant
      client    <- ZIO.service[ChessComClient]
      (apiClub, resolvedUrlName) <- withNameFallback(
        clubSlug,
        name => ApiClub.get(client, name),
        resolveClubSlug(client, _)
      )
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), resolvedUrlName, apiClub.name)
      _                     <- Club.upsertResolvingSlugConflict(club, client)
      runId                 <- ZIO.when(trackRun)(MembershipRun.insert(clubId, trigger, startedAt, jobRunId))
      (apiMembers, dbState) <- ApiClubMembers.get(client, resolvedUrlName).zipPar(buildDbState(clubId))
      apiMap = apiMembers.toMap
      now    = Instant.now()
      phaseB <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, now, trustUsernames)
      phaseC <- MembershipClassify.classifyDisappeared(
        client,
        dbState,
        phaseB.resolvedIds,
        apiMap,
        resolvedUrlName,
        now
      )
      _ <- persist(phaseB, phaseC)
      completedAt = Instant.now()
      _ <- ZIO.foreachDiscard(runId)(id => MembershipRun.complete(id, completedAt))
    } yield mergeResults(phaseB, phaseC, apiMap.size, dbState.membersByPlayerId.size, startedAt, completedAt)

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

  // --- Merge & Persist ---

  private[membership] def mergeResults(
    b: MembershipClassify.PhaseBResult,
    c: MembershipClassify.PhaseCResult,
    currentMemberCount: Int,
    previousMemberCount: Int,
    startedAt: Instant,
    completedAt: Instant
  ): ReconciliationResult =
    ReconciliationResult(
      changes = b.changes ++ c.changes,
      newPlayers = b.newPlayers,
      updatedPlayers = b.updatedPlayers ++ c.updatedPlayers,
      archivedSnapshots = b.archivedSnapshots ++ c.archivedSnapshots,
      newMemberships = b.newMemberships,
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

  // --- Helpers ---

  private def withNameFallback[Name, T](
    name: Name,
    effect: Name => Task[T],
    resolve: Name => RIO[PostgresClient, Option[Name]]
  ): RIO[PostgresClient, (T, Name)] = effect(name).map(_ -> name).catchAll { originalError =>
    resolve(name).flatMap {
      case None          => ZIO.fail(originalError)
      case Some(newName) => effect(newName).map(_ -> newName)
    }
  }

  private def resolveClubSlug(client: ChessComClient, oldUrlName: ClubSlug): RIO[PostgresClient, Option[ClubSlug]] =
    (for {
      clubOpt <- Club.selectBySlug(oldUrlName)
      refOpt  <- ZIO.foreach(clubOpt)(club => ClubMatchRef.selectId(club.clubId)).map(_.flatten)
      result <- ZIO.foreach(refOpt) { ref =>
        ccas.analysis.apps.ref.RefHelpers.fetchTeamMatchTeams(client, ref.matchId, ref.isLive).map { teams =>
          val team = if (ref.isTeam1) { teams.team1 }
          else { teams.team2 }
          team.`@id`.path.segments.lastOption.map(ClubSlug.wrap).filter(_ != oldUrlName)
        }
      }.map(_.flatten)
    } yield result).catchAll(_ => ZIO.none)
}
