package ccas.analysis.apps.membership

import java.time.Instant
import scala.annotation.tailrec

import com.augustnagro.magnum.Transactor
import zio.{Chunk, NonEmptyChunk, RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.{CcasLogger, OutputFile}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.SqlZioTypes.withTransaction

object MembershipApp extends ZIOAppDefault {
  private val help = "Usage: MembershipApp <club-slug> [club-slug ...] [--since <date>] [--until <date>]"

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args           <- ZIOAppArgs.getArgs
      (slugs, flags) <- parseArgs(args)
      mode           <- parseRunMode(flags)
      _ <- ZIO.foreachDiscard(slugs) { clubName =>
        mode match {
          case ReconcileOnly =>
            reconcile(clubName).flatMap { result =>
              MembershipReport.reportReconciliation(result) *>
                OutputFile.writeAndLog("membership", clubName, MembershipReport.formatReconciliation(result))
            }
          case SinceNow(since) =>
            reconcile(clubName) *> MembershipReport.report(clubName, since, Instant.now()).flatMap { rr =>
              OutputFile.writeAndLog("membership", clubName, MembershipReport.formatReport(rr))
            }
          case SinceUntil(since, until) =>
            reconcileIfStale(clubName, until) *> MembershipReport.report(clubName, since, until).flatMap { rr =>
              OutputFile.writeAndLog("membership", clubName, MembershipReport.formatReport(rr))
            }
        }
      }
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = true),
      ChessComClient.live("membership"),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
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
      case ("--since" | "--until") :: value :: rest => loop(rest, slugs, flags + (remaining.head -> value))
      case ("--since" | "--until") :: Nil           => Left(s"${remaining.head} requires a value")
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
    (flags.get("--since"), flags.get("--until")) match {
      case (None, None) => ZIO.succeed(ReconcileOnly)
      case (None, Some(_)) =>
        ZIO.fail(BadRequestException("--until requires --since"))
      case (Some(sinceStr), None) =>
        ZIO.attempt(Instant.parse(sinceStr))
          .mapError(_ => BadRequestException(s"Invalid date format: $sinceStr"))
          .map(SinceNow(_))
      case (Some(sinceStr), Some(untilStr)) =>
        ZIO.attempt(Instant.parse(sinceStr))
          .mapError(_ => BadRequestException(s"Invalid date format: $sinceStr"))
          .flatMap { since =>
            ZIO.attempt(Instant.parse(untilStr))
              .mapBoth(_ => BadRequestException(s"Invalid date format: $untilStr"), SinceUntil(since, _))
          }
    }

  private def reconcileIfStale(
    clubSlug: ClubSlug,
    until: Instant
  ): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      clubOpt <- Club.selectBySlug(clubSlug)
      _ <- ZIO.fromOption(clubOpt).flatMap { club =>
        MembershipRun.selectLatest(club.clubId).flatMap {
          case Some(run) if !until.isAfter(run.startedAt) => ZIO.unit
          case _                                          => reconcile(clubSlug).unit
        }
      }.orElse(reconcile(clubSlug).unit)
    } yield ()

  // --- Phase A: Gather data ---

  def reconcile(
    clubSlug: ClubSlug,
    trustUsernames: Boolean = true,
    trackRun: Boolean = true,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunId: Option[String] = None
  ): RIO[CcasLogger & ChessComClient & Transactor, ReconciliationResult] =
    for {
      startedAt <- ZIO.succeed(Instant.now())
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

  private[membership] def buildDbState(clubId: ClubId): RIO[Transactor, DbState] =
    for {
      players <- Player.selectAll
      members <- ClubMember.selectClubCurrent(clubId)
    } yield {
      val playerMap = players.map(p => p.playerId -> p).toMap
      val states    = members.flatMap(m => playerMap.get(m.playerId).map(p => MemberState(p, m)))
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
  ): RIO[Transactor, Unit] = {
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
    resolve: Name => RIO[Transactor, Option[Name]]
  ): RIO[Transactor, (T, Name)] = effect(name).map(_ -> name).catchAll { originalError =>
    resolve(name).flatMap {
      case None          => ZIO.fail(originalError)
      case Some(newName) => effect(newName).map(_ -> newName)
    }
  }

  private def resolveClubSlug(
    client: ChessComClient,
    oldUrlName: ClubSlug
  ): RIO[Transactor, Option[ClubSlug]] =
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
