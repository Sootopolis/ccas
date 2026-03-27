package ccas.analysis.apps.membership

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, TeamMatchPlayerStarted, TeamMatchTeams}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerClubs}
import ccas.api.tournament.ApiTournament
import ccas.utils.{CcasLogger, OutputFile}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.DataSourceLayer
import com.augustnagro.magnum.Transactor
import zio.http.Client
import zio.{Chunk, RIO, Ref, Scope, Task, UIO, URIO, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.time.{Instant, Duration as JDuration}
import scala.annotation.nowarn

object MembershipApp extends ZIOAppDefault {
  private val help = "Usage: MembershipApp <club-slug> [since [until]]"

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      clubName <- args.headOption match {
        case None    => ZIO.fail(BadRequestException(help))
        case Some(s) => ZIO.succeed(ClubSlug.wrap(s))
      }
      mode <- parseRunMode(args)
      _ <- mode match {
        case ReconcileOnly =>
          reconcile(clubName).flatMap { result =>
            reportReconciliation(result) *>
              OutputFile.writeAndLog("membership", clubName, formatReconciliation(result))
          }
        case SinceNow(since) =>
          reconcile(clubName) *> report(clubName, since, Instant.now()).flatMap { rr =>
            OutputFile.writeAndLog("membership", clubName, formatReport(rr))
          }
        case SinceUntil(since, until) =>
          reconcileIfStale(clubName, until) *> report(clubName, since, until).flatMap { rr =>
            OutputFile.writeAndLog("membership", clubName, formatReport(rr))
          }
      }
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = true),
      ChessComClient.live,
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  private sealed trait RunMode
  private case object ReconcileOnly                             extends RunMode
  private case class SinceNow(since: Instant)                   extends RunMode
  private case class SinceUntil(since: Instant, until: Instant) extends RunMode

  private def parseRunMode(args: Chunk[String]): Task[RunMode] =
    args.lift(1) match {
      case None => ZIO.succeed(ReconcileOnly)
      case Some(sinceStr) =>
        ZIO.attempt(Instant.parse(sinceStr))
          .orElseFail(BadRequestException(s"Invalid date format: $sinceStr"))
          .flatMap { since =>
            args.lift(2) match {
              case None => ZIO.succeed(SinceNow(since))
              case Some(untilStr) =>
                ZIO.attempt(Instant.parse(untilStr))
                  .mapBoth(_ => BadRequestException(s"Invalid date format: $untilStr"), SinceUntil(since, _))
            }
          }
    }

  private def reconcileIfStale(clubSlug: ClubSlug, until: Instant): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      clubOpt <- Club.selectBySlug(clubSlug)
      _ <- ZIO.fromOption(clubOpt).flatMap { club =>
        MembershipRun.selectLatest(club.clubId).flatMap {
          case Some(run) if !until.isAfter(run.startedAt) => ZIO.unit
          case _                                      => reconcile(clubSlug).unit
        }
      }.orElse(reconcile(clubSlug).unit)
    } yield ()

  // --- Phase A: Gather data ---

  def reconcile(
    clubSlug: ClubSlug,
    trustUsernames: Boolean = true,
    trackRun: Boolean = true,
    trigger: RunTrigger = RunTrigger.Cli
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
      _                     <- Club.upsert(club)
      runId                 <- ZIO.when(trackRun)(MembershipRun.insert(clubId, trigger, startedAt))
      (apiMembers, dbState) <- ApiClubMembers.get(client, resolvedUrlName).zipPar(buildDbState(clubId))
      apiMap = apiMembers.toMap
      now    = Instant.now()
      phaseB      <- classifyApiMembers(client, clubId, apiMap, dbState, now, trustUsernames)
      phaseC      <- classifyDisappeared(client, dbState, phaseB.resolvedIds, apiMap, resolvedUrlName, now)
      _           <- persist(phaseB, phaseC)
      completedAt = Instant.now()
      _           <- ZIO.foreachDiscard(runId)(id => MembershipRun.complete(id, completedAt))
    } yield mergeResults(phaseB, phaseC, apiMap.size, dbState.membersByPlayerId.size, startedAt, completedAt)

  private[membership] def buildDbState(clubId: ClubId): RIO[Transactor, DbState] =
    for {
      snapshots <- PlayerSnapshot.selectLatest
      members   <- ClubMember.selectClubCurrent(clubId)
    } yield {
      val snapshotMap = snapshots.map(s => s.playerId -> s).toMap
      val states      = members.flatMap(m => snapshotMap.get(m.playerId).map(s => MemberState(s, m)))
      DbState(
        membersByPlayerId = states.map(s => s.player.playerId -> s).toMap,
        membersByUsername = states.map(s => s.player.username -> s).toMap,
        knownPlayersByUsername = snapshots.map(s => s.username -> s).toMap
      )
    }

  // --- Phase B: Classify API members ---

  private[membership] final case class PhaseBResult(
    resolvedIds: Set[PlayerId],
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    newSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember]
  )

  private case class PhaseBMemberResult(
    resolvedId: PlayerId,
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    newSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember]
  )

  private[membership] def classifyApiMembers(
    client: ChessComClient,
    clubId: ClubId,
    apiMap: Map[Username, Long],
    dbState: DbState,
    now: Instant,
    trustUsernames: Boolean = true
  ): RIO[CcasLogger & Transactor, PhaseBResult] = {
    val total = apiMap.size
    ZIO.scoped {
      for {
        bar     <- CcasLogger.progressBar
        counter <- Ref.make(0)
        results <- ZIO.foreachPar(Chunk.from(apiMap)) { case (username, joinedEpoch) =>
          classifyOneMember(client, clubId, username, joinedEpoch, dbState, now, trustUsernames)
            .tap(_ => counter.updateAndGet(_ + 1).flatMap(n =>
              bar.print(n, total, s"  Classifying API members: $n/$total")
            ))
        }
      } yield PhaseBResult(
        resolvedIds = results.map(_.resolvedId).toSet,
        changes = results.flatMap(_.changes),
        newPlayers = results.flatMap(_.newPlayers),
        newSnapshots = results.flatMap(_.newSnapshots),
        newMemberships = results.flatMap(_.newMemberships),
        closedMemberships = results.flatMap(_.closedMemberships)
      )
    }
  }

  private def classifyOneMember(
    client: ChessComClient,
    clubId: ClubId,
    username: Username,
    joinedEpoch: Long,
    dbState: DbState,
    now: Instant,
    trustUsernames: Boolean
  ): RIO[Transactor, PhaseBMemberResult] = {
    val since = Instant.ofEpochSecond(joinedEpoch)
    def resolved(playerId: PlayerId) =
      PhaseBMemberResult(playerId, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)

    dbState.membersByUsername.get(username) match {
      case Some(state) if state.member.sinceApproximate =>
        // Replace approximate with authoritative timestamp
        ClubMember.replaceSince(clubId, state.player.playerId, state.member.since, since)
          .as(resolved(state.player.playerId))

      case Some(state) if state.member.since == since =>
        // Unchanged member
        ZIO.succeed(resolved(state.player.playerId))

      case Some(state) =>
        // Rejoin: different `since` timestamp
        val closedMember = state.member.copy(until = Some(now))
        val newMember    = ClubMember(clubId, state.player.playerId, since, None, sinceApproximate = false)
        val change = MemberChangeSummary(state.player.playerId, username, Chunk(Rejoined(now, state.member.since)))
        ZIO.succeed(PhaseBMemberResult(
          state.player.playerId, Chunk(change), Chunk.empty, Chunk.empty, Chunk(newMember), Chunk(closedMember)
        ))

      case None =>
        // Unknown by username — check trusted snapshots first, then fall back to API
        if (!trustUsernames) { fetchAndClassifyNewMember(client, clubId, username, since, dbState, now) }
        else {
          dbState.knownPlayersByUsername.get(username) match {
            case None => fetchAndClassifyNewMember(client, clubId, username, since, dbState, now)
            case Some(snapshot) =>
              val playerId = snapshot.playerId
              dbState.membersByPlayerId.get(playerId) match {
                case Some(state) =>
                  // Username change detected via trusted snapshot
                  val change =
                    MemberChangeSummary(playerId, username, Chunk(UsernameChange(now, state.player.username)))
                  val newSnapshot = PlayerSnapshot(playerId, now, username, snapshot.status, snapshot.title)
                  ZIO.succeed(PhaseBMemberResult(
                    playerId, Chunk(change), Chunk.empty, Chunk(newSnapshot), Chunk.empty, Chunk.empty
                  ))
                case None =>
                  // Known player joined this club
                  val newMember = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
                  val change    = MemberChangeSummary(playerId, username, Chunk(JoinedClub(now)))
                  ZIO.succeed(PhaseBMemberResult(
                    playerId, Chunk(change), Chunk.empty, Chunk.empty, Chunk(newMember), Chunk.empty
                  ))
              }
          }
        }
    }
  }

  private def fetchAndClassifyNewMember(
    client: ChessComClient,
    clubId: ClubId,
    username: Username,
    since: Instant,
    dbState: DbState,
    now: Instant
  ): RIO[Transactor, PhaseBMemberResult] =
    client.get[ApiPlayer](ApiPlayer.getUrl(username)).flatMap { apiPlayer =>
      val playerId       = apiPlayer.playerId
      val statusCategory = apiPlayer.status.category

      dbState.membersByPlayerId.get(playerId) match {
        case Some(state) =>
          // Username change: same player ID, different username
          val (snapshots, changes) = snapshotChanges(state, username, statusCategory, apiPlayer.title, playerId, now)
          val summary              = MemberChangeSummary(playerId, username, changes)
          ZIO.succeed(PhaseBMemberResult(playerId, Chunk(summary), Chunk.empty, snapshots, Chunk.empty, Chunk.empty))

        case None =>
          // Check if player exists in DB at all
          Player.selectId(playerId).flatMap {
            case Some(_) =>
              // Player exists but not current club member — joined club
              val newMember      = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
              val snapshotChunks = Chunk.newBuilder[PlayerSnapshot]
              val changeChunks   = Chunk.newBuilder[MemberChange]

              changeChunks += JoinedClub(now)

              // Check if snapshot needs updating
              PlayerSnapshot.selectIdLatest(playerId).map { latestOpt =>
                val needsSnapshot = latestOpt.forall(l =>
                  l.username != username || l.status != statusCategory || l.title != apiPlayer.title
                )
                if (needsSnapshot) {
                  snapshotChunks += PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
                  latestOpt.foreach { latest =>
                    if (latest.username != username) { changeChunks += UsernameChange(now, latest.username) }
                    if (latest.status != statusCategory) { changeChunks += StatusChange(now, latest.status) }
                  }
                }

                val summary = MemberChangeSummary(playerId, username, changeChunks.result())
                PhaseBMemberResult(
                  playerId, Chunk(summary), Chunk.empty, snapshotChunks.result(), Chunk(newMember), Chunk.empty
                )
              }

            case None =>
              // Brand new player
              val player   = Player(playerId, Instant.ofEpochSecond(apiPlayer.joined))
              val snapshot = PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
              val member   = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
              val summary  = MemberChangeSummary(playerId, username, Chunk(NewMember(now)))
              ZIO.succeed(PhaseBMemberResult(
                playerId, Chunk(summary), Chunk(player), Chunk(snapshot), Chunk(member), Chunk.empty
              ))
          }
      }
    }

  // --- Phase C: Classify disappeared members ---

  private[membership] final case class PhaseCResult(
    changes: Chunk[MemberChangeSummary],
    newSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  private case class PhaseCMemberResult(
    changes: Chunk[MemberChangeSummary],
    newSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  private[membership] def classifyDisappeared(
    client: ChessComClient,
    dbState: DbState,
    resolvedIds: Set[PlayerId],
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: Instant
  ): RIO[CcasLogger & Transactor, PhaseCResult] = {
    val disappearedList = dbState.membersByPlayerId.values.filterNot(s => resolvedIds.contains(s.player.playerId)).toList
    val total           = disappearedList.size

    ZIO.scoped {
      for {
        bar     <- CcasLogger.progressBar
        counter <- Ref.make(0)
        results <- ZIO.foreachPar(Chunk.from(disappearedList)) { state =>
          classifyOneDisappeared(client, state, apiMap, clubSlug, now)
            .tap(_ => counter.updateAndGet(_ + 1).flatMap(n =>
              bar.print(n, total, s"  Classifying disappeared members: $n/$total")
            ))
        }
      } yield PhaseCResult(
        changes = results.flatMap(_.changes),
        newSnapshots = results.flatMap(_.newSnapshots),
        closedMemberships = results.flatMap(_.closedMemberships)
      )
    }
  }

  private def classifyOneDisappeared(
    client: ChessComClient,
    state: MemberState,
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: Instant
  ): RIO[Transactor, PhaseCMemberResult] = {
    val playerId     = state.player.playerId
    val oldUsername  = state.player.username
    val closedMember = state.member.copy(until = Some(now))

    client.get[ApiPlayer](ApiPlayer.getUrl(oldUsername)).foldZIO(
      _ => matchRefFallback(client, state, closedMember, apiMap, clubSlug, now),
      apiPlayer =>
        if (apiPlayer.playerId != playerId) {
          matchRefFallback(client, state, closedMember, apiMap, clubSlug, now)
        } else {
          val statusCategory = apiPlayer.status.category
          val (snapshots, extraChanges) =
            snapshotChanges(state, apiPlayer.username, statusCategory, apiPlayer.title, playerId, now)
          val primaryChange =
            if (statusCategory == PlayerStatusCategory.Active) { Chunk(LeftClub(now)) }
            else { Chunk(AccountClosed(now, statusCategory)) }
          val allChanges = primaryChange ++ extraChanges
          val summary    = MemberChangeSummary(playerId, apiPlayer.username, allChanges)

          if (statusCategory == PlayerStatusCategory.Active) {
            ZIO.succeed(PhaseCMemberResult(Chunk(summary), snapshots, Chunk(closedMember)))
          } else {
            checkClubMembership(client, clubSlug, apiPlayer.username).map { stillMember =>
              PhaseCMemberResult(
                Chunk(summary), snapshots,
                if (stillMember) { Chunk.empty } else { Chunk(closedMember) }
              )
            }
          }
        }
    )
  }

  private def snapshotChanges(
    state: MemberState,
    username: Username,
    statusCategory: PlayerStatusCategory,
    title: Option[ccas.api.misc.enums.Title],
    playerId: PlayerId,
    now: Instant
  ): (Chunk[PlayerSnapshot], Chunk[MemberChange]) =
    if (state.player.status != statusCategory || state.player.username != username || state.player.title != title) {
      val snapshot = PlayerSnapshot(playerId, now, username, statusCategory, title)
      val changes  = Chunk.newBuilder[MemberChange]
      if (state.player.status != statusCategory) { changes += StatusChange(now, state.player.status) }
      if (state.player.username != username) { changes += UsernameChange(now, state.player.username) }
      (Chunk(snapshot), changes.result())
    } else { (Chunk.empty, Chunk.empty) }

  private def checkClubMembership(
    client: ChessComClient,
    clubSlug: ClubSlug,
    username: Username
  ): Task[Boolean] =
    client.get[ApiPlayerClubs](ApiPlayerClubs.getUrl(username)).map { playerClubs =>
      playerClubs.clubs.exists(_.clubName == clubSlug)
    }.catchAll(_ => ZIO.succeed(false))

  private def matchRefFallback(
    client: ChessComClient,
    state: MemberState,
    closedMember: ClubMember,
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: Instant
  ): RIO[Transactor, PhaseCMemberResult] = {
    val playerId    = state.player.playerId
    val oldUsername = state.player.username

    def unresolvable: PhaseCMemberResult = PhaseCMemberResult(
      Chunk(MemberChangeSummary(playerId, oldUsername, Chunk(Unresolvable(now, oldUsername)))),
      Chunk.empty,
      Chunk(closedMember)
    )

    PlayerMatchRef.selectId(playerId).flatMap {
      case Some(ref) => resolveUsernameFromMatchRef(client, ref, oldUsername)
      case None =>
        PlayerTournamentRef.selectId(playerId).flatMap {
          case None      => ZIO.none
          case Some(ref) => resolveUsernameFromTournamentRef(client, ref, oldUsername)
        }
    }.flatMap {
      case None => ZIO.succeed(unresolvable)
      case Some(resolvedUsername) =>
        def onProfileFetchFailed: UIO[PhaseCMemberResult] = {
          val snapshot = PlayerSnapshot(playerId, now, resolvedUsername, state.player.status, state.player.title)
          val changes  = Chunk(UsernameChange(now, oldUsername))
          ZIO.succeed(PhaseCMemberResult(
            Chunk(MemberChangeSummary(playerId, resolvedUsername, changes)),
            Chunk(snapshot),
            Chunk(closedMember)
          ))
        }

        def onProfileResolved(resolvedProfile: ApiPlayer): Task[PhaseCMemberResult] = {
          val statusCategory = resolvedProfile.status.category
          val snapshot = PlayerSnapshot(playerId, now, resolvedUsername, statusCategory, resolvedProfile.title)
          val changes = Chunk(UsernameChange(now, oldUsername)) ++
            Option.when(state.player.status != statusCategory)(StatusChange(now, state.player.status))

          if (statusCategory == PlayerStatusCategory.Active) {
            val hasLeft = apiMap.contains(resolvedUsername)
            val leftOpt = Option.unless(hasLeft)(LeftClub(now))
            ZIO.succeed(PhaseCMemberResult(
              Chunk(MemberChangeSummary(playerId, resolvedUsername, changes ++ leftOpt)),
              Chunk(snapshot),
              Chunk.fromIterable(Option.when(hasLeft)(closedMember))
            ))
          } else {
            checkClubMembership(client, clubSlug, resolvedUsername).map { stillMember =>
              val allChanges = changes :+ AccountClosed(now, statusCategory)
              PhaseCMemberResult(
                Chunk(MemberChangeSummary(playerId, resolvedUsername, allChanges)),
                Chunk(snapshot),
                if (stillMember) { Chunk.empty } else { Chunk(closedMember) }
              )
            }
          }
        }

        client.get[ApiPlayer](ApiPlayer.getUrl(resolvedUsername))
          .foldZIO(_ => onProfileFetchFailed, onProfileResolved)
    }
  }

  private def resolveUsernameFromMatchRef(
    client: ChessComClient,
    ref: PlayerMatchRef,
    oldUsername: Username
  ): Task[Option[Username]] =
    fetchTeamMatchTeams(client, ref.matchId, ref.isLive).map { teams =>
      val team = if (ref.isTeam1) { teams.team1 }
      else { teams.team2 }
      val boardSuffix = s"/${ref.boardIdx}"
      team.players.collectFirst {
        case p: TeamMatchPlayerStarted if p.board.path.toString.endsWith(boardSuffix) => p.username
      }.filter(_ != oldUsername)
    }.catchAll(_ => ZIO.none)

  private def resolveUsernameFromTournamentRef(
    client: ChessComClient,
    ref: PlayerTournamentRef,
    oldUsername: Username
  ): Task[Option[Username]] =
    client.get[ApiTournament](ApiTournament.getUrl(ref.tournamentSlug)).map { tournament =>
      tournament.players.lift(ref.playerIdx).map(_.username).filter(_ != oldUsername)
    }.catchAll(_ => ZIO.none)

  private def fetchTeamMatchTeams(
    client: ChessComClient,
    matchId: ClubMatchId,
    isLive: Boolean
  ): Task[TeamMatchTeams] =
    if (isLive) { client.get[ApiLiveMatch](ApiLiveMatch.getUrl(matchId)).map(_.teams) }
    else { client.get[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).map(_.teams) }

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
        fetchTeamMatchTeams(client, ref.matchId, ref.isLive).map { teams =>
          val team = if (ref.isTeam1) { teams.team1 }
          else { teams.team2 }
          team.`@id`.path.segments.lastOption.map(ClubSlug.wrap).filter(_ != oldUrlName)
        }
      }.map(_.flatten)
    } yield result).catchAll(_ => ZIO.none)

  @nowarn("msg=unused")
  private def resolvePlayerUsername(
    client: ChessComClient,
    oldUsername: Username
  ): RIO[Transactor, Option[Username]] =
    for {
      snapOpt <- PlayerSnapshot.selectNameLatest(oldUsername)
      refOpt  <- ZIO.foreach(snapOpt)(snap => PlayerMatchRef.selectId(snap.playerId)).map(_.flatten)
      result  <- ZIO.foreach(refOpt)(ref => resolveUsernameFromMatchRef(client, ref, oldUsername)).map(_.flatten)
    } yield result

  // --- Merge & Persist ---

  private[membership] def mergeResults(
    b: PhaseBResult,
    c: PhaseCResult,
    currentMemberCount: Int,
    previousMemberCount: Int,
    startedAt: Instant,
    completedAt: Instant
  ): ReconciliationResult =
    ReconciliationResult(
      changes = b.changes ++ c.changes,
      newPlayers = b.newPlayers,
      newSnapshots = b.newSnapshots ++ c.newSnapshots,
      newMemberships = b.newMemberships,
      closedMemberships = b.closedMemberships ++ c.closedMemberships,
      currentMemberCount = currentMemberCount,
      previousMemberCount = previousMemberCount,
      startedAt = startedAt,
      completedAt = completedAt
    )

  private def persist(b: PhaseBResult, c: PhaseCResult): RIO[Transactor, Unit] =
    for {
      _ <- ZIO.whenDiscard(b.newPlayers.nonEmpty)(Player.insertBatch(b.newPlayers))
      _ <- ZIO.collectAllParDiscard(List(
        ZIO.whenDiscard((b.newSnapshots ++ c.newSnapshots).nonEmpty)(
          PlayerSnapshot.insertBatch(b.newSnapshots ++ c.newSnapshots)
        ),
        ZIO.whenDiscard(b.newMemberships.nonEmpty)(ClubMember.insertBatch(b.newMemberships)),
        ZIO.whenDiscard((b.closedMemberships ++ c.closedMemberships).nonEmpty)(
          ClubMember.updateBatch(b.closedMemberships ++ c.closedMemberships)
        )
      ))
    } yield ()

  // --- Reporting ---

  private def reportReconciliation(result: ReconciliationResult): URIO[CcasLogger, Unit] = {
    val delta    = result.currentMemberCount - result.previousMemberCount
    val sign     = if (delta >= 0) "+" else ""
    val duration = JDuration.between(result.startedAt, result.completedAt)
    for {
      _ <- CcasLogger.info(s"=== Reconciliation Complete ===")
      _ <- CcasLogger.info(s"Duration:           ${duration.toMinutes}m ${duration.toSecondsPart}s")
      _ <- CcasLogger.info(s"Total members:      ${result.currentMemberCount} ($sign$delta)")
      _ <- CcasLogger.info(s"New players:        ${result.newPlayers.size}")
      _ <- CcasLogger.info(s"New snapshots:      ${result.newSnapshots.size}")
      _ <- CcasLogger.info(s"New memberships:    ${result.newMemberships.size}")
      _ <- CcasLogger.info(s"Closed memberships: ${result.closedMemberships.size}")
      _ <- CcasLogger.info("")
      _ <- ZIO.foreachDiscard(result.changes)(printChangeSummary)
    } yield ()
  }

  private def printChangeSummary(summary: MemberChangeSummary): URIO[CcasLogger, Unit] =
    for {
      _ <- CcasLogger.info(s"${summary.username}:")
      _ <- ZIO.foreachDiscard(summary.changes)(change => CcasLogger.info(s"  ${formatChange(change)}"))
    } yield ()

  private def formatChange(change: MemberChange): String = change match {
    case NewMember(ts)                 => s"[NEW MEMBER] at $ts"
    case JoinedClub(ts)                => s"[JOINED CLUB] at $ts"
    case LeftClub(ts)                  => s"[LEFT CLUB] at $ts"
    case AccountClosed(ts, status)     => s"[ACCOUNT CLOSED] at $ts — status: $status"
    case Rejoined(ts, prevUntil)       => s"[REJOINED] at $ts — previously left at $prevUntil"
    case Unresolvable(ts, oldUsername) => s"[UNRESOLVABLE] at $ts — old username: $oldUsername"
    case UsernameChange(ts, oldName)   => s"[USERNAME CHANGE] at $ts — was: $oldName"
    case StatusChange(ts, oldStatus)   => s"[STATUS CHANGE] at $ts — was: $oldStatus"
  }

  // --- File output formatting ---

  private def formatReconciliation(result: ReconciliationResult): String = {
    val duration = JDuration.between(result.startedAt, result.completedAt)
    val delta    = result.currentMemberCount - result.previousMemberCount
    val sign     = if (delta >= 0) "+" else ""
    val header = s"""Started:   ${result.startedAt}
                    |Completed: ${result.completedAt}
                    |Duration:  ${duration.toMinutes}m ${duration.toSecondsPart}s
                    |
                    |=== Reconciliation Complete ===
                    |Total members:      ${result.currentMemberCount} ($sign$delta)
                    |New players:        ${result.newPlayers.size}
                    |New snapshots:      ${result.newSnapshots.size}
                    |New memberships:    ${result.newMemberships.size}
                    |Closed memberships: ${result.closedMemberships.size}
                    |""".stripMargin
    header + "\n" + formatChangeSummaries(result.changes.toList)
  }

  private def formatReport(rr: ReportResult): String = {
    val delta  = rr.memberCountAtEnd - rr.memberCountAtStart
    val sign   = if (delta >= 0) "+" else ""
    val header = s"Total members: ${rr.memberCountAtEnd} ($sign$delta)\n\n"
    if (rr.summaries.isEmpty) { header + "No changes\n" }
    else { header + formatChangeSummaries(rr.summaries) }
  }

  private def formatChangeSummaries(summaries: List[MemberChangeSummary]): String = {
    val sb = new StringBuilder
    summaries.foreach { summary =>
      sb.append(s"${summary.username}:\n")
      summary.changes.foreach(change => sb.append(s"  ${formatChange(change)}\n"))
    }
    sb.toString
  }

  // --- Report mode: DB-only ---

  private case class ReportResult(
    summaries: List[MemberChangeSummary],
    memberCountAtStart: Int,
    memberCountAtEnd: Int
  )

  private def report(clubSlug: ClubSlug, since: Instant, until: Instant): RIO[CcasLogger & Transactor, ReportResult] =
    for {
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(NotFoundException(s"Club '$clubSlug' not found in database"))
      clubId = club.clubId
      members <- ClubMember.selectClub(clubId)
      snaps   <- PlayerSnapshot.selectSince(since)
      summaries    = classifyFromDb(clubId, members, snaps, since, until)
      countAtStart = members.count(m => !m.since.isAfter(since) && m.until.forall(_.isAfter(since)))
      countAtEnd   = members.count(m => !m.since.isAfter(until) && m.until.forall(_.isAfter(until)))
      _ <- CcasLogger.info(s"=== Report for $clubSlug from $since to $until ===")
      _ <- CcasLogger.info(s"Members: $countAtStart -> $countAtEnd")
      _ <- ZIO.foreachDiscard(summaries)(printChangeSummary)
    } yield ReportResult(summaries, countAtStart, countAtEnd)

  private[membership] def classifyFromDb(
    clubId: ClubId,
    members: List[ClubMember],
    snaps: List[PlayerSnapshot],
    since: Instant,
    until: Instant
  ): List[MemberChangeSummary] = {
    val snapsByPlayer = snaps.groupBy(_.playerId)

    // Find membership changes in the time range
    val changedMembers = members.filter { m =>
      (m.since.compareTo(since) >= 0 && m.since.compareTo(until) <= 0) ||
      m.until.exists(u => u.compareTo(since) >= 0 && u.compareTo(until) <= 0)
    }

    // Group by player
    val membersByPlayer = changedMembers.groupBy(_.playerId)

    membersByPlayer.toList.map { case (playerId, cms) =>
      val playerSnaps = snapsByPlayer.getOrElse(playerId, Nil).sortBy(_.since)
      val changes     = Chunk.newBuilder[MemberChange]

      cms.foreach { cm =>
        // New membership in range
        if (cm.since.compareTo(since) >= 0 && cm.since.compareTo(until) <= 0) {
          // Check if there's a prior membership for same club+player
          val priorMemberships = members
            .filter(m => m.clubId == clubId && m.playerId == playerId && m.until.isDefined && m.since != cm.since)
          if (priorMemberships.nonEmpty) {
            val latestPrior = priorMemberships.maxBy(_.since)
            changes += Rejoined(cm.since, latestPrior.until.getOrElse(latestPrior.since))
          } else {
            // Check if player has snapshots before this membership — existing player joining club
            val priorSnaps = playerSnaps.filter(_.since.isBefore(cm.since))
            if (priorSnaps.nonEmpty) { changes += JoinedClub(cm.since) }
            else { changes += NewMember(cm.since) }
          }
        }

        // Closed membership in range
        cm.until.foreach { u =>
          if (u.compareTo(since) >= 0 && u.compareTo(until) <= 0) {
            // Check latest snapshot to determine reason
            val latestSnap = playerSnaps.filter(s => !s.since.isAfter(u)).lastOption
            latestSnap match {
              case Some(snap) if snap.status != PlayerStatusCategory.Active => changes += AccountClosed(u, snap.status)
              case Some(_)                                                  => changes += LeftClub(u)
              case None                                                     =>
                // No snapshot found near the closure — unresolvable
                val username = playerSnaps.headOption.fold(Username.wrap("unknown"))(_.username)
                changes += Unresolvable(u, username)
            }
          }
        }
      }

      // Detect username and status changes from snapshots in range
      val snapsInRange = playerSnaps.filter(s => s.since.compareTo(since) >= 0 && s.since.compareTo(until) <= 0)
      snapsInRange.foreach { snap =>
        val previousSnap = playerSnaps.filter(_.since.isBefore(snap.since)).lastOption
        previousSnap.foreach { prev =>
          if (prev.username != snap.username) { changes += UsernameChange(snap.since, prev.username) }
          if (prev.status != snap.status) { changes += StatusChange(snap.since, prev.status) }
        }
      }

      val latestUsername = playerSnaps.lastOption.fold(Username.wrap("unknown"))(_.username)
      MemberChangeSummary(playerId, latestUsername, changes.result())
    }.filter(_.changes.nonEmpty)
  }
}
