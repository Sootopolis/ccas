package ccas.analysis.apps.membership

import java.time.Instant
import scala.annotation.nowarn

import com.augustnagro.magnum.Transactor
import zio.{Chunk, RIO, Scope, Task, UIO, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerClubs}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.OutputFile

object MembershipApp extends ZIOAppDefault {

  private sealed trait RunMode
  private case object ReconcileOnly                             extends RunMode
  private case class SinceNow(since: Instant)                   extends RunMode
  private case class SinceUntil(since: Instant, until: Instant) extends RunMode

  private def parseRunMode(args: Chunk[String]): Task[RunMode] =
    args.lift(1) match {
      case None => ZIO.succeed(ReconcileOnly)
      case Some(sinceStr) =>
        ZIO.attempt(Instant.parse(sinceStr))
          .orElseFail(ExternalException(s"Invalid date format: $sinceStr"))
          .flatMap { since =>
            args.lift(2) match {
              case None => ZIO.succeed(SinceNow(since))
              case Some(untilStr) =>
                ZIO.attempt(Instant.parse(untilStr))
                  .mapBoth(_ => ExternalException(s"Invalid date format: $untilStr"), SinceUntil(since, _))
            }
          }
    }

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      clubName <- args.headOption match {
        case None    => ZIO.fail(ExternalException("Usage: MembershipApp <club-url-name> [since [until]]"))
        case Some(s) => ZIO.succeed(ClubUrlName.wrap(s))
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
      ChessComClient.live(),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  private def reconcileIfStale(clubUrlName: ClubUrlName, until: Instant): RIO[ChessComClient & Transactor, Unit] =
    for {
      clubOpt <- Club.selectByUrlName(clubUrlName)
      _ <- ZIO.fromOption(clubOpt).flatMap { club =>
        MembershipRun.selectLatest(club.clubId).flatMap {
          case Some(run) if !until.isAfter(run.ranAt) => ZIO.unit
          case _                                      => reconcile(clubUrlName).unit
        }
      }.orElse(reconcile(clubUrlName).unit)
    } yield ()

  // --- Phase A: Gather data ---

  def reconcile(
    clubUrlName: ClubUrlName,
    trustUsernames: Boolean = true,
    trackRun: Boolean = true
  ): RIO[ChessComClient & Transactor, ReconciliationResult] =
    for {
      client <- ZIO.service[ChessComClient]
      (apiClub, resolvedUrlName) <- withNameFallback(
        clubUrlName,
        name => ApiClub.get(client, name),
        resolveClubUrlName(client, _)
      )
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), resolvedUrlName)
      _                     <- Club.upsert(club)
      (apiMembers, dbState) <- ApiClubMembers.get(client, resolvedUrlName).zipPar(buildDbState(clubId))
      apiMap = apiMembers.toMap
      now    = Instant.now()
      phaseB <- classifyApiMembers(client, clubId, apiMap, dbState, now, trustUsernames)
      phaseC <- classifyDisappeared(client, dbState, phaseB.resolvedIds, apiMap, resolvedUrlName, now)
      result = mergeResults(phaseB, phaseC).copy(
        currentMemberCount = apiMap.size,
        previousMemberCount = dbState.membersByPlayerId.size
      )
      _ <- persist(result)
      _ <- ZIO.whenDiscard(trackRun)(MembershipRun.insert(clubId, now))
    } yield result

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

  private[membership] def classifyApiMembers(
    client: ChessComClient,
    clubId: ClubId,
    apiMap: Map[Username, Long],
    dbState: DbState,
    now: Instant,
    trustUsernames: Boolean = true
  ): RIO[Transactor, PhaseBResult] = {
    val initial = PhaseBResult(Set.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)
    ZIO.foldLeft(apiMap.toList)(initial) { case (acc, (username, joinedEpoch)) =>
      val since = Instant.ofEpochSecond(joinedEpoch)
      dbState.membersByUsername.get(username) match {
        case Some(state) if state.member.sinceApproximate =>
          // Replace approximate with authoritative timestamp
          ClubMember.replaceSince(clubId, state.player.playerId, state.member.since, since)
            .as(acc.copy(resolvedIds = acc.resolvedIds + state.player.playerId))

        case Some(state) if state.member.since == since =>
          // Unchanged member
          ZIO.succeed(acc.copy(resolvedIds = acc.resolvedIds + state.player.playerId))

        case Some(state) =>
          // Rejoin: different `since` timestamp
          val closedMember = state.member.copy(until = Some(now))
          val newMember    = ClubMember(clubId, state.player.playerId, since, None, sinceApproximate = false)
          val change = MemberChangeSummary(state.player.playerId, username, Chunk(Rejoined(now, state.member.since)))
          ZIO.succeed(
            acc.copy(
              resolvedIds = acc.resolvedIds + state.player.playerId,
              changes = acc.changes :+ change,
              newMemberships = acc.newMemberships :+ newMember,
              closedMemberships = acc.closedMemberships :+ closedMember
            )
          )

        case None =>
          // Unknown by username — check trusted snapshots first, then fall back to API
          if (!trustUsernames) { fetchAndClassifyNewMember(client, clubId, username, since, dbState, acc, now) }
          else {
            dbState.knownPlayersByUsername.get(username) match {
              case None => fetchAndClassifyNewMember(client, clubId, username, since, dbState, acc, now)
              case Some(snapshot) =>
                val playerId = snapshot.playerId
                dbState.membersByPlayerId.get(playerId) match {
                  case Some(state) =>
                    // Username change detected via trusted snapshot
                    val change =
                      MemberChangeSummary(playerId, username, Chunk(UsernameChange(now, state.player.username)))
                    val newSnapshot = PlayerSnapshot(playerId, now, username, snapshot.status, snapshot.title)
                    ZIO.succeed(
                      acc.copy(
                        resolvedIds = acc.resolvedIds + playerId,
                        changes = acc.changes :+ change,
                        newSnapshots = acc.newSnapshots :+ newSnapshot
                      )
                    )
                  case None =>
                    // Known player joined this club
                    val newMember = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
                    val change    = MemberChangeSummary(playerId, username, Chunk(JoinedClub(now)))
                    ZIO.succeed(
                      acc.copy(
                        resolvedIds = acc.resolvedIds + playerId,
                        changes = acc.changes :+ change,
                        newMemberships = acc.newMemberships :+ newMember
                      )
                    )
                }
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
    acc: PhaseBResult,
    now: Instant
  ): RIO[Transactor, PhaseBResult] =
    client.get[ApiPlayer](ApiPlayer.getUrl(username)).flatMap { apiPlayer =>
      val playerId       = apiPlayer.playerId
      val statusCategory = apiPlayer.status.category

      dbState.membersByPlayerId.get(playerId) match {
        case Some(state) =>
          // Username change: same player ID, different username
          val (snapshots, changes) = snapshotChanges(state, username, statusCategory, apiPlayer.title, playerId, now)
          val summary              = MemberChangeSummary(playerId, username, changes)
          ZIO.succeed(
            acc.copy(
              resolvedIds = acc.resolvedIds + playerId,
              changes = acc.changes :+ summary,
              newSnapshots = acc.newSnapshots ++ snapshots
            )
          )

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
                acc.copy(
                  resolvedIds = acc.resolvedIds + playerId,
                  changes = acc.changes :+ summary,
                  newSnapshots = acc.newSnapshots ++ snapshotChunks.result(),
                  newMemberships = acc.newMemberships :+ newMember
                )
              }

            case None =>
              // Brand new player
              val player   = Player(playerId, Instant.ofEpochSecond(apiPlayer.joined))
              val snapshot = PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
              val member   = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
              val summary  = MemberChangeSummary(playerId, username, Chunk(NewMember(now)))
              ZIO.succeed(
                acc.copy(
                  resolvedIds = acc.resolvedIds + playerId,
                  changes = acc.changes :+ summary,
                  newPlayers = acc.newPlayers :+ player,
                  newSnapshots = acc.newSnapshots :+ snapshot,
                  newMemberships = acc.newMemberships :+ member
                )
              )
          }
      }
    }

  // --- Phase C: Classify disappeared members ---

  private[membership] final case class PhaseCResult(
    changes: Chunk[MemberChangeSummary],
    newSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  private[membership] def classifyDisappeared(
    client: ChessComClient,
    dbState: DbState,
    resolvedIds: Set[PlayerId],
    apiMap: Map[Username, Long],
    clubUrlName: ClubUrlName,
    now: Instant
  ): RIO[Transactor, PhaseCResult] = {
    val disappeared = dbState.membersByPlayerId.values.filterNot(s => resolvedIds.contains(s.player.playerId))
    val initial     = PhaseCResult(Chunk.empty, Chunk.empty, Chunk.empty)

    ZIO.foldLeft(disappeared)(initial) { case (acc, state) =>
      val playerId     = state.player.playerId
      val oldUsername  = state.player.username
      val closedMember = state.member.copy(until = Some(now))

      client.get[ApiPlayer](ApiPlayer.getUrl(oldUsername)).foldZIO(
        _ => matchRefFallback(client, acc, state, closedMember, apiMap, clubUrlName, now),
        apiPlayer =>
          if (apiPlayer.playerId != playerId) {
            matchRefFallback(client, acc, state, closedMember, apiMap, clubUrlName, now)
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
              ZIO.succeed(
                acc.copy(
                  changes = acc.changes :+ summary,
                  newSnapshots = acc.newSnapshots ++ snapshots,
                  closedMemberships = acc.closedMemberships :+ closedMember
                )
              )
            } else {
              checkClubMembership(client, clubUrlName, apiPlayer.username).map { stillMember =>
                acc.copy(
                  changes = acc.changes :+ summary,
                  newSnapshots = acc.newSnapshots ++ snapshots,
                  closedMemberships = if (stillMember) { acc.closedMemberships }
                  else { acc.closedMemberships :+ closedMember }
                )
              }
            }
          }
      )
    }
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
    clubUrlName: ClubUrlName,
    username: Username
  ): Task[Boolean] =
    client.get[ApiPlayerClubs](ApiPlayerClubs.getUrl(username)).map { playerClubs =>
      playerClubs.clubs.exists(_.clubName == clubUrlName)
    }.catchAll(_ => ZIO.succeed(false))

  private def matchRefFallback(
    client: ChessComClient,
    acc: PhaseCResult,
    state: MemberState,
    closedMember: ClubMember,
    apiMap: Map[Username, Long],
    clubUrlName: ClubUrlName,
    now: Instant
  ): RIO[Transactor, PhaseCResult] = {
    val playerId    = state.player.playerId
    val oldUsername = state.player.username

    def unresolvable: PhaseCResult = acc.copy(
      changes = acc.changes :+ MemberChangeSummary(playerId, oldUsername, Chunk(Unresolvable(now, oldUsername))),
      closedMemberships = acc.closedMemberships :+ closedMember
    )

    PlayerMatchRef.selectId(playerId).flatMap {
      case None => ZIO.succeed(unresolvable)
      case Some(ref) =>
        resolveUsernameFromMatchRef(client, ref, oldUsername).flatMap {
          case None => ZIO.succeed(unresolvable)
          case Some(resolvedUsername) =>
            def onProfileFetchFailed: UIO[PhaseCResult] = {
              val snapshot = PlayerSnapshot(playerId, now, resolvedUsername, state.player.status, state.player.title)
              val changes  = Chunk(UsernameChange(now, oldUsername))
              ZIO.succeed(
                acc.copy(
                  changes = acc.changes :+ MemberChangeSummary(playerId, resolvedUsername, changes),
                  newSnapshots = acc.newSnapshots :+ snapshot,
                  closedMemberships = acc.closedMemberships :+ closedMember
                )
              )
            }

            def onProfileResolved(resolvedProfile: ApiPlayer): Task[PhaseCResult] = {
              val statusCategory = resolvedProfile.status.category
              val snapshot = PlayerSnapshot(playerId, now, resolvedUsername, statusCategory, resolvedProfile.title)
              val changes = Chunk(UsernameChange(now, oldUsername)) ++
                Option.when(state.player.status != statusCategory)(StatusChange(now, state.player.status))

              if (statusCategory == PlayerStatusCategory.Active) {
                val hasLeft = apiMap.contains(resolvedUsername)
                val leftOpt = Option.unless(hasLeft)(LeftClub(now))
                ZIO.succeed(
                  acc.copy(
                    changes = acc.changes :+ MemberChangeSummary(playerId, resolvedUsername, changes ++ leftOpt),
                    newSnapshots = acc.newSnapshots :+ snapshot,
                    closedMemberships = acc.closedMemberships ++ Option.when(hasLeft)(closedMember)
                  )
                )
              } else {
                checkClubMembership(client, clubUrlName, resolvedUsername).map { stillMember =>
                  val allChanges = changes :+ AccountClosed(now, statusCategory)
                  acc.copy(
                    changes = acc.changes :+ MemberChangeSummary(playerId, resolvedUsername, allChanges),
                    newSnapshots = acc.newSnapshots :+ snapshot,
                    closedMemberships = acc.closedMemberships ++ Option.unless(stillMember)(closedMember)
                  )
                }
              }
            }

            client.get[ApiPlayer](ApiPlayer.getUrl(resolvedUsername))
              .foldZIO(_ => onProfileFetchFailed, onProfileResolved)
        }
    }
  }

  private def resolveUsernameFromMatchRef(
    client: ChessComClient,
    ref: PlayerMatchRef,
    oldUsername: Username
  ): Task[Option[Username]] =
    client.get[ApiDailyMatch](ApiDailyMatch.getUrl(ref.matchId)).map { dailyMatch =>
      val team = if (ref.teamIdx == 1) { dailyMatch.teams.team1 }
      else { dailyMatch.teams.team2 }
      val boardSuffix = s"/${ref.boardIdx}"
      team.players.collectFirst {
        case p: ApiDailyMatch.ApiDailyMatchPlayerStarted if p.board.path.toString.endsWith(boardSuffix) => p.username
      }.filter(_ != oldUsername)
    }.catchAll(_ => ZIO.none)

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

  private def resolveClubUrlName(
    client: ChessComClient,
    oldUrlName: ClubUrlName
  ): RIO[Transactor, Option[ClubUrlName]] =
    (for {
      clubOpt <- Club.selectByUrlName(oldUrlName)
      refOpt  <- ZIO.foreach(clubOpt)(club => ClubMatchRef.selectId(club.clubId)).map(_.flatten)
      result <- ZIO.foreach(refOpt) { ref =>
        client.get[ApiDailyMatch](ApiDailyMatch.getUrl(ref.matchId)).map { dailyMatch =>
          val team = if (ref.teamIdx == 1) { dailyMatch.teams.team1 }
          else { dailyMatch.teams.team2 }
          team.`@id`.path.segments.lastOption.map(ClubUrlName.wrap).filter(_ != oldUrlName)
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

  private[membership] def mergeResults(b: PhaseBResult, c: PhaseCResult): ReconciliationResult =
    ReconciliationResult(
      changes = b.changes ++ c.changes,
      newPlayers = b.newPlayers,
      newSnapshots = b.newSnapshots ++ c.newSnapshots,
      newMemberships = b.newMemberships,
      closedMemberships = b.closedMemberships ++ c.closedMemberships
    )

  private def persist(result: ReconciliationResult): RIO[Transactor, Unit] =
    for {
      _ <- ZIO.whenDiscard(result.newPlayers.nonEmpty)(Player.insertBatch(result.newPlayers))
      _ <- ZIO.whenDiscard(result.newSnapshots.nonEmpty)(PlayerSnapshot.insertBatch(result.newSnapshots))
      _ <- ZIO.whenDiscard(result.newMemberships.nonEmpty)(ClubMember.insertBatch(result.newMemberships))
      _ <- ZIO.whenDiscard(result.closedMemberships.nonEmpty)(ClubMember.updateBatch(result.closedMemberships))
    } yield ()

  // --- Reporting ---

  private def reportReconciliation(result: ReconciliationResult): UIO[Unit] = {
    val delta = result.currentMemberCount - result.previousMemberCount
    val sign  = if (delta >= 0) "+" else ""
    for {
      _ <- ZIO.logInfo(s"=== Reconciliation Complete ===")
      _ <- ZIO.logInfo(s"Total members:      ${result.currentMemberCount} ($sign$delta)")
      _ <- ZIO.logInfo(s"New players:        ${result.newPlayers.size}")
      _ <- ZIO.logInfo(s"New snapshots:      ${result.newSnapshots.size}")
      _ <- ZIO.logInfo(s"New memberships:    ${result.newMemberships.size}")
      _ <- ZIO.logInfo(s"Closed memberships: ${result.closedMemberships.size}")
      _ <- ZIO.logInfo("")
      _ <- ZIO.foreachDiscard(result.changes)(printChangeSummary)
    } yield ()
  }

  private def printChangeSummary(summary: MemberChangeSummary): UIO[Unit] =
    for {
      _ <- ZIO.logInfo(s"${summary.username}:")
      _ <- ZIO.foreachDiscard(summary.changes)(change => ZIO.logInfo(s"  ${formatChange(change)}"))
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
    val delta = result.currentMemberCount - result.previousMemberCount
    val sign  = if (delta >= 0) "+" else ""
    val header = s"""=== Reconciliation Complete ===
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

  private def report(clubUrlName: ClubUrlName, since: Instant, until: Instant): RIO[Transactor, ReportResult] =
    for {
      club <- Club.selectByUrlName(clubUrlName)
        .someOrFail(ExternalException(s"Club '$clubUrlName' not found in database"))
      clubId = club.clubId
      members <- ClubMember.selectClub(clubId)
      snaps   <- PlayerSnapshot.selectSince(since)
      summaries    = classifyFromDb(clubId, members, snaps, since, until)
      countAtStart = members.count(m => !m.since.isAfter(since) && m.until.forall(_.isAfter(since)))
      countAtEnd   = members.count(m => !m.since.isAfter(until) && m.until.forall(_.isAfter(until)))
      _ <- ZIO.logInfo(s"=== Report for $clubUrlName from $since to $until ===")
      _ <- ZIO.logInfo(s"Members: $countAtStart -> $countAtEnd")
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
