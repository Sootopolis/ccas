package ccas.analysis.apps.membership

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.{Chunk, Console, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.{Club, ClubMember, Player, PlayerSnapshot}
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer

object MembershipApp extends ZIOAppDefault {

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    for {
      args <- ZIOAppArgs.getArgs
      clubName <- ZIO.fromOption(args.headOption).map(ClubUrlName.wrap)
        .orElseFail(ExternalException("Usage: MembershipApp <club-url-name> [since until]"))
      _ <- (args.lift(1) match
        case Some(sinceStr) =>
          ZIO.attempt(Instant.parse(sinceStr)).mapError(_ => ExternalException(s"Invalid date format: $sinceStr"))
            .flatMap { since =>
              ZIO.attempt(args.lift(2).map(Instant.parse).getOrElse(Instant.now()))
                .mapError(_ => ExternalException(s"Invalid date format: ${args.lift(2).get}"))
                .flatMap(until => report(clubName, since, until))
            }
        case None =>
          reconcile(clubName).flatMap(result => reportReconciliation(result))
      ).provide(
        ChessComClient.live(),
        Client.default,
        DataSourceLayer.liveFromPrefix()
      )
    } yield ()

  // --- Phase A: Gather data ---

  private def reconcile(clubUrlName: ClubUrlName): ZIO[ChessComClient & Transactor, Throwable, ReconciliationResult] =
    for {
      client  <- ZIO.service[ChessComClient]
      apiClub <- ApiClub.get(client, clubUrlName)
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), clubUrlName)
      _    <- Club.upsert(club)
      pair <- ApiClubMembers.get(client, clubUrlName).zipPar(buildDbState(clubId))
      (apiMembers, dbState) = pair
      apiMap                = apiMembers.toMap
      now                   = Instant.now()
      phaseB <- classifyApiMembers(client, clubId, apiMap, dbState, now)
      phaseC <- classifyDisappeared(client, dbState, phaseB.resolvedIds, now)
      result = mergeResults(phaseB, phaseC)
      _ <- persist(result)
    } yield result

  private[membership] def buildDbState(clubId: ClubId): ZIO[Transactor, Throwable, DbState] =
    for {
      snapshots <- PlayerSnapshot.selectLatest
      members   <- ClubMember.selectClubCurrent(clubId)
    } yield {
      val snapshotMap = snapshots.map(s => s.playerId -> s).toMap
      val states = members.flatMap { m =>
        snapshotMap.get(m.playerId).map(s => MemberState(s, m))
      }
      DbState(
        membersByPlayerId = states.map(s => s.player.playerId -> s).toMap,
        membersByUsername = states.map(s => s.player.username -> s).toMap
      )
    }

  // --- Phase B: Classify API members ---

  private[membership] final case class PhaseBResult(
      resolvedIds: Set[PlayerId],
      changes: Chunk[MemberChangeSummary],
      newPlayers: Chunk[Player],
      newSnapshots: Chunk[PlayerSnapshot],
      newMemberships: Chunk[ClubMember],
      closedMemberships: Chunk[ClubMember])

  private[membership] def classifyApiMembers(
      client: ChessComClient,
      clubId: ClubId,
      apiMap: Map[Username, Long],
      dbState: DbState,
      now: Instant
    ): ZIO[Transactor, Throwable, PhaseBResult] = {
    val initial = PhaseBResult(Set.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)
    ZIO
      .foldLeft(apiMap.toList)(initial) { case (acc, (username, joinedEpoch)) =>
        val since = Instant.ofEpochSecond(joinedEpoch)
        dbState.membersByUsername.get(username) match {
          case Some(state) if state.member.since == since =>
            // Unchanged member
            ZIO.succeed(acc.copy(resolvedIds = acc.resolvedIds + state.player.playerId))

          case Some(state) =>
            // Rejoin: different `since` timestamp
            val closedMember = state.member.copy(until = Some(now))
            val newMember    = ClubMember(clubId, state.player.playerId, since, None)
            val change       = MemberChangeSummary(state.player.playerId, Chunk(Rejoined(now, state.member.since)))
            ZIO
              .succeed(
                acc.copy(
                  resolvedIds = acc.resolvedIds + state.player.playerId,
                  changes = acc.changes :+ change,
                  newMemberships = acc.newMemberships :+ newMember,
                  closedMemberships = acc.closedMemberships :+ closedMember
                )
              )

          case None =>
            // Unknown by username — fetch from API
            client.getWithPermit[ApiPlayer](ApiPlayer.getUrl(username)).flatMap { apiPlayer =>
              val playerId       = apiPlayer.playerId
              val statusCategory = apiPlayer.status.category

              dbState.membersByPlayerId.get(playerId) match {
                case Some(state) =>
                  // Username change: same player ID, different username
                  val changeChunks   = Chunk.newBuilder[MemberChange]
                  val snapshotChunks = Chunk.newBuilder[PlayerSnapshot]

                  changeChunks += UsernameChange(now, state.player.username)
                  val newSnapshot = PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
                  snapshotChunks += newSnapshot

                  // Also detect status/title changes
                  if state.player.status != statusCategory then changeChunks += StatusChange(now, state.player.status)

                  val summary = MemberChangeSummary(playerId, changeChunks.result())
                  ZIO
                    .succeed(
                      acc.copy(
                        resolvedIds = acc.resolvedIds + playerId,
                        changes = acc.changes :+ summary,
                        newSnapshots = acc.newSnapshots ++ snapshotChunks.result()
                      )
                    )

                case None =>
                  // Check if player exists in DB at all
                  Player.selectId(playerId).flatMap {
                    case Some(_) =>
                      // Player exists but not current club member — joined club
                      val newMember      = ClubMember(clubId, playerId, since, None)
                      val snapshotChunks = Chunk.newBuilder[PlayerSnapshot]
                      val changeChunks   = Chunk.newBuilder[MemberChange]

                      changeChunks += JoinedClub(now)

                      // Check if snapshot needs updating
                      PlayerSnapshot.selectIdLatest(playerId).map { latestOpt =>
                        val needsSnapshot = latestOpt.forall(l =>
                          l.username != username || l.status != statusCategory || l.title != apiPlayer.title
                        )
                        if needsSnapshot then
                          snapshotChunks += PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
                          latestOpt.foreach { latest =>
                            if latest.username != username then changeChunks += UsernameChange(now, latest.username)
                            if latest.status != statusCategory then changeChunks += StatusChange(now, latest.status)
                          }

                        val summary = MemberChangeSummary(playerId, changeChunks.result())
                        acc.copy(
                          resolvedIds = acc.resolvedIds + playerId,
                          changes = acc.changes :+ summary,
                          newSnapshots = acc.newSnapshots ++ snapshotChunks.result(),
                          newMemberships = acc.newMemberships :+ newMember
                        )
                      }

                    case None =>
                      // Brand new player
                      val player   = Player(playerId, Instant.ofEpochSecond(apiPlayer.joined), None)
                      val snapshot = PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
                      val member   = ClubMember(clubId, playerId, since, None)
                      val summary  = MemberChangeSummary(playerId, Chunk(NewMember(now)))
                      ZIO
                        .succeed(
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
        }
      }
  }

  // --- Phase C: Classify disappeared members ---

  private[membership] final case class PhaseCResult(
      changes: Chunk[MemberChangeSummary],
      newSnapshots: Chunk[PlayerSnapshot],
      closedMemberships: Chunk[ClubMember])

  private[membership] def classifyDisappeared(
      client: ChessComClient,
      dbState: DbState,
      resolvedIds: Set[PlayerId],
      now: Instant
    ): ZIO[Transactor, Throwable, PhaseCResult] = {
    val disappeared = dbState.membersByPlayerId.values.filterNot(s => resolvedIds.contains(s.player.playerId))
    val initial     = PhaseCResult(Chunk.empty, Chunk.empty, Chunk.empty)

    ZIO
      .foldLeft(disappeared)(initial) { case (acc, state) =>
        val playerId     = state.player.playerId
        val oldUsername  = state.player.username
        val closedMember = state.member.copy(until = Some(now))

        client.getWithPermit[ApiPlayer](ApiPlayer.getUrl(oldUsername)).foldZIO(
          // API failure (404 etc.) — unresolvable
          _ =>
            ZIO
              .succeed(
                acc.copy(
                  changes = acc.changes :+ MemberChangeSummary(playerId, Chunk(Unresolvable(now, oldUsername))),
                  closedMemberships = acc.closedMemberships :+ closedMember
                )
              ),
          apiPlayer =>
            if apiPlayer.playerId != playerId then
              // Different player ID at same username — unresolvable
              ZIO
                .succeed(
                  acc.copy(
                    changes = acc.changes :+ MemberChangeSummary(playerId, Chunk(Unresolvable(now, oldUsername))),
                    closedMemberships = acc.closedMemberships :+ closedMember
                  )
                )
            else
              val statusCategory = apiPlayer.status.category
              val changeChunks   = Chunk.newBuilder[MemberChange]
              val snapshotChunks = Chunk.newBuilder[PlayerSnapshot]

              if statusCategory != PlayerStatusCategory.Active then changeChunks += AccountClosed(now, statusCategory)
              else changeChunks += LeftClub(now)

              // Detect status/title/username changes vs latest snapshot
              if state.player.status != statusCategory || state.player.username != apiPlayer.username || state.player
                  .title != apiPlayer.title
              then
                snapshotChunks += PlayerSnapshot(playerId, now, apiPlayer.username, statusCategory, apiPlayer.title)
                if state.player.status != statusCategory then changeChunks += StatusChange(now, state.player.status)
                if state.player.username != apiPlayer.username then
                  changeChunks += UsernameChange(now, state.player.username)

              ZIO
                .succeed(
                  acc.copy(
                    changes = acc.changes :+ MemberChangeSummary(playerId, changeChunks.result()),
                    newSnapshots = acc.newSnapshots ++ snapshotChunks.result(),
                    closedMemberships = acc.closedMemberships :+ closedMember
                  )
                )
        )
      }
  }

  // --- Merge & Persist ---

  private[membership] def mergeResults(b: PhaseBResult, c: PhaseCResult): ReconciliationResult =
    ReconciliationResult(
      changes = b.changes ++ c.changes,
      newPlayers = b.newPlayers,
      newSnapshots = b.newSnapshots ++ c.newSnapshots,
      newMemberships = b.newMemberships,
      closedMemberships = b.closedMemberships ++ c.closedMemberships
    )

  private def persist(result: ReconciliationResult): ZIO[Transactor, Throwable, Unit] =
    for {
      _ <- ZIO.when(result.newPlayers.nonEmpty)(Player.insertBatch(result.newPlayers))
      _ <- ZIO.when(result.newSnapshots.nonEmpty)(PlayerSnapshot.insertBatch(result.newSnapshots))
      _ <- ZIO.when(result.newMemberships.nonEmpty)(ClubMember.insertBatch(result.newMemberships))
      _ <- ZIO.when(result.closedMemberships.nonEmpty)(ClubMember.updateBatch(result.closedMemberships))
    } yield ()

  // --- Reporting ---

  private def reportReconciliation(result: ReconciliationResult): ZIO[Any, Nothing, Unit] =
    for {
      _ <- Console.printLine(s"=== Reconciliation Complete ===").orDie
      _ <- Console.printLine(s"New players:        ${result.newPlayers.size}").orDie
      _ <- Console.printLine(s"New snapshots:      ${result.newSnapshots.size}").orDie
      _ <- Console.printLine(s"New memberships:    ${result.newMemberships.size}").orDie
      _ <- Console.printLine(s"Closed memberships: ${result.closedMemberships.size}").orDie
      _ <- Console.printLine("").orDie
      _ <- ZIO.foreachDiscard(result.changes)(printChangeSummary)
    } yield ()

  private def printChangeSummary(summary: MemberChangeSummary): ZIO[Any, Nothing, Unit] =
    for {
      _ <- Console.printLine(s"Player ${summary.playerId}:").orDie
      _ <- ZIO.foreachDiscard(summary.changes) { change =>
        Console.printLine(s"  ${formatChange(change)}").orDie
      }
    } yield ()

  private def formatChange(change: MemberChange): String = change match
    case NewMember(ts)                 => s"[NEW MEMBER] at $ts"
    case JoinedClub(ts)                => s"[JOINED CLUB] at $ts"
    case LeftClub(ts)                  => s"[LEFT CLUB] at $ts"
    case AccountClosed(ts, status)     => s"[ACCOUNT CLOSED] at $ts — status: $status"
    case Rejoined(ts, prevUntil)       => s"[REJOINED] at $ts — previously left at $prevUntil"
    case Unresolvable(ts, oldUsername) => s"[UNRESOLVABLE] at $ts — old username: $oldUsername"
    case UsernameChange(ts, oldName)   => s"[USERNAME CHANGE] at $ts — was: $oldName"
    case StatusChange(ts, oldStatus)   => s"[STATUS CHANGE] at $ts — was: $oldStatus"

  // --- Report mode: DB-only ---

  private def report(clubUrlName: ClubUrlName, since: Instant, until: Instant): ZIO[Transactor, Throwable, Unit] =
    for {
      clubs <- Club.selectAll
      club <- ZIO.fromOption(clubs.find(_.urlName == clubUrlName))
        .orElseFail(ExternalException(s"Club '$clubUrlName' not found in database"))
      clubId = club.clubId
      members <- ClubMember.selectClub(clubId)
      snaps   <- PlayerSnapshot.selectSince(since)
      _       <- Console.printLine(s"=== Report for ${clubUrlName} from $since to $until ===").orDie
      _       <- reportFromDb(clubId, members, snaps, since, until)
    } yield ()

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
        if cm.since.compareTo(since) >= 0 && cm.since.compareTo(until) <= 0 then
          // Check if there's a prior membership for same club+player
          val priorMemberships = members
            .filter(m => m.clubId == clubId && m.playerId == playerId && m.until.isDefined && m.since != cm.since)
          if priorMemberships.nonEmpty then
            val latestPrior = priorMemberships.maxBy(_.since)
            changes += Rejoined(cm.since, latestPrior.until.getOrElse(latestPrior.since))
          else
            // Check if player has snapshots before this membership — existing player joining club
            val priorSnaps = playerSnaps.filter(_.since.isBefore(cm.since))
            if priorSnaps.nonEmpty then changes += JoinedClub(cm.since)
            else changes += NewMember(cm.since)

        // Closed membership in range
        cm.until.foreach { u =>
          if u.compareTo(since) >= 0 && u.compareTo(until) <= 0 then
            // Check latest snapshot to determine reason
            val latestSnap = playerSnaps.filter(s => !s.since.isAfter(u)).lastOption
            latestSnap match
              case Some(snap) if snap.status != PlayerStatusCategory.Active =>
                changes += AccountClosed(u, snap.status)
              case Some(_) =>
                changes += LeftClub(u)
              case None =>
                // No snapshot found near the closure — unresolvable
                val username = playerSnaps.headOption.map(_.username).getOrElse(Username.wrap("unknown"))
                changes += Unresolvable(u, username)
        }
      }

      // Detect username and status changes from snapshots in range
      val snapsInRange = playerSnaps.filter(s => s.since.compareTo(since) >= 0 && s.since.compareTo(until) <= 0)
      snapsInRange.foreach { snap =>
        val previousSnap = playerSnaps.filter(_.since.isBefore(snap.since)).lastOption
        previousSnap.foreach { prev =>
          if prev.username != snap.username then changes += UsernameChange(snap.since, prev.username)
          if prev.status != snap.status then changes += StatusChange(snap.since, prev.status)
        }
      }

      MemberChangeSummary(playerId, changes.result())
    }.filter(_.changes.nonEmpty)
  }

  private def reportFromDb(
      clubId: ClubId,
      members: List[ClubMember],
      snaps: List[PlayerSnapshot],
      since: Instant,
      until: Instant
    ): ZIO[Any, Nothing, Unit] =
    ZIO.foreachDiscard(classifyFromDb(clubId, members, snaps, since, until))(printChangeSummary)
}
