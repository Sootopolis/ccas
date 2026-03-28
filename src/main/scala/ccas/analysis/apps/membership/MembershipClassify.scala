package ccas.analysis.apps.membership

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.ref.RefHelpers
import ccas.analysis.tables.*
import ccas.api.clubmatch.TeamMatchPlayerStarted
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerClubs}
import ccas.api.tournament.ApiTournament
import ccas.utils.CcasLogger
import ccas.utils.client.ChessComClient
import com.augustnagro.magnum.Transactor
import zio.{Chunk, RIO, Ref, Task, UIO, ZIO}

private[membership] object MembershipClassify {

  // --- Phase B: Classify API members ---

  final case class PhaseBResult(
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

  def classifyApiMembers(
    client: ChessComClient,
    clubId: ClubId,
    apiMap: Map[Username, Long],
    dbState: DbState,
    now: java.time.Instant,
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
    now: java.time.Instant,
    trustUsernames: Boolean
  ): RIO[Transactor, PhaseBMemberResult] = {
    val since = java.time.Instant.ofEpochSecond(joinedEpoch)
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
    since: java.time.Instant,
    dbState: DbState,
    now: java.time.Instant
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
              val player   = Player(playerId, java.time.Instant.ofEpochSecond(apiPlayer.joined))
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

  final case class PhaseCResult(
    changes: Chunk[MemberChangeSummary],
    newSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  private case class PhaseCMemberResult(
    changes: Chunk[MemberChangeSummary],
    newSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  def classifyDisappeared(
    client: ChessComClient,
    dbState: DbState,
    resolvedIds: Set[PlayerId],
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: java.time.Instant
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
    now: java.time.Instant
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
    now: java.time.Instant
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
    now: java.time.Instant
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
    RefHelpers.fetchTeamMatchTeams(client, ref.matchId, ref.isLive).map { teams =>
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
}
