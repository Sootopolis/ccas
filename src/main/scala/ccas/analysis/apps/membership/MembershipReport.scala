package ccas.analysis.apps.membership

import java.sql.SQLException
import java.time.{Duration as JDuration, Instant}

import zio.{Chunk, RIO, URIO, ZIO}

import ccas.analysis.apps.NamedClub
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.utils.{display, ProgressDisplay}
import ccas.utils.sql.PostgresClient

private[membership] object MembershipReport {

  /** A membership inferred from matches, under its player's name now. */
  final case class InferredMember(username: Username, membership: InferredMembership)

  /** `inferred` holds the memberships inferred from matches that overlap the range. */
  case class ReportResult(
    summaries: List[MemberChangeSummary],
    inferred: List[InferredMember],
    memberCountAtStart: Int,
    memberCountAtEnd: Int,
    invitations: Map[PlayerId, Instant]
  )

  /** Counts come from the rows alone, so each change in them has an event in `summaries` behind it. */
  def report(club: NamedClub, since: Instant, until: Instant): RIO[ProgressDisplay & PostgresClient, ReportResult] =
    for {
      members   <- ClubMember.selectClub(club.clubId)
      sightings <- ClubMatchBoard.selectSightings(club.clubId)
      coverage  <- ClubRosterCoverage.select(club.clubId)
      inferred  = InferredMembership.infer(members, sightings, coverage.map(_.firstObservedAt))
      memberIds = members.map(_.playerId).distinct
      // Only rows are classified, so only their players' states are read; an inferred membership needs just a name.
      states <- PlayerSnapshot.selectHistory(memberIds)
      names  <- PlayerName.selectPlayers((memberIds ++ inferred.map(_.playerId)).distinct)
      summaries   = classifyFromDb(club.clubId, members, inferred, states, names, since, until)
      overlapping = inferredOverlapping(inferred, names, since, until)
      invitations  <- lookupJoinInvitations(club.clubId, summaries)
      countAtStart <- ClubMember.countAt(club.clubId, since)
      countAtEnd   <- ClubMember.countAt(club.clubId, until)
      _ <- ZIO.logInfo(s"=== Report for ${club.slug} from $since to $until ===")
      _ <- ZIO.logInfo(s"Members: $countAtStart -> $countAtEnd")
      _ <- printChangeSummaries(summaries, invitations)
      _ <- ZIO.foreachDiscard(inferredLines(overlapping))(ZIO.logInfo(_))
    } yield ReportResult(summaries, overlapping, countAtStart, countAtEnd, invitations)

  /** Each of `inferred` that overlaps `[since, until]`, under its player's name now, earliest first. */
  private def inferredOverlapping(
    inferred: List[InferredMembership],
    names: List[PlayerName],
    since: Instant,
    until: Instant
  ): List[InferredMember] = {
    val namesByPlayer = names.groupBy(_.playerId)
    inferred
      .filter(membership => !membership.firstMatch.isAfter(until) && !membership.lastMatch.isBefore(since))
      .sortBy(_.firstMatch)
      .map { membership =>
        InferredMember(
          username = latestName(namesByPlayer.getOrElse(membership.playerId, Nil)),
          membership = membership
        )
      }
  }

  private def latestName(names: List[PlayerName]): Username =
    names.maxByOption(_.since).fold(Username.wrap("unknown"))(_.username)

  /** Memberships no roster recorded, which the club's matches show (ADR 0023). */
  private def inferredLines(inferred: List[InferredMember]): List[String] = {
    val lines = inferred.map { case InferredMember(username, membership) =>
      s"  $username — played for the club from ${membership.firstMatch} to ${membership.lastMatch}, " +
        s"${membership.matches} ${if (membership.matches == 1) "match" else "matches"}"
    }
    Option.when(lines.nonEmpty)("[INFERRED FROM MATCHES]").toList ++ lines
  }

  def reportReconciliation(
    result: ReconciliationResult,
    invitations: Map[PlayerId, Instant]
  ): URIO[ProgressDisplay, Unit] = {
    val delta    = result.currentMemberCount - result.previousMemberCount
    val sign     = if (delta >= 0) "+" else ""
    val duration = JDuration.between(result.startedAt, result.completedAt)
    for {
      _ <- ZIO.logInfo(s"=== Reconciliation Complete ===")
      _ <- ZIO.logInfo(s"Duration:             ${duration.display}")
      _ <- ZIO.logInfo(s"Total members:        ${result.currentMemberCount} ($sign$delta)")
      _ <- ZIO.logInfo(s"New players:          ${result.newPlayers.size}")
      _ <- ZIO.logInfo(s"Updated players:      ${result.updatedPlayers.size}")
      _ <- ZIO.logInfo(s"New memberships:      ${result.newMemberships.size}")
      _ <- ZIO.logInfo(s"Closed memberships:   ${result.closedMemberships.size}")
      _ <- ZIO.logInfo(s"Hidden memberships:   ${result.hiddenMemberships.size}")
      _ <- ZIO.logInfo(s"Reopened memberships: ${result.reopenedMemberships.size}")
      _ <- ZIO.logInfo("")
      _ <- printChangeSummaries(result.changes.toList, invitations)
      _ <- ZIO.foreachDiscard(unsettledLines(result))(ZIO.logInfo(_))
    } yield ()
  }

  /** Closed accounts whose club list settled nothing, so their rows stay as they were (ADR 0022). */
  private def unsettledLines(result: ReconciliationResult): List[String] =
    Option.when(result.unsettledLists.nonEmpty)("[CLUB LIST UNSETTLED]").toList ++
      result.unsettledLists.map(unsettled => s"  ${unsettled.username} — ${unsettled.reason}")

  private def printChangeSummaries(
    summaries: List[MemberChangeSummary],
    invitations: Map[PlayerId, Instant]
  ): URIO[ProgressDisplay, Unit] = {
    val grouped = groupByCategory(summaries, invitations)
    ZIO.foreachDiscard(grouped) { case (label, entries) =>
      ZIO.logInfo(label) *>
        ZIO.foreachDiscard(entries) { case (display, detail) =>
          ZIO.logInfo(s"  $display — $detail")
        }
    }
  }

  private def categoryLabel(change: MemberChange): String = change match {
    case _: NewMember      => "[JOINED]"
    case _: JoinedClub     => "[JOINED]"
    case _: Rejoined       => "[REJOINED]"
    case _: LeftClub       => "[LEFT CLUB]"
    case _: AccountClosed  => "[ACCOUNT CLOSED]"
    case _: Unresolvable   => "[UNRESOLVABLE]"
    case _: UsernameChange => "[USERNAME CHANGE]"
    case _: StatusChange   => "[STATUS CHANGE]"
  }

  private def formatChangeDetail(change: MemberChange, lastInvited: Option[Instant]): String = {
    val invitedSuffix = lastInvited.fold("")(ts => s" — invited at $ts")
    change match {
      case NewMember(ts)                 => s"at $ts$invitedSuffix"
      case JoinedClub(ts)                => s"at $ts$invitedSuffix"
      case Rejoined(ts, previous)        => s"at $ts — ${previousMembership(previous)}$invitedSuffix"
      case LeftClub(ts)                  => s"at $ts"
      case AccountClosed(ts, status)     => s"after $ts — status: $status"
      case Unresolvable(ts, oldUsername) => s"at $ts — old username: $oldUsername"
      case UsernameChange(ts, oldName)   => s"at $ts — was: $oldName"
      case StatusChange(ts, oldStatus)   => s"at $ts — was: $oldStatus"
    }
  }

  /** No one is seen in and out at once, so recorded ends that meet hold no sighting of the player gone, only of them
    * in.
    */
  private def previousMembership(previous: PreviousMembership): String =
    previous match {
      case PreviousMembership.Recorded(lastSeen, until) =>
        if (lastSeen == until) s"previous membership last seen at $lastSeen"
        else s"previous membership ended between $lastSeen and $until"
      case PreviousMembership.Inferred(firstMatch, lastMatch) =>
        s"previously played for the club from $firstMatch to $lastMatch"
    }

  private def groupByCategory(
    summaries: List[MemberChangeSummary],
    invitations: Map[PlayerId, Instant]
  ): List[(String, List[(String, String)])] = {
    val entries = summaries.flatMap { summary =>
      val lastInvited = invitations.get(summary.playerId)
      summary.changes.map(change => (change, summary.username, lastInvited))
    }
    def categoryOrder(change: MemberChange): Int = change match {
      case _: NewMember | _: JoinedClub => 0
      case other                        => other.ordinal
    }
    entries
      .groupBy { case (change, _, _) => categoryOrder(change) }
      .toList
      .sortBy(_._1)
      .map { case (_, grouped) =>
        val label = categoryLabel(grouped.head._1)
        val items = grouped
          .sortBy(_._1.timestamp)
          .map { case (change, username, lastInvited) =>
            (username.value, formatChangeDetail(change, lastInvited))
          }
        (label, items)
      }
  }

  def formatReconciliation(result: ReconciliationResult, invitations: Map[PlayerId, Instant]): String = {
    val duration = JDuration.between(result.startedAt, result.completedAt)
    val delta    = result.currentMemberCount - result.previousMemberCount
    val sign     = if (delta >= 0) "+" else ""
    val header = s"""Started:   ${result.startedAt}
                    |Completed: ${result.completedAt}
                    |Duration:  ${duration.display}
                    |
                    |=== Reconciliation Complete ===
                    |Total members:        ${result.currentMemberCount} ($sign$delta)
                    |New players:          ${result.newPlayers.size}
                    |Updated players:      ${result.updatedPlayers.size}
                    |New memberships:      ${result.newMemberships.size}
                    |Closed memberships:   ${result.closedMemberships.size}
                    |Hidden memberships:   ${result.hiddenMemberships.size}
                    |Reopened memberships: ${result.reopenedMemberships.size}
                    |""".stripMargin
    header + "\n" + formatChangeSummaries(result.changes.toList, invitations) +
      unsettledLines(result).map(_ + "\n").mkString
  }

  def formatReport(rr: ReportResult): String = {
    val delta    = rr.memberCountAtEnd - rr.memberCountAtStart
    val sign     = if (delta >= 0) "+" else ""
    val header   = s"Total members: ${rr.memberCountAtEnd} ($sign$delta)\n\n"
    val inferred = inferredLines(rr.inferred)
    if (rr.summaries.isEmpty && inferred.isEmpty) { header + "No changes\n" }
    else { header + formatChangeSummaries(rr.summaries, rr.invitations) + inferred.map(_ + "\n").mkString }
  }

  private def formatChangeSummaries(
    summaries: List[MemberChangeSummary],
    invitations: Map[PlayerId, Instant]
  ): String = {
    val sb      = new StringBuilder
    val grouped = groupByCategory(summaries, invitations)
    grouped.foreach { case (label, entries) =>
      sb.append(s"$label\n")
      entries.foreach { case (display, detail) =>
        sb.append(s"  $display — $detail\n")
      }
      sb.append("\n")
    }
    sb.toString
  }

  // --- Invitation lookup ---

  private[membership] def lookupJoinInvitations(
    clubId: ClubId,
    summaries: List[MemberChangeSummary]
  ): ZIO[PostgresClient, SQLException, Map[PlayerId, Instant]] = {
    val joinPlayerIds = summaries
      .filter(_.changes.exists {
        case _: NewMember | _: JoinedClub | _: Rejoined => true
        case _                                          => false
      })
      .map(_.playerId)
      .distinct
    ZIO.foreach(joinPlayerIds) { pid =>
      RecruitmentCandidate.selectLatestInvitedByClub(pid, clubId).map(_.map(c => pid -> c.evaluatedAt))
    }.map(_.flatten.toMap)
  }

  // --- Report mode: DB-only ---

  /** Classifies each member whose membership began or ended in `[since, until]`, from the club's memberships, those
    * `inferred` from its matches, the states its players have been in (status and title) and the names they have held.
    */
  def classifyFromDb(
    clubId: ClubId,
    members: List[ClubMember],
    inferred: List[InferredMembership],
    states: List[PlayerSnapshot],
    names: List[PlayerName],
    since: Instant,
    until: Instant
  ): List[MemberChangeSummary] = {
    val statesByPlayer   = states.groupBy(_.playerId)
    val namesByPlayer    = names.groupBy(_.playerId)
    val inferredByPlayer = inferred.groupBy(_.playerId)
    def inRange(t: Instant): Boolean = t.compareTo(since) >= 0 && t.compareTo(until) <= 0

    /** The membership `playerId` had before the one starting at `start`, recorded or inferred, by when it started. */
    def previousTo(playerId: PlayerId, start: Instant): Option[PreviousMembership] = {
      val recorded = members.collect {
        case m if m.clubId == clubId && m.playerId == playerId && m.until.isDefined && m.since.isBefore(start) =>
          m.since -> PreviousMembership.of(m)
      }
      val fromMatches = inferredByPlayer.getOrElse(playerId, Nil).collect {
        case membership if membership.lastMatch.isBefore(start) =>
          membership.firstMatch -> PreviousMembership.Inferred(membership.firstMatch, membership.lastMatch)
      }
      (recorded ++ fromMatches).maxByOption((began, _) => began).map((_, previous) => previous)
    }

    // Find membership changes in the time range
    val changedMembers = members.filter(m => inRange(m.since) || m.until.exists(inRange) || m.hiddenAt.exists(inRange))

    // Group by player
    val membersByPlayer = changedMembers.groupBy(_.playerId)

    membersByPlayer.toList.map { case (playerId, cms) =>
      val playerStates = statesByPlayer.getOrElse(playerId, Nil).sortBy(_.since)
      val playerNames  = namesByPlayer.getOrElse(playerId, Nil).sortBy(_.since)
      val changes      = Chunk.newBuilder[MemberChange]

      cms.foreach { cm =>
        // New membership in range
        if (inRange(cm.since)) {
          previousTo(playerId, cm.since) match {
            case Some(previous) => changes += Rejoined(ts = cm.since, previous = previous)
            case None           =>
              // Check if player has states before this membership — existing player joining club
              if (playerStates.exists(_.since.isBefore(cm.since))) { changes += JoinedClub(cm.since) }
              else { changes += NewMember(cm.since) }
          }
        }

        // Closed membership in range
        cm.until.filter(inRange).foreach { u =>
          // Check latest state to determine reason
          playerStates.filter(s => !s.since.isAfter(u)).lastOption match {
            case Some(state) =>
              if (state.status == PlayerStatusCategory.Active) { changes += LeftClub(u) }
              else { changes += AccountClosed(u, state.status) }
            case None =>
              // No state found near the closure — unresolvable
              val username = playerNames.headOption.fold(Username.wrap("unknown"))(_.username)
              changes += Unresolvable(u, username)
          }
        }

        // Whatever the account's status now: History can see it active again before a roster reopens the row.
        cm.hiddenAt.filter(inRange).foreach { lastSeen =>
          playerStates.findLast(_.status != PlayerStatusCategory.Active).foreach { state =>
            changes += AccountClosed(lastSeen, state.status)
          }
        }
      }

      // Two histories now (#254): a status change is read from the states, a rename from the name windows.
      playerStates.zip(playerStates.drop(1)).foreach { (previous, state) =>
        if (inRange(state.since) && previous.status != state.status) {
          changes += StatusChange(state.since, previous.status)
        }
      }
      playerNames.zip(playerNames.drop(1)).foreach { (previous, name) =>
        if (inRange(name.since) && previous.username != name.username) {
          changes += UsernameChange(name.since, previous.username)
        }
      }

      MemberChangeSummary(playerId, latestName(playerNames), changes.result())
    }.filter(_.changes.nonEmpty)
  }
}
