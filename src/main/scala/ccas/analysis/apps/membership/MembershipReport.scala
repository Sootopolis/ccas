package ccas.analysis.apps.membership

import java.sql.SQLException
import java.time.{Duration as JDuration, Instant}

import zio.{Chunk, RIO, UIO, URIO, ZIO}

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
      _            <- ZIO.logInfo(s"=== Report for ${club.slug} from $since to $until ===")
      _            <- ZIO.logInfo(s"Members: $countAtStart -> $countAtEnd")
      _            <- printChangeSummaries(summaries, invitations)
      _            <- ZIO.foreachDiscard(inferredLines(overlapping))(ZIO.logInfo(_))
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

  private[membership] def latestName(names: List[PlayerName]): Username =
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
      _ <- ZIO.logInfo(s"Settled memberships:  ${result.settlement.settledCount}")
      _ <- ZIO.logInfo(s"Reopened memberships: ${result.reopenedMemberships.size}")
      _ <- ZIO.logInfo(s"Unsettled joins:      ${result.unsettledJoins.size}")
      _ <- ZIO.logInfo(s"Not recorded:         ${result.refused.size}")
      _ <- ZIO.logInfo("")
      _ <- printChangeSummaries(result.changes.toList, invitations)
      _ <- ZIO.foreachDiscard(listSections(result).flatten)(ZIO.logInfo(_))
    } yield ()
  }

  /** The sections of a reconciliation's report that are not changes to a member, those with anything to say. */
  private def listSections(result: ReconciliationResult): List[List[String]] =
    List(
      unsettledJoinLines(result.unsettledJoins),
      refusedLines(result.refused),
      settledLines(result.settlement),
      unsettledLines(result.settlement)
    ).filter(_.nonEmpty)

  /** The rows closed accounts' kept club lists hid or opened, by when each account closed (ADR 0022). */
  private def settledLines(settlement: ClosedAccountClubs.Settlement): List[String] = {
    val hidden = settlement.hidden.map { hidden =>
      (hidden.username, hidden.row, s"end undone, was: ${hidden.undoneUntil}")
    }
    val opened = settlement.opened.map(opened => (opened.username, opened.row, "opened"))
    val lines = (hidden ++ opened).sortBy((_, row, _) => row.hiddenAt).map { (username, row, outcome) =>
      val closed = row.hiddenAt.fold("")(at => s", closed after $at")
      s"  $username — joined at ${row.since}$closed — $outcome"
    }
    Option.when(lines.nonEmpty)("[CLUB LIST SETTLED]").toList ++ lines
  }

  /** Closed accounts whose club list settled nothing, so their rows stay as they were (ADR 0022). */
  private def unsettledLines(settlement: ClosedAccountClubs.Settlement): List[String] =
    Option.when(settlement.unsettled.nonEmpty)("[CLUB LIST UNSETTLED]").toList ++
      settlement.unsettled.map(unsettled => s"  ${unsettled.username} — ${unsettled.reason}")

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
    case _: Reopened       => "[REOPENED]"
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
      case Reopened(ts, was)             => s"joined at $ts — was: ${reopenedFrom(was)}"
      case LeftClub(ts)                  => s"at $ts"
      case AccountClosed(ts, status)     => s"after $ts — status: $status"
      case Unresolvable(ts, oldUsername) => s"at $ts — old username: $oldUsername"
      case UsernameChange(ts, oldName)   => s"at $ts — was: $oldName"
      case StatusChange(ts, oldStatus)   => s"at $ts — was: $oldStatus"
    }
  }

  private def reopenedFrom(was: MembershipStanding.OffRoster): String =
    was match {
      case MembershipStanding.Hidden(lastSeen)       => s"hidden after $lastSeen"
      case MembershipStanding.Ended(lastSeen, until) => howItEnded(lastSeen, until)
    }

  private val UnsettledJoinLabel = "[ROSTER JOIN UNSETTLED]"
  private val RefusedLabel       = "[MEMBERSHIP NOT RECORDED]"

  private def unsettledJoinLines(joins: Chunk[UnsettledJoin]): List[String] =
    Option.when(joins.nonEmpty)(UnsettledJoinLabel).toList ++
      joins.sortBy(_.joinedAt).map(join => s"  ${join.username} — ${unsettledJoin(join)}")

  /** When an ended row was last seen is why a join inside it was refused, and only that row leaves the total short. */
  private def unsettledJoin(join: UnsettledJoin): String = {
    val latest = join.latest
    val (standing, outcome) = latest.standing match {
      case MembershipStanding.Open       => ("", "left as recorded")
      case MembershipStanding.Hidden(at) => (s", hidden after $at", "left as recorded")
      case MembershipStanding.Ended(lastSeen, until) =>
        (s" to $until, last seen $lastSeen", "left as recorded, not counted as a member")
    }
    s"listed as joined at ${join.joinedAt}, latest recorded membership from ${latest.since}$standing — $outcome"
  }

  private def refusedLines(refused: Chunk[RefusedMembershipVerdict]): List[String] =
    Option.when(refused.nonEmpty)(RefusedLabel).toList ++
      refused.map(verdict => s"  ${verdict.username} — ${refusal(verdict)}")

  private def refusal(verdict: RefusedMembershipVerdict): String = {
    val why = verdict.conflict match {
      case MemberRowConflict.Overlap(earlier, later) =>
        s"memberships from ${earlier.since} and from ${later.since} would overlap"
      case MemberRowConflict.SameStart(row) => s"two memberships would both start at ${row.since}"
      case MemberRowConflict.OutOfOrder(row) =>
        val lastSeen = row.lastSeen.fold("never")(_.toString)
        val until    = row.until.fold("not ended")(_.toString)
        s"the membership from ${row.since} would have its dates out of order: last seen $lastSeen, ended $until"
    }
    s"$why — left as recorded until the roster changes or someone corrects the rows by hand; worth an issue if it persists"
  }

  /** Warns of each join the player's rows cannot place (ADR 0026) and each verdict refused (ADR 0027). Only a Membership
    * reconcile prints its report, so without this another job's reconcile, or one a report runs first, would not say.
    */
  def warnOfWhatWasNotRecorded(result: ReconciliationResult): UIO[Unit] =
    ZIO.foreachDiscard(result.unsettledJoins) { join =>
      ZIO.logWarning(s"$UnsettledJoinLabel ${join.username} — ${unsettledJoin(join)}")
    } *> ZIO.foreachDiscard(result.refused) { verdict =>
      ZIO.logWarning(s"$RefusedLabel ${verdict.username} — ${refusal(verdict)}")
    }

  /** No one is seen in and out at once, so recorded ends that meet hold no sighting of the player gone, only of them
    * in.
    */
  private def howItEnded(lastSeen: Instant, until: Instant): String =
    if (lastSeen == until) s"last seen at $lastSeen" else s"ended between $lastSeen and $until"

  private def previousMembership(previous: PreviousMembership): String =
    previous match {
      case PreviousMembership.Recorded(lastSeen, until) => s"previous membership ${howItEnded(lastSeen, until)}"
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
                    |Settled memberships:  ${result.settlement.settledCount}
                    |Reopened memberships: ${result.reopenedMemberships.size}
                    |Unsettled joins:      ${result.unsettledJoins.size}
                    |Not recorded:         ${result.refused.size}
                    |""".stripMargin
    header + "\n" + formatChangeSummaries(result.changes.toList, invitations) +
      listSections(result).map(_.map(_ + "\n").mkString).mkString("\n")
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
    val statesByPlayer               = states.groupBy(_.playerId)
    val namesByPlayer                = names.groupBy(_.playerId)
    val inferredByPlayer             = inferred.groupBy(_.playerId)
    def inRange(t: Instant): Boolean = t.compareTo(since) >= 0 && t.compareTo(until) <= 0

    /** The membership `playerId` had before the one starting at `start`, recorded or inferred, by when it started. */
    def previousTo(playerId: PlayerId, start: Instant): Option[PreviousMembership] = {
      val recorded = members
        .filter(m => m.clubId == clubId && m.playerId == playerId && m.since.isBefore(start))
        .flatMap(m => PreviousMembership.of(m).map(m.since -> _))
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

        cm.standing match {
          case MembershipStanding.Open => ()
          case MembershipStanding.Ended(_, until) =>
            if (inRange(until)) {
              // Check latest state to determine reason
              playerStates.filter(s => !s.since.isAfter(until)).lastOption match {
                case Some(state) =>
                  if (state.status == PlayerStatusCategory.Active) { changes += LeftClub(until) }
                  else { changes += AccountClosed(until, state.status) }
                case None =>
                  // No state found near the closure — unresolvable
                  val username = playerNames.headOption.fold(Username.wrap("unknown"))(_.username)
                  changes += Unresolvable(until, username)
              }
            }
          case MembershipStanding.Hidden(lastSeen) =>
            // Whatever the account's status now: History can see it active again before a roster reopens the row.
            if (inRange(lastSeen)) {
              playerStates.findLast(_.status != PlayerStatusCategory.Active).foreach { state =>
                changes += AccountClosed(lastSeen, state.status)
              }
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
