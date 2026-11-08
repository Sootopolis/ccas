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
      _            <- printSections(memberLines(summaries, invitations))
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
    val duration = JDuration.between(result.startedAt, result.completedAt)
    for {
      _ <- ZIO.logInfo(s"=== Reconciliation Complete ===")
      _ <- ZIO.logInfo(s"Duration:             ${duration.display}")
      _ <- ZIO.foreachDiscard(rosterSummary(result.rosterMovement))(ZIO.logInfo(_))
      _ <- ZIO.logInfo(s"New players:          ${result.newPlayers.size}")
      _ <- ZIO.logInfo(s"Updated players:      ${result.updatedPlayers.size}")
      _ <- ZIO.logInfo(s"Unsettled joins:      ${result.unsettledJoins.size}")
      _ <- ZIO.logInfo(s"Not recorded:         ${result.refused.size}")
      _ <- ZIO.logInfo("")
      _ <- printSections(reconciliationLines(result, invitations))
      _ <- ZIO.foreachDiscard(listSections(result).flatten)(ZIO.logInfo(_))
    } yield ()
  }

  private def rosterSummary(movement: RosterMovement): List[String] = {
    val sign = if (movement.net >= 0) "+" else ""
    List(
      s"Active roster:        ${movement.before.size} → ${movement.after.size} ($sign${movement.net})",
      s"Roster gains:         ${movement.gains.size}",
      s"Roster losses:        ${movement.losses.size}"
    )
  }

  /** The sections of a reconciliation's report that are not changes to a member, those with anything to say. */
  private def listSections(result: ReconciliationResult): List[List[String]] =
    List(
      unsettledJoinLines(result.unsettledJoins),
      refusedLines(result.refused),
      unsettledLines(result.settlement)
    ).filter(_.nonEmpty)

  /** Closed accounts whose club list settled nothing, so their rows stay as they were (ADR 0022). */
  private def unsettledLines(settlement: ClosedAccountClubs.Settlement): List[String] =
    Option.when(settlement.unsettled.nonEmpty)("[MEMBERSHIP NEEDS REVIEW]").toList ++
      settlement.unsettled.map(unsettled => s"  ${unsettled.username} — ${unsettled.reason}")

  /** The sections of changes to members, in the order a report prints them. */
  private enum Section(val label: String) {
    case Joins         extends Section("[JOINED]")
    case Rejoins       extends Section("[REJOINED]")
    case Returns       extends Section("[BACK ON ROSTER]")
    case Leaves        extends Section("[LEFT CLUB]")
    case Closures      extends Section("[ACCOUNT CLOSED]")
    case Reopenings    extends Section("[ACCOUNT REOPENED]")
    case Renames       extends Section("[USERNAME CHANGED]")
    case Unresolved    extends Section("[UNRESOLVED MEMBER]")
    case StatusChanges extends Section("[ACCOUNT STATUS CHANGE]")
  }

  /** What happened to `username`, under `section`, placed in it by `timestamp`. */
  private final case class ReportLine(section: Section, username: Username, timestamp: Instant, detail: String)

  private def printSections(lines: List[ReportLine]): URIO[ProgressDisplay, Unit] =
    ZIO.foreachDiscard(sections(lines)) { (label, entries) =>
      ZIO.logInfo(label) *> ZIO.foreachDiscard(entries)(ZIO.logInfo(_))
    }

  private def formatSections(lines: List[ReportLine]): String =
    sections(lines).map((label, entries) => (label :: entries).map(_ + "\n").mkString + "\n").mkString

  /** Each section with anything in it, under its label, its lines earliest first. */
  private def sections(lines: List[ReportLine]): List[(String, List[String])] =
    lines.groupBy(_.section).toList.sortBy((section, _) => section.ordinal).map { (section, inSection) =>
      val ordered = inSection.sortBy(line => (line.timestamp, line.username.value))
      section.label -> ordered.map(line => s"  ${line.username} — ${line.detail}")
    }

  private def memberLines(summaries: List[MemberChangeSummary], invitations: Map[PlayerId, Instant]): List[ReportLine] =
    summaries.flatMap(summary => summaryLines(summary, invitations.get(summary.playerId)))

  /** A reconcile's changes to members, then what closed accounts' kept club lists showed, as the events they record. */
  private def reconciliationLines(result: ReconciliationResult, invitations: Map[PlayerId, Instant]): List[ReportLine] =
    memberLines(result.changes.toList, invitations) ++ settlementLines(result.settlement)

  /** The lines `summary`'s changes make, one per event: a closure a row names covers the status change behind it, and
    * an account turning active covers the row it reopens.
    */
  private def summaryLines(summary: MemberChangeSummary, lastInvited: Option[Instant]): Chunk[ReportLine] = {
    val closureNamed = summary.changes.exists(_.isInstanceOf[AccountClosed])
    val reactivated = summary.changes.exists {
      case StatusChange(_, _, newStatus) => newStatus == PlayerStatusCategory.Active
      case _                             => false
    }
    val events = summary.changes.filterNot {
      case _: Reopened => reactivated
      case StatusChange(_, oldStatus, newStatus) =>
        closureNamed && oldStatus == PlayerStatusCategory.Active && newStatus != PlayerStatusCategory.Active
      case _ => false
    }
    events.map { change =>
      val (section, detail) = changeLine(change, lastInvited)
      ReportLine(section = section, username = summary.username, timestamp = change.timestamp, detail = detail)
    }
  }

  private def changeLine(change: MemberChange, lastInvited: Option[Instant]): (Section, String) = {
    val invited = lastInvited.fold("")(ts => s" — invited at $ts")
    change match {
      case NewMember(ts)                   => Section.Joins      -> s"at $ts$invited"
      case JoinedClub(ts)                  => Section.Joins      -> s"at $ts$invited"
      case Rejoined(ts, previous)          => Section.Rejoins    -> s"at $ts — ${previousMembership(previous)}$invited"
      case Reopened(since, was)            => reopening(since, was)
      case LeftClub(ts)                    => Section.Leaves     -> s"by $ts"
      case AccountClosed(lastSeen, status) => Section.Closures   -> closure(status, lastSeen)
      case Unresolvable(ts, oldUsername)   => Section.Unresolved -> s"at $ts — old username: $oldUsername"
      case UsernameChange(ts, oldName)     => Section.Renames    -> s"at $ts — previously $oldName"
      case StatusChange(ts, oldStatus, newStatus) => statusChangeLine(ts, oldStatus, newStatus)
    }
  }

  /** A row reopened at its start `since`. A hidden one was a closed account's; an ended one we had taken as gone. */
  private def reopening(since: Instant, was: MembershipStanding.OffRoster): (Section, String) =
    was match {
      case MembershipStanding.Hidden(lastSeen) => Section.Reopenings -> s"member since $since, last seen $lastSeen"
      case MembershipStanding.Ended(lastSeen, until) =>
        val missing = if (lastSeen == until) "" else s", missing by $until"
        Section.Returns -> s"same membership since $since; last seen $lastSeen$missing"
    }

  /** A status change seen at `ts`: a closure or a reopening if it crosses active, else one closure for another. */
  private def statusChangeLine(
    ts: Instant,
    oldStatus: PlayerStatusCategory,
    newStatus: PlayerStatusCategory
  ): (Section, String) =
    (oldStatus == PlayerStatusCategory.Active, newStatus == PlayerStatusCategory.Active) match {
      case (true, false) => Section.Closures   -> s"status: ${statusDescription(newStatus)}, by $ts"
      case (false, true) => Section.Reopenings -> s"at $ts"
      case _ =>
        Section.StatusChanges -> s"at $ts — changed from ${statusDescription(oldStatus)} to ${statusDescription(newStatus)}"
    }

  /** An account that closed some time after `lastSeen`, the last we knew of it. */
  private def closure(status: PlayerStatusCategory, lastSeen: Instant): String =
    s"status: ${statusDescription(status)}, after $lastSeen"

  private def statusDescription(status: PlayerStatusCategory): String = status match {
    case PlayerStatusCategory.Active   => "active"
    case PlayerStatusCategory.Closed   => "closed"
    case PlayerStatusCategory.Fairplay => "closed for fair-play violations"
    case PlayerStatusCategory.Abuse    => "closed for abuse"
    case PlayerStatusCategory.Unknown  => "unknown"
  }

  /** What kept club lists showed (ADR 0022): a closure the rows had as an end, or a membership they lacked. */
  private def settlementLines(settlement: ClosedAccountClubs.Settlement): List[ReportLine] = {
    def closureLine(username: Username, row: ClubMember, status: PlayerStatusCategory) = {
      val lastSeen = row.hiddenAt.getOrElse(row.since)
      ReportLine(Section.Closures, username, lastSeen, closure(status, lastSeen))
    }
    val hidden = settlement.hidden.map(hidden => closureLine(hidden.username, hidden.row, hidden.status))
    val opened = settlement.opened.flatMap { opened =>
      Chunk(
        ReportLine(Section.Joins, opened.username, opened.row.since, s"at ${opened.row.since}"),
        closureLine(opened.username, opened.row, opened.status)
      )
    }
    (hidden ++ opened).toList
  }

  private val UnsettledJoinLabel = "[ROSTER JOIN UNSETTLED]"
  private val RefusedLabel       = "[MEMBERSHIP NOT RECORDED]"

  private def unsettledJoinLines(joins: Chunk[UnsettledJoin]): List[String] =
    Option.when(joins.nonEmpty)(UnsettledJoinLabel).toList ++
      joins.sortBy(_.joinedAt).map(join => s"  ${join.username} — ${unsettledJoin(join)}")

  /** When an ended row was last seen is why a join inside it was refused, and only that row keeps them off the roster. */
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

  def formatReconciliation(result: ReconciliationResult, invitations: Map[PlayerId, Instant]): String = {
    val duration = JDuration.between(result.startedAt, result.completedAt)
    val header = s"""Started:   ${result.startedAt}
                    |Completed: ${result.completedAt}
                    |Duration:  ${duration.display}
                    |
                    |=== Reconciliation Complete ===
                    |${rosterSummary(result.rosterMovement).mkString("\n")}
                    |New players:          ${result.newPlayers.size}
                    |Updated players:      ${result.updatedPlayers.size}
                    |Unsettled joins:      ${result.unsettledJoins.size}
                    |Not recorded:         ${result.refused.size}
                    |""".stripMargin
    header + "\n" + formatSections(reconciliationLines(result, invitations)) +
      listSections(result).map(_.map(_ + "\n").mkString).mkString("\n")
  }

  def formatReport(rr: ReportResult): String = {
    val delta    = rr.memberCountAtEnd - rr.memberCountAtStart
    val sign     = if (delta >= 0) "+" else ""
    val header   = s"Total members: ${rr.memberCountAtEnd} ($sign$delta)\n\n"
    val inferred = inferredLines(rr.inferred)
    if (rr.summaries.isEmpty && inferred.isEmpty) { header + "No changes\n" }
    else { header + formatSections(memberLines(rr.summaries, rr.invitations)) + inferred.map(_ + "\n").mkString }
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

  /** How `row` went off the roster, read from the states its player has been in and the names they have held, each
    * oldest first. `None` while it is open.
    */
  private[membership] def offRosterChange(
    row: ClubMember,
    playerStates: List[PlayerSnapshot],
    playerNames: List[PlayerName]
  ): Option[MemberChange] =
    row.standing match {
      case MembershipStanding.Open => None
      case MembershipStanding.Ended(lastSeen, until) =>
        Some(playerStates.findLast(!_.since.isAfter(until)) match {
          case Some(state) =>
            if (state.status == PlayerStatusCategory.Active) { LeftClub(until) }
            else { AccountClosed(lastSeen, state.status) }
          case None => Unresolvable(until, playerNames.headOption.fold(Username.wrap("unknown"))(_.username))
        })
      // Whatever the account's status now: History can see it active again before a roster reopens the row.
      case MembershipStanding.Hidden(lastSeen) =>
        playerStates.findLast(_.status != PlayerStatusCategory.Active).map { state =>
          AccountClosed(lastSeen, state.status)
        }
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

        if (cm.until.orElse(cm.hiddenAt).exists(inRange)) { changes ++= offRosterChange(cm, playerStates, playerNames) }
      }

      // Two histories now (#254): a status change is read from the states, a rename from the name windows.
      playerStates.zip(playerStates.drop(1)).foreach { (previous, state) =>
        if (inRange(state.since) && previous.status != state.status) {
          changes += StatusChange(state.since, previous.status, state.status)
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
