package ccas.analysis.apps.membership

import java.sql.SQLException
import java.time.{Duration as JDuration, Instant}

import zio.{Chunk, RIO, URIO, ZIO}

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.{display, CcasLogger}
import ccas.utils.errors.NotFoundException
import ccas.utils.sql.PostgresClient

private[membership] object MembershipReport {

  case class ReportResult(
    summaries: List[MemberChangeSummary],
    memberCountAtStart: Int,
    memberCountAtEnd: Int,
    invitations: Map[PlayerId, Instant]
  )

  def report(clubSlug: ClubSlug, since: Instant, until: Instant): RIO[CcasLogger & PostgresClient, ReportResult] =
    for {
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(NotFoundException(s"Club '$clubSlug' not found in database"))
      clubId = club.clubId
      members <- ClubMember.selectClub(clubId)
      snaps   <- PlayerSnapshot.selectSince(since)
      summaries    = classifyFromDb(clubId, members, snaps, since, until)
      invitations <- lookupJoinInvitations(clubId, summaries)
      countAtStart <- ClubMember.countActiveCurrentAt(clubId, since)
      countAtEnd   <- ClubMember.countActiveCurrentAt(clubId, until)
      _ <- CcasLogger.info(s"=== Report for $clubSlug from $since to $until ===")
      _ <- CcasLogger.info(s"Members: $countAtStart -> $countAtEnd")
      _ <- printChangeSummaries(summaries, invitations)
    } yield ReportResult(summaries, countAtStart, countAtEnd, invitations)

  def reportReconciliation(
    result: ReconciliationResult,
    invitations: Map[PlayerId, Instant]
  ): URIO[CcasLogger, Unit] = {
    val delta    = result.currentMemberCount - result.previousMemberCount
    val sign     = if (delta >= 0) "+" else ""
    val duration = JDuration.between(result.startedAt, result.completedAt)
    for {
      _ <- CcasLogger.info(s"=== Reconciliation Complete ===")
      _ <- CcasLogger.info(s"Duration:           ${duration.display}")
      _ <- CcasLogger.info(s"Total members:      ${result.currentMemberCount} ($sign$delta)")
      _ <- CcasLogger.info(s"New players:        ${result.newPlayers.size}")
      _ <- CcasLogger.info(s"Updated players:    ${result.updatedPlayers.size}")
      _ <- CcasLogger.info(s"New memberships:    ${result.newMemberships.size}")
      _ <- CcasLogger.info(s"Closed memberships: ${result.closedMemberships.size}")
      _ <- CcasLogger.info("")
      _ <- printChangeSummaries(result.changes.toList, invitations)
    } yield ()
  }

  private def printChangeSummaries(
    summaries: List[MemberChangeSummary],
    invitations: Map[PlayerId, Instant]
  ): URIO[CcasLogger, Unit] = {
    val grouped = groupByCategory(summaries, invitations)
    ZIO.foreachDiscard(grouped) { case (label, entries) =>
      CcasLogger.info(label) *>
        ZIO.foreachDiscard(entries) { case (username, detail) =>
          CcasLogger.info(s"  $username — $detail")
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
      case Rejoined(ts, prevUntil)       => s"at $ts — previously left at $prevUntil$invitedSuffix"
      case LeftClub(ts)                  => s"at $ts"
      case AccountClosed(ts, status)     => s"at $ts — status: $status"
      case Unresolvable(ts, oldUsername) => s"at $ts — old username: $oldUsername"
      case UsernameChange(ts, oldName)   => s"at $ts — was: $oldName"
      case StatusChange(ts, oldStatus)   => s"at $ts — was: $oldStatus"
    }
  }

  private def groupByCategory(
    summaries: List[MemberChangeSummary],
    invitations: Map[PlayerId, Instant]
  ): List[(String, List[(Username, String)])] = {
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
          .map { case (change, username, lastInvited) => (username, formatChangeDetail(change, lastInvited)) }
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
                    |Total members:      ${result.currentMemberCount} ($sign$delta)
                    |New players:        ${result.newPlayers.size}
                    |Updated players:    ${result.updatedPlayers.size}
                    |New memberships:    ${result.newMemberships.size}
                    |Closed memberships: ${result.closedMemberships.size}
                    |""".stripMargin
    header + "\n" + formatChangeSummaries(result.changes.toList, invitations)
  }

  def formatReport(rr: ReportResult): String = {
    val delta  = rr.memberCountAtEnd - rr.memberCountAtStart
    val sign   = if (delta >= 0) "+" else ""
    val header = s"Total members: ${rr.memberCountAtEnd} ($sign$delta)\n\n"
    if (rr.summaries.isEmpty) { header + "No changes\n" }
    else { header + formatChangeSummaries(rr.summaries, rr.invitations) }
  }

  private def formatChangeSummaries(
    summaries: List[MemberChangeSummary],
    invitations: Map[PlayerId, Instant]
  ): String = {
    val sb      = new StringBuilder
    val grouped = groupByCategory(summaries, invitations)
    grouped.foreach { case (label, entries) =>
      sb.append(s"$label\n")
      entries.foreach { case (username, detail) =>
        sb.append(s"  $username — $detail\n")
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

  def classifyFromDb(
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
