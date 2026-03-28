package ccas.analysis.apps.membership

import java.time.{Instant, Duration as JDuration}
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.{CcasLogger, display}
import ccas.utils.errors.NotFoundException
import com.augustnagro.magnum.Transactor
import zio.{Chunk, RIO, URIO, ZIO}

private[membership] object MembershipReport {

  case class ReportResult(
    summaries: List[MemberChangeSummary],
    memberCountAtStart: Int,
    memberCountAtEnd: Int
  )

  def report(clubSlug: ClubSlug, since: Instant, until: Instant): RIO[CcasLogger & Transactor, ReportResult] =
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

  def reportReconciliation(result: ReconciliationResult): URIO[CcasLogger, Unit] = {
    val delta    = result.currentMemberCount - result.previousMemberCount
    val sign     = if (delta >= 0) "+" else ""
    val duration = JDuration.between(result.startedAt, result.completedAt)
    for {
      _ <- CcasLogger.info(s"=== Reconciliation Complete ===")
      _ <- CcasLogger.info(s"Duration:           ${duration.display}")
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

  def formatReconciliation(result: ReconciliationResult): String = {
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
                    |New snapshots:      ${result.newSnapshots.size}
                    |New memberships:    ${result.newMemberships.size}
                    |Closed memberships: ${result.closedMemberships.size}
                    |""".stripMargin
    header + "\n" + formatChangeSummaries(result.changes.toList)
  }

  def formatReport(rr: ReportResult): String = {
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
