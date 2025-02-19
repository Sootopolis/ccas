package ccas.analysis.apps.membership

import ccas.analysis.tables.general.Player
import ccas.api.misc.subtypes.Username
import ccas.api.player.ApiPlayer

sealed trait MembershipChange {
  def report: String
}

object MembershipChange {
  sealed trait WithoutRenaming extends MembershipChange {
    val username: Username

    override def report: String = s"$username ${ ApiPlayer.getProfileUrl(username) }"
  }

  sealed trait WithRenaming extends MembershipChange {
    val oldUsername: Username
    val newUsername: Username

    override def report: String = s"$oldUsername -> $newUsername ${ ApiPlayer.getProfileUrl(newUsername) }"
  }

  case class Unchanged(username: Username) extends WithoutRenaming

  case class Joined(username: Username) extends WithoutRenaming

  case class Returned(username: Username) extends WithoutRenaming

  case class Reopened(username: Username) extends WithoutRenaming

  case class Removed(username: Username) extends WithoutRenaming

  case class Closed(username: Username) extends WithoutRenaming

  case class Banned(username: Username) extends WithoutRenaming

  /** The member has changed their username and is no longer a member of the club, therefore we cannot access
   *  their API endpoint. There is also no way to know the exact circumstance of the member's removal, whether
   *  the member has left the club, got banned, closed their account, or any combination of those.
   *  It's `WithoutRenaming` because we do not know the new username. */
  case class RenamedRemoved(username: Username) extends WithoutRenaming

  case class Renamed(oldUsername: Username, newUsername: Username) extends WithRenaming

  case class RenamedReopened(oldUsername: Username, newUsername: Username) extends WithRenaming

  case class RenamedReturned(oldUsername: Username, newUsername: Username) extends WithRenaming

  /*
    isMember: Boolean,
    wasMember: Boolean,
    isOpen: Boolean,
    wasOpen: Boolean,
    hasRenamed: Boolean,

    true, true, true, true, true => renamed
    true, true, true, true, false => unchanged
    true, true, true, false, true => reopened renamed
    true, true, true, false, false => reopened
    true, true, false, true, true => 
   */
}
