package ccas.analysis.tables.general

import ccas.api.misc.subtypes.{ClubId, ClubUrlName}

import java.time.Instant

case class Club(clubId: ClubId, clubUrlName: ClubUrlName, created: Instant)

object Club {

}
