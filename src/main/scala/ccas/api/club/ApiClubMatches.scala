package ccas.api.club

import ccas.api.club.ApiClubMatches.{ApiClubMatchFinished, ApiClubMatchInProgress, ApiClubMatchRegistered}
import ccas.api.utils.Enums.{ClubMatchResult, TimeClass}
import ccas.api.utils.Subtypes.ClubName
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

import java.time.Instant

case class ApiClubMatches(
  finished  : Chunk[ApiClubMatchFinished],
  inProgress: Chunk[ApiClubMatchInProgress],
  registered: Chunk[ApiClubMatchRegistered]
) extends PrettyPrinting[ApiClubMatches] {
  def dailyFinished: Chunk[ApiClubMatchFinished] = finished.filter(_.timeClass == TimeClass.daily)

  def dailyInProgress: Chunk[ApiClubMatchInProgress] = inProgress.filter(_.timeClass == TimeClass.daily)

  def dailyRegistered: Chunk[ApiClubMatchRegistered] = registered.filter(_.timeClass == TimeClass.daily)
}

object ApiClubMatches {
  sealed trait ApiClubMatch {
    val name: String
    val `@id`: URL
    val opponent: URL
    val timeClass: TimeClass
  }

  sealed trait ApiClubMatchStarted extends ApiClubMatch {
    val startTime: Instant
  }

  case class ApiClubMatchRegistered(
    name     : String,
    `@id`    : URL,
    opponent : URL,
    timeClass: TimeClass
  ) extends ApiClubMatch

  case class ApiClubMatchInProgress(
    name     : String,
    `@id`    : URL,
    opponent : URL,
    timeClass: TimeClass,
    startTime: Instant
  ) extends ApiClubMatchStarted

  case class ApiClubMatchFinished(
    name     : String,
    `@id`    : URL,
    opponent : URL,
    timeClass: TimeClass,
    startTime: Instant,
    result   : ClubMatchResult
  ) extends ApiClubMatchStarted

  def getUrl(clubName: ClubName): URL = ApiClub.getUrl(clubName).addPath("matches")
}
