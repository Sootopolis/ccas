package ccas.api.club

import ccas.api.club.ApiClubMatches.{ApiClubMatchFinished, ApiClubMatchInProgress, ApiClubMatchRegistered}
import ccas.api.utils.enums.{ClubMatchResult, TimeClass}
import ccas.api.utils.subtypes.ClubUrlName
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
case class ApiClubMatches(
  finished  : Chunk[ApiClubMatchFinished],
  inProgress: Chunk[ApiClubMatchInProgress],
  registered: Chunk[ApiClubMatchRegistered]
) {
  def dailyFinished: Chunk[ApiClubMatchFinished] = finished.filter(_.timeClass == TimeClass.Daily)

  def dailyInProgress: Chunk[ApiClubMatchInProgress] = inProgress.filter(_.timeClass == TimeClass.Daily)

  def dailyRegistered: Chunk[ApiClubMatchRegistered] = registered.filter(_.timeClass == TimeClass.Daily)
}

object ApiClubMatches extends JsonDecoding[ApiClubMatches] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiClubMatches] = DeriveJsonDecoder.gen

  sealed trait ApiClubMatch {
    val name: String
    val `@id`: URL
    val opponent: URL
    val timeClass: TimeClass
  }

  sealed trait ApiClubMatchStarted extends ApiClubMatch {
    val startTime: Instant
  }

  @jsonMemberNames(SnakeCase)
  case class ApiClubMatchRegistered(
    name     : String,
    `@id`    : URL,
    opponent : URL,
    timeClass: TimeClass
  ) extends ApiClubMatch derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiClubMatchInProgress(
    name     : String,
    `@id`    : URL,
    opponent : URL,
    timeClass: TimeClass,
    startTime: Instant
  ) extends ApiClubMatchStarted derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiClubMatchFinished(
    name     : String,
    `@id`    : URL,
    opponent : URL,
    timeClass: TimeClass,
    startTime: Instant,
    result   : ClubMatchResult
  ) extends ApiClubMatchStarted derives JsonDecoder

  def getUrl(clubUrlName: ClubUrlName): URL = ApiClub.getUrl(clubUrlName).addPath("matches")
}
