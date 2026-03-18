package ccas.api.club

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.club.ApiClubMatches.{ApiClubMatchFinished, ApiClubMatchInProgress, ApiClubMatchRegistered}
import ccas.api.misc.enums.{ClubMatchResult, TimeClass}
import ccas.api.misc.subtypes.ClubUrlName
import ccas.utils.json.JsonDecoding

@jsonMemberNames(SnakeCase)
final case class ApiClubMatches(
  finished: Chunk[ApiClubMatchFinished],
  inProgress: Chunk[ApiClubMatchInProgress],
  registered: Chunk[ApiClubMatchRegistered]) {
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
    val startTime: Long
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiClubMatchRegistered(
    name: String,
    `@id`: URL,
    opponent: URL,
    timeClass: TimeClass)
      extends ApiClubMatch derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiClubMatchInProgress(
    name: String,
    `@id`: URL,
    opponent: URL,
    timeClass: TimeClass,
    startTime: Long)
      extends ApiClubMatchStarted derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiClubMatchFinished(
    name: String,
    `@id`: URL,
    opponent: URL,
    timeClass: TimeClass,
    startTime: Long,
    result: ClubMatchResult)
      extends ApiClubMatchStarted derives JsonDecoder

  def getUrl(clubUrlName: ClubUrlName): URL = ApiClub.getUrl(clubUrlName).addPath("matches")
}
