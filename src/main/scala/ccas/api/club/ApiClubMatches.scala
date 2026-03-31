package ccas.api.club

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}
import zio.Chunk

import ccas.api.club.ApiClubMatches.{ApiClubMatchFinished, ApiClubMatchInProgress, ApiClubMatchRegistered}
import ccas.api.misc.enums.{ClubMatchResult, TimeClass}
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.json.JsonDecoding.given

@jsonMemberNames(SnakeCase)
final case class ApiClubMatches(
  finished: Chunk[ApiClubMatchFinished],
  inProgress: Chunk[ApiClubMatchInProgress],
  registered: Chunk[ApiClubMatchRegistered]
) derives JsonDecoder {
  def dailyFinished: Chunk[ApiClubMatchFinished] = finished.filter(_.timeClass == TimeClass.Daily)

  def dailyInProgress: Chunk[ApiClubMatchInProgress] = inProgress.filter(_.timeClass == TimeClass.Daily)

  def dailyRegistered: Chunk[ApiClubMatchRegistered] = registered.filter(_.timeClass == TimeClass.Daily)
}

object ApiClubMatches {
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
  final case class ApiClubMatchRegistered(name: String, `@id`: URL, opponent: URL, timeClass: TimeClass)
      extends ApiClubMatch derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiClubMatchInProgress(
    name: String,
    `@id`: URL,
    opponent: URL,
    timeClass: TimeClass,
    startTime: Long
  ) extends ApiClubMatchStarted derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiClubMatchFinished(
    name: String,
    `@id`: URL,
    opponent: URL,
    timeClass: TimeClass,
    startTime: Long,
    result: ClubMatchResult
  ) extends ApiClubMatchStarted derives JsonDecoder

  def getUrl(clubSlug: ClubSlug): URL = ApiClub.getUrl(clubSlug).addPath("matches")
}
