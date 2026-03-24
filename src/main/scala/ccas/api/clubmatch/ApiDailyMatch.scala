package ccas.api.clubmatch

import zio.http.URL
import zio.json.{jsonDiscriminator, jsonHint, jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}
import zio.json.ast.Json
import zio.Chunk

import ccas.api.clubmatch.ApiDailyMatch.{ApiDailyMatchSettings, ApiDailyMatchTeams}
import ccas.api.misc.*
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.{ClubMatchId, Elo, Username}
import ccas.utils.json.JsonDecoding

// match
@jsonDiscriminator("status") @jsonMemberNames(SnakeCase)
sealed trait ApiDailyMatch {
  val `@id`: URL
  val name: String
  val url: URL
  val status: ClubMatchStatus
  val boards: Int
  val settings: ApiDailyMatchSettings
  val teams: ApiDailyMatchTeams
}

object ApiDailyMatch extends JsonDecoding[ApiDailyMatch] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiDailyMatch] = {
    val gen       = DeriveJsonDecoder.gen[ApiDailyMatch]
    val cancelled = JsonDecoder[ApiDailyMatchCancelled]
    JsonDecoder[Json].mapOrFail { json =>
      gen.fromJsonAST(json).orElse(cancelled.fromJsonAST(json).map(_.asInstanceOf[ApiDailyMatch]))
    }
  }

  val host: URL = Hosts.api.addPath("match")

  def getUrl(clubMatchId: ClubMatchId): URL = host.addPath(clubMatchId.toString)

  @jsonMemberNames(SnakeCase) @jsonHint(ClubMatchStatus.Registration.encodeJson.replace("\"", ""))
  final case class ApiDailyMatchRegistered(
    `@id`: URL,
    name: String,
    url: URL,
    startTime: Option[Long],
    status: ClubMatchStatus,
    boards: Int,
    settings: ApiDailyMatchSettings,
    teams: ApiDailyMatchTeamsRegistered
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.Registration)
  }

  @jsonMemberNames(SnakeCase) @jsonHint(ClubMatchStatus.InProgress.encodeJson.replace("\"", ""))
  final case class ApiDailyMatchInProgress(
    `@id`: URL,
    name: String,
    url: URL,
    startTime: Long,
    status: ClubMatchStatus,
    boards: Int,
    settings: ApiDailyMatchSettings,
    teams: ApiDailyMatchTeamsInProgress
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.InProgress)
  }

  @jsonMemberNames(SnakeCase) @jsonHint(ClubMatchStatus.Finished.encodeJson.replace("\"", ""))
  final case class ApiDailyMatchFinished(
    `@id`: URL,
    name: String,
    url: URL,
    startTime: Long,
    endTime: Long,
    status: ClubMatchStatus,
    boards: Int,
    settings: ApiDailyMatchSettings,
    teams: ApiDailyMatchTeamsFinished
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.Finished)
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchCancelled(
    `@id`: URL,
    name: String,
    url: URL,
    startTime: Long,
    endTime: Long,
    status: ClubMatchStatus,
    boards: Int,
    settings: ApiDailyMatchSettings,
    teams: ApiDailyMatchTeamsCancelled
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.Finished)
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchSettings(
    rules: GameRule,
    timeClass: TimeClass,
    timeControl: String,
    initialSetup: Option[String],
    minTeamPlayers: Option[Int],
    maxTeamPlayers: Option[Int],
    minRequiredGames: Int,
    minRating: Option[Elo],
    maxRating: Option[Elo],
    autoStart: Option[Boolean]
  ) derives JsonDecoder

  // teams

  sealed trait ApiDailyMatchTeams extends TeamMatchTeams {
    val team1: ApiDailyMatchTeam
    val team2: ApiDailyMatchTeam
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamsRegistered(team1: ApiDailyMatchTeamRegistered, team2: ApiDailyMatchTeamRegistered)
      extends ApiDailyMatchTeams derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamsInProgress(team1: ApiDailyMatchTeamInProgress, team2: ApiDailyMatchTeamInProgress)
      extends ApiDailyMatchTeams derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamsFinished(team1: MatchTeamFinished, team2: MatchTeamFinished)
      extends ApiDailyMatchTeams derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamsCancelled(team1: ApiDailyMatchTeamCancelled, team2: ApiDailyMatchTeamCancelled)
      extends ApiDailyMatchTeams derives JsonDecoder

  // team

  sealed trait ApiDailyMatchTeam extends TeamMatchTeam {
    val `@id`: URL
    val name: String
    val url: URL
    val score: Double
    val players: Chunk[ApiDailyMatchPlayer]
    val fairPlayRemovals: Set[Username]
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamRegistered(
    `@id`: URL,
    name: String,
    url: URL,
    score: Double,
    players: Chunk[ApiDailyMatchPlayerRegistered],
    fairPlayRemovals: Set[Username],
    locked: Boolean
  ) extends ApiDailyMatchTeam derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamInProgress(
    `@id`: URL,
    name: String,
    url: URL,
    score: Double,
    players: Chunk[MatchPlayerStarted],
    fairPlayRemovals: Set[Username]
  ) extends ApiDailyMatchTeam derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class MatchTeamFinished(
    `@id`: URL,
    name: String,
    url: URL,
    score: Double,
    result: ClubMatchResult,
    players: Chunk[MatchPlayerStarted],
    fairPlayRemovals: Set[Username]
  ) extends ApiDailyMatchTeam derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchTeamCancelled(
    `@id`: URL,
    name: String,
    url: URL,
    score: Double,
    result: ClubMatchResult,
    players: Chunk[ApiDailyMatchPlayerCancelled],
    fairPlayRemovals: Set[Username]
  ) extends ApiDailyMatchTeam derives JsonDecoder

  // player

  sealed trait ApiDailyMatchPlayer extends TeamMatchPlayer {
    val username: Username
  }

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchPlayerRegistered(
    username: Username,
    rating: Option[Elo],
    timeoutPercent: Double,
    rd: Double,
    status: PlayerStatus
  ) extends ApiDailyMatchPlayer derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class MatchPlayerStarted(
    username: Username,
    stats: URL,
    status: PlayerStatus,
    playedAsWhite: Option[GameResultDetail],
    playedAsBlack: Option[GameResultDetail],
    board: URL
  ) extends ApiDailyMatchPlayer with TeamMatchPlayerStarted derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  final case class ApiDailyMatchPlayerCancelled(
    username: Username,
    stats: URL,
    status: PlayerStatus
  ) extends ApiDailyMatchPlayer derives JsonDecoder
}
