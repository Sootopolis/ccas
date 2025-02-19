package ccas.api.clubmatch

import ccas.api.clubmatch.ApiDailyMatch.{ApiDailyMatchSettings, ApiDailyMatchTeams}
import ccas.api.misc.*
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.{ClubMatchId, Elo, Username}
import ccas.utils.json.JsonDecoding
import zio.Chunk
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonDiscriminator, jsonHint, jsonMemberNames}

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
  override protected val jsonDecoderDerived: JsonDecoder[ApiDailyMatch] = DeriveJsonDecoder.gen

  val host: URL = Hosts.api.addPath("match")

  def getUrl(clubMatchId: ClubMatchId): URL = host.addPath(clubMatchId.toString)

  @jsonMemberNames(SnakeCase) @jsonHint(ClubMatchStatus.Registration.encodeJson.replace("\"", ""))
  case class ApiDailyMatchRegistered(
    `@id`    : URL,
    name     : String,
    url      : URL,
    startTime: Option[Long],
    status   : ClubMatchStatus,
    boards   : Int,
    settings : ApiDailyMatchSettings,
    teams    : ApiDailyMatchTeamsRegistered
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.Registration)
  }

  @jsonMemberNames(SnakeCase) @jsonHint(ClubMatchStatus.InProgress.encodeJson.replace("\"", ""))
  case class ApiDailyMatchInProgress(
    `@id`    : URL,
    name     : String,
    url      : URL,
    startTime: Long,
    status   : ClubMatchStatus,
    boards   : Int,
    settings : ApiDailyMatchSettings,
    teams    : ApiDailyMatchTeamsInProgress
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.InProgress)
  }

  @jsonMemberNames(SnakeCase) @jsonHint(ClubMatchStatus.Finished.encodeJson.replace("\"", ""))
  case class ApiDailyMatchFinished(
    `@id`    : URL,
    name     : String,
    url      : URL,
    startTime: Long,
    endTime  : Long,
    status   : ClubMatchStatus,
    boards   : Int,
    settings : ApiDailyMatchSettings,
    teams    : ApiDailyMatchTeamsFinished
  ) extends ApiDailyMatch derives JsonDecoder {
    require(status == ClubMatchStatus.Finished)
  }

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchSettings(
    rules           : GameRule,
    timeClass       : TimeClass,
    timeControl     : String,
    initialSetup    : Option[String],
    minTeamPlayers  : Option[Int],
    maxTeamPlayers  : Option[Int],
    minRequiredGames: Int,
    minRating       : Option[Elo],
    maxRating       : Option[Elo],
    autoStart       : Option[Boolean]
  ) derives JsonDecoder

  // teams

  sealed trait ApiDailyMatchTeams {
    val team1: ApiDailyMatchTeam
    val team2: ApiDailyMatchTeam
  }

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchTeamsRegistered(team1: ApiDailyMatchTeamRegistered, team2: ApiDailyMatchTeamRegistered)
    extends ApiDailyMatchTeams derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchTeamsInProgress(team1: ApiDailyMatchTeamInProgress, team2: ApiDailyMatchTeamInProgress)
    extends ApiDailyMatchTeams derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchTeamsFinished(team1: ApiDailyMatchTeamFinished, team2: ApiDailyMatchTeamFinished)
    extends ApiDailyMatchTeams derives JsonDecoder

  // team

  sealed trait ApiDailyMatchTeam {
    val `@id`: URL
    val name: String
    val url: URL
    val score: Double
    val players: Chunk[ApiDailyMatchPlayer]
    val fairPlayRemovals: Set[Username]
  }

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchTeamRegistered(
    `@id`           : URL,
    name            : String,
    url             : URL,
    score           : Double,
    players         : Chunk[ApiDailyMatchPlayerRegistered],
    fairPlayRemovals: Set[Username],
    locked          : Boolean
  ) extends ApiDailyMatchTeam derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchTeamInProgress(
    `@id`           : URL,
    name            : String,
    url             : URL,
    score           : Double,
    players         : Chunk[ApiDailyMatchPlayerStarted],
    fairPlayRemovals: Set[Username]
  ) extends ApiDailyMatchTeam derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchTeamFinished(
    `@id`           : URL,
    name            : String,
    url             : URL,
    score           : Double,
    result          : ClubMatchResult,
    players         : Chunk[ApiDailyMatchPlayerStarted],
    fairPlayRemovals: Set[Username]
  ) extends ApiDailyMatchTeam derives JsonDecoder

  // player

  sealed trait ApiDailyMatchPlayer {
    val username: Username
  }

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchPlayerRegistered(
    username      : Username,
    rating        : Elo,
    timeoutPercent: Double,
    rd            : Double,
    status        : PlayerStatus
  ) extends ApiDailyMatchPlayer derives JsonDecoder

  @jsonMemberNames(SnakeCase)
  case class ApiDailyMatchPlayerStarted(
    username     : Username,
    stats        : URL,
    status       : PlayerStatus,
    playedAsWhite: Option[GameResultDetail],
    playedAsBlack: Option[GameResultDetail],
    board        : URL
  ) extends ApiDailyMatchPlayer derives JsonDecoder
}
