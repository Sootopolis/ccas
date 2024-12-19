package ccas.api.player

import ccas.api.player.ApiPlayerMatches.{ApiPlayerMatchFinished, ApiPlayerMatchInProgress, ApiPlayerMatchRegistered}
import ccas.api.utils.Enums.GameResultDetail
import zio.Chunk
import zio.http.URL
import zio.json.{SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
case class ApiPlayerMatches(
  finished  : Chunk[ApiPlayerMatchFinished],
  inProgress: Chunk[ApiPlayerMatchInProgress],
  registered: Chunk[ApiPlayerMatchRegistered]
)

object ApiPlayerMatches {
  sealed trait ApiPlayerMatch {
    val name: String
    val url: URL
    val `@id`: URL
    val club: URL
  }

  sealed trait ApiPlayerMatchStarted extends ApiPlayerMatch {
    val board: URL
  }

  case class ApiPlayerMatchResults(playedAsWhite: GameResultDetail, playedAsBlack: GameResultDetail)

  case class ApiPlayerMatchRegistered(
    name : String,
    url  : URL,
    `@id`: URL,
    club : URL
  ) extends ApiPlayerMatch

  case class ApiPlayerMatchInProgress(
    name : String,
    url  : URL,
    `@id`: URL,
    club : URL,
    board: URL
  ) extends ApiPlayerMatchStarted

  case class ApiPlayerMatchFinished(
    name   : String,
    url    : URL,
    `@id`  : URL,
    club   : URL,
    board  : URL,
    results: ApiPlayerMatchResults
  ) extends ApiPlayerMatchStarted
}
