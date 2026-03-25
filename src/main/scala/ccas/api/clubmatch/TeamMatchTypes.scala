package ccas.api.clubmatch

import zio.Chunk
import zio.http.URL

import ccas.api.misc.enums.{GameResultDetail, PlayerStatus}
import ccas.api.misc.subtypes.Username

/** Shared traits for team match structures (daily and live). */

trait TeamMatchTeams {
  val team1: TeamMatchTeam
  val team2: TeamMatchTeam
}

trait TeamMatchTeam {
  val `@id`: URL
  val name: String
  val url: URL
  val score: Double
  val players: Chunk[TeamMatchPlayer]
  val fairPlayRemovals: Set[Username]
}

trait TeamMatchPlayer {
  val username: Username
}

trait TeamMatchPlayerStarted extends TeamMatchPlayer {
  val stats: URL
  val status: PlayerStatus
  val playedAsWhite: Option[GameResultDetail]
  val playedAsBlack: Option[GameResultDetail]
  val board: URL
}
