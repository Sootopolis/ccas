package ccas.api.clubmatch

import zio.http.URL
import zio.Chunk

import ccas.api.misc.enums.{GameResultDetail, PlayerStatus}
import ccas.api.misc.subtypes.{ClubSlug, Username}

/** Shared traits for team match structures (daily and live). */

trait TeamMatchTeams {
  val team1: TeamMatchTeam
  val team2: TeamMatchTeam

  def findPlayerIsTeam1(username: Username): Option[Boolean] =
    if (team1.players.exists(_.username == username)) { Some(true) }
    else if (team2.players.exists(_.username == username)) { Some(false) }
    else { None }

  def findClubIsTeam1(slug: ClubSlug): Option[Boolean] =
    if (ClubSlug.fromUrlOption(team1.`@id`).contains(slug)) { Some(true) }
    else if (ClubSlug.fromUrlOption(team2.`@id`).contains(slug)) { Some(false) }
    else { None }
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
