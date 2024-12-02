package ccas.api.player

import ccas.api.Hosts
import ccas.api.utils.Enums.{League, PlayerStatus, Title}
import ccas.api.utils.Subtypes.{Elo, PlayerId, Username}
import ccas.utils.PrettyPrinting
import zio.http.URL

import java.time.Instant

case class ApiPlayer(
  playerId  : PlayerId, // the non-changing Chess.com ID of this player
  username  : Username, // the username of this player
  name      : Option[String], // (optional) the personal first and last name
  country   : URL, // API location of this player's country's profile
  location  : Option[String], // (optional) the city or location
  status    : PlayerStatus, // account status: closed, closed:fair_play_violations, basic, premium, mod, staff
  joined    : Instant, // timestamp of registration on Chess.com
  lastOnline: Instant, // timestamp of the most recent login
  title     : Option[Title], // (optional) abbreviation of chess title, if any
  avatar    : Option[URL], // (optional) URL of a 200x200 image
  followers : Int, // the number of players tracking this player's activity
  isStreamer: Boolean, //if the member is a Chess.com streamer
  verified  : Boolean,
  league    : League,
  fide      : Option[Elo], // FIDE rating
) extends PrettyPrinting[ApiPlayer] {
  val profileUrl: URL = Hosts.website.addPath(s"member/$username")
  val apiUrl: URL = ApiPlayer.getUrl(username)
  val apiStatsUrl: URL = ApiPlayerStats.getUrl(username)
}

object ApiPlayer extends App {
  val host: URL = Hosts.api.addPath("player")

  def getUrl(username: Username): URL = host.addPath(username)
}
