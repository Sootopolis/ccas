package ccas.api.player

import ccas.api.utils.Hosts
import ccas.api.utils.enums.{League, PlayerStatus, Title}
import ccas.api.utils.subtypes.{Elo, PlayerId, Username}
import ccas.utils.json.JsonDecoding
import zio.http.URL
import zio.json.{DeriveJsonDecoder, JsonDecoder, SnakeCase, jsonMemberNames}

import java.time.Instant

@jsonMemberNames(SnakeCase)
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
) {
  val profileUrl: URL = Hosts.website.addPath(s"member/$username")
  val apiUrl: URL = ApiPlayer.getUrl(username)
  val apiStatsUrl: URL = ApiPlayerStats.getUrl(username)
}

object ApiPlayer extends JsonDecoding[ApiPlayer] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayer] = DeriveJsonDecoder.gen

  val host: URL = Hosts.api.addPath("player")

  def getUrl(username: Username): URL = host.addPath(username)
}
