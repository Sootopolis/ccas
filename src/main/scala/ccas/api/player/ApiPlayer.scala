package ccas.api.player

import zio.http.URL
import zio.json.{jsonMemberNames, DeriveJsonDecoder, JsonDecoder, SnakeCase}

import ccas.api.misc.enums.{League, PlayerStatus, Title}
import ccas.api.misc.subtypes.{Elo, PlayerId, Username}
import ccas.api.misc.Hosts
import ccas.utils.json.JsonDecoding
import ccas.utils.prettyprinting.PrettyPrinter

@jsonMemberNames(SnakeCase)
final case class ApiPlayer(
    playerId: PlayerId,       // the non-changing Chess.com ID of this player
    username: Username,       // the username of this player
    name: Option[String],     // (optional) the personal first and last name
    country: URL,             // API location of this player's country's profile
    location: Option[String], // (optional) the city or location
    status: PlayerStatus,     // account status: closed, closed:fair_play_violations, basic, premium, mod, staff
    joined: Long,             // timestamp of registration on Chess.com
    lastOnline: Long,         // timestamp of the most recent login
    title: Option[Title],     // (optional) abbreviation of chess title, if any
    avatar: Option[URL],      // (optional) URL of a 200x200 image
    followers: Int,           // the number of players tracking this player's activity
    isStreamer: Boolean,      // if the member is a Chess.com streamer
    verified: Boolean,
    league: Option[League],
    fide: Option[Elo] // FIDE rating
  ) derives PrettyPrinter {
  val profileUrl: URL  = ApiPlayer.getProfileUrl(username)
  val apiUrl: URL      = ApiPlayer.getUrl(username)
  val apiStatsUrl: URL = ApiPlayerStats.getUrl(username)
}

object ApiPlayer extends JsonDecoding[ApiPlayer] {
  override protected val jsonDecoderDerived: JsonDecoder[ApiPlayer] = DeriveJsonDecoder.gen

  val host: URL = Hosts.api.addPath("player")

  def getUrl(username: Username): URL = host.addPath(username)

  def getProfileUrl(username: Username): URL = Hosts.website.addPath("member").addPath(username)
}
