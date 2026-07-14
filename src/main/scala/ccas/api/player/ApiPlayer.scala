package ccas.api.player

import java.time.Instant

import zio.http.URL
import zio.json.{jsonMemberNames, JsonDecoder, SnakeCase}

import ccas.api.misc.enums.{League, PlayerStatus, Title}
import ccas.api.misc.subtypes.{Elo, PlayerId, Username}
import ccas.api.misc.Hosts
import ccas.utils.json.JsonDecoding.given

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
) derives JsonDecoder {
  val joinedAt: Instant     = Instant.ofEpochSecond(joined)
  val lastOnlineAt: Instant = Instant.ofEpochSecond(lastOnline)
}

object ApiPlayer {
  val host: URL = Hosts.api.addPath("player")

  def getUrl(username: Username): URL = host.addPath(username.value)

  def getProfileUrl(username: Username): URL = Hosts.website.addPath("member").addPath(username.value)

  // A `<username> <profile-url>` review line — the shared building block for the recruitment out-file detail and the
  // CLI `--report` / confirm-prompt listings, so an operator sees the same clickable form everywhere.
  def profileLine(username: Username): String = s"$username ${getProfileUrl(username)}"

  // The recruitment review block shared by the out file and the CLI `--report` / confirm delivery: a space-separated
  // username line (neat and paste-ready for invites), a blank line, then one `<username> <profile-url>` line per player
  // for manual inspection. One source so the file and clipboard/console render identically.
  def profileReviewBlock(usernames: List[Username]): String =
    usernames.mkString(" ") + "\n\n" + usernames.map(profileLine).mkString("\n")
}
