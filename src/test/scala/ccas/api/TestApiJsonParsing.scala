package ccas.api

import java.net.URI

import zio.http.URL
import zio.json.{readJsonLinesAs, JsonDecoder}
import zio.test.{assertCompletes, assertTrue, Spec, ZIOSpecDefault}

import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.clubmatch.{ApiDailyMatch, ApiDailyMatchBoard}
import ccas.api.misc.enums.{League, PlayerStatus}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.*

object TestApiJsonParsing extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("API JSON parsing tests")(
    generateTest[ApiPlayer]("player")(player),
    generateTest[ApiPlayerStats]("playerStats"),
    generateTest[ApiPlayerGamesCurrent]("playerGames"),
    generateTest[ApiPlayerGamesToMove]("playerGamesToMove"),
    generateTest[ApiPlayerClubs]("playerClubs"),
    generateTest[ApiPlayerMatches]("playerMatches"),
    generateTest[ApiClub]("club"),
    generateTest[ApiClubMatches]("clubMatches"),
    generateTest[ApiClubMembers]("clubMembers"),
    generateTest[ApiDailyMatch]("matchFinished"),
    generateTest[ApiDailyMatch]("matchInProgress"),
    generateTest[ApiDailyMatch]("matchRegistered"),
    generateTest[ApiDailyMatch]("matchCancelled"),
    generateTest[ApiDailyMatchBoard]("matchBoard")
  )

  private def getFileName(label: String) = s"data/test/api/$label.json"

  private def generateTest[T](label: String)(expected: => T)(using decoder: JsonDecoder[T]) = test(label) {
    readJsonLinesAs(getFileName(label)).runHead.someOrFailException.map(x => assertTrue(x == expected))
  }

  private def generateTest[T](label: String)(using decoder: JsonDecoder[T]) = test(label) {
    readJsonLinesAs(getFileName(label))(using decoder).runHead.someOrFailException.as(assertCompletes)
  }

  private val player = ApiPlayer(
    playerId = PlayerId.wrap(41),
    username = Username.wrap("erik"),
    name = Some("Erik"),
    country = URL.fromURI(URI("https://api.chess.com/pub/country/US")).get,
    location = Some("Bay Area, CA"),
    status = PlayerStatus.Staff,
    joined = 1178556600,
    lastOnline = 1735164010,
    title = None,
    avatar = {
      val uri = "https://images.chesscomfiles.com/uploads/v1/user/41.5434c4ff.200x200o.5b102889d835.jpeg"
      Some(URL.decode(uri).toOption.get)
    },
    followers = 7930,
    isStreamer = false,
    verified = false,
    league = Some(League.Silver),
    fide = None
  )
}
