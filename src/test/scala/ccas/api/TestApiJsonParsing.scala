package ccas.api

import java.net.URI
import java.nio.file.{Files, Path}

import zio.ZIO
import zio.http.URL
import zio.json.{readJsonLinesAs, DecoderOps, JsonDecoder}
import zio.test.{assertCompletes, assertTrue, Spec, ZIOSpecDefault}

import ccas.api.club.{ApiClub, ApiClubMatches, ApiClubMembers}
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, ApiMatchBoard}
import ccas.api.clubmatch.ApiDailyMatch.{ApiDailyMatchCancelled, ApiDailyMatchFinished}
import ccas.api.misc.enums.PlayerStatus
import ccas.api.misc.subtypes.{ClubSlug, Elo, PlayerId, Username}
import ccas.api.player.*
import ccas.api.tournament.{ApiTournament, ApiTournamentRound}

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
    generateTest[ApiMatchBoard]("matchBoard"),
    generateTest[ApiMatchBoard]("matchBoardOngoing"),
    generateTest[ApiMatchBoard]("matchBoardLive"),
    generateTest[ApiLiveMatch]("liveMatchFinished"),
    generateTest[ApiPlayerTournaments]("playerTournaments"),
    generateTest[ApiTournament]("tournament"),
    generateTest[ApiTournamentRound]("tournamentRound"),
    test("an unknown status fails the whole profile, and the error names the field") {
      readFixture("player").map { body =>
        val decoded = body.replace("\"staff\"", "\"mythic\"").fromJson[ApiPlayer]
        assertTrue(decoded.swap.exists(_.contains(".status")))
      }
    },
    suite("a value we do not know, in a field nothing reads, does not fail the response")(
      ignoresValuesOf[ApiPlayer]("player")("league"),
      ignoresValuesOf[ApiClub]("club")("visibility"),
      ignoresValuesOf[ApiClubMatches]("clubMatches")("result"),
      ignoresValuesOf[ApiPlayerGamesCurrent]("playerGames")("turn", "time_class", "rules"),
      ignoresValuesOf[ApiPlayerArchive]("playerArchive")("rules"),
      ignoresValuesOf[ApiPlayerMatches]("playerMatches")("played_as_white", "played_as_black"),
      ignoresValuesOf[ApiMatchBoard]("matchBoard")("time_class", "rules"),
      ignoresValuesOf[ApiDailyMatch]("matchRegistered")("rules", "status"),
      ignoresValuesOf[ApiDailyMatch]("matchInProgress")("rules", "status"),
      ignoresValuesOf[ApiDailyMatch]("matchFinished")("rules", "result", "status"),
      ignoresValuesOf[ApiDailyMatch]("matchCancelled")("rules", "result", "status"),
      ignoresValuesOf[ApiLiveMatch]("liveMatchFinished")("time_class", "rules", "result", "status")
    ),
    test("rules and visibility come through as Chess.com sends them") {
      for {
        club       <- readJsonLinesAs[ApiClub](getFileName("club")).runHead.someOrFailException
        dailyMatch <- readJsonLinesAs[ApiDailyMatch](getFileName("matchFinished")).runHead.someOrFailException
      } yield assertTrue(club.visibility.contains("public"), dailyMatch.settings.rules.contains("chess"))
    },
    suite("a response without rules or visibility still decodes")(
      decodesWithout[ApiClub]("club", "visibility"),
      decodesWithout[ApiPlayerGamesCurrent]("playerGames", "rules"),
      decodesWithout[ApiPlayerArchive]("playerArchive", "rules"),
      decodesWithout[ApiMatchBoard]("matchBoard", "rules"),
      decodesWithout[ApiDailyMatch]("matchFinished", "rules"),
      decodesWithout[ApiLiveMatch]("liveMatchFinished", "rules")
    ),
    test("a match decodes to its own variant, though a cancelled one reports itself finished") {
      for {
        finished  <- readJsonLinesAs[ApiDailyMatch](getFileName("matchFinished")).runHead.someOrFailException
        cancelled <- readJsonLinesAs[ApiDailyMatch](getFileName("matchCancelled")).runHead.someOrFailException
      } yield assertTrue(finished.isInstanceOf[ApiDailyMatchFinished], cancelled.isInstanceOf[ApiDailyMatchCancelled])
    },
    suite("only a match nobody played reads as cancelled")(
      test("an unknown game result fails a finished match, and the error names the field") {
        readFixture("matchFinished").map { body =>
          val decoded = withUnknownResult(body).fromJson[ApiDailyMatch]
          assertTrue(decoded.swap.exists(_.contains("played_as_white")))
        }
      },
      test("an unknown game result fails a match in progress, and the error names the field") {
        readFixture("matchInProgress").map { body =>
          val decoded = withUnknownResult(body).fromJson[ApiDailyMatch]
          assertTrue(decoded.swap.exists(_.contains("played_as_white")))
        }
      },
      test("one player without a board fails a finished match, and the error names the field") {
        readFixture("matchFinished").map { body =>
          val decoded = body.replaceFirst(boardField, "").fromJson[ApiDailyMatch]
          assertTrue(decoded.swap.exists(_.contains(".board(missing)")))
        }
      },
      test("no boards or game results with a non-zero score fails rather than passing as cancelled") {
        readFixture("matchFinished").map { body =>
          val decoded = body.replaceAll(boardField, "").replaceAll(resultField, "").fromJson[ApiDailyMatch]
          assertTrue(decoded.swap.exists(_.contains(".board(missing)")))
        }
      },
      test("game results with no boards and a 0-0 score fail rather than passing as cancelled") {
        readFixture("matchFinished").map { body =>
          val decoded = withNoScore(body.replaceAll(boardField, "")).fromJson[ApiDailyMatch]
          assertTrue(decoded.swap.exists(_.contains(".board(missing)")))
        }
      },
      test("one side that never played, beside one that did, fails rather than passing as cancelled") {
        readFixture("matchFinished").map { body =>
          val (team1, team2) = body.splitAt(body.indexOf("\"team2\""))
          val neverPlayed    = withNoScore(team1.replaceAll(boardField, "").replaceAll(resultField, ""))
          val decoded        = (neverPlayed + team2).fromJson[ApiDailyMatch]
          assertTrue(decoded.swap.exists(_.contains(".board(missing)")))
        }
      },
      test("boards with a 0-0 score stay a finished match") {
        readFixture("matchFinished").map { body =>
          val changed = withNoScore(body)
          assertTrue(changed != body, changed.fromJson[ApiDailyMatch].exists(_.isInstanceOf[ApiDailyMatchFinished]))
        }
      },
      test("no players on either side decodes as cancelled") {
        readFixture("matchCancelled").map { body =>
          val changed = body.replaceAll(""""players"\s*:\s*\[[^\]]*\]""", """"players":[]""")
          assertTrue(changed != body, changed.fromJson[ApiDailyMatch].exists(_.isInstanceOf[ApiDailyMatchCancelled]))
        }
      },
      test("an aborted status returns a failure instead of throwing") {
        readFixture("matchCancelled").map { body =>
          val decoded = body.replace(""""status":"finished"""", """"status":"aborted"""").fromJson[ApiDailyMatch]
          assertTrue(decoded.isLeft)
        }
      }
    ),
    test("ApiClub.canonicalSlug reads the slug Chess.com answers to, from `@id`") {
      readJsonLinesAs[ApiClub](getFileName("club")).runHead.someOrFailException
        .map(club => assertTrue(club.canonicalSlug == ClubSlug.wrap("chess-com-developer-community")))
    },
    test("ApiClub decodes with or without average_daily_rating") {
      for {
        rated   <- readJsonLinesAs[ApiClub](getFileName("club")).runHead.someOrFailException
        unrated <- readJsonLinesAs[ApiClub](getFileName("clubWithoutRating")).runHead.someOrFailException
      } yield assertTrue(rated.averageDailyRating.contains(Elo.wrap(925)), unrated.averageDailyRating.isEmpty)
    },
    test("ApiMatchBoard.usernames names each player once, a side given as a bare URL by its last path segment") {
      val profile = URL.decode("https://api.chess.com/pub/player/gone-player").toOption.get
      val noPath  = URL.decode("https://api.chess.com").toOption.get
      for {
        board <- readJsonLinesAs[ApiMatchBoard](getFileName("matchBoard")).runHead.someOrFailException
        game = board.games.head
      } yield assertTrue(
        board.usernames == List(Username("wonderfolly"), Username("Melayev_77")),
        game.copy(black = Left(profile)).usernames == List(Username("wonderfolly"), Username("gone-player")),
        game.copy(black = Left(noPath)).usernames == List(Username("wonderfolly"))
      )
    }
  )

  private def getFileName(label: String) = s"data/test/api/$label.json"

  private def readFixture(label: String) = ZIO.attemptBlocking(Files.readString(Path.of(getFileName(label))))

  private def withUnknownResult(body: String) =
    body.replaceFirst(""""played_as_white"\s*:\s*"[^"]*"""", """"played_as_white":"no-such-result"""")

  private def withNoScore(body: String) = body.replaceAll(""""score"\s*:\s*[0-9.]+""", """"score":0""")

  // A player's `board` and game results, with the comma before each: `username` always comes first.
  private val boardField  = """,\s*"board"\s*:\s*"[^"]*""""
  private val resultField = """,\s*"played_as_(?:white|black)"\s*:\s*"[^"]*""""

  // Spares a match's own `status`, which is read: it picks the variant. A field the fixture lacks would prove nothing,
  // so the test also fails if a field's values did not change.
  private def ignoresValuesOf[T: JsonDecoder](label: String)(fields: String*) =
    test(s"$label: ${fields.mkString(", ")}") {
      readFixture(label).map { body =>
        val changed = fields.map { field =>
          val value = s""""$field"\\s*:\\s*"(?!(?:finished|in_progress|registration)")[^"]*""""
          value.r.replaceAllIn(_: String, s""""$field":"no-such-value"""")
        }
        val eachChangesBody = changed.forall(change => change(body) != body)
        assertTrue(eachChangesBody) && decodesAsBefore[T](body, changed.reduce(_ andThen _)(body))
      }
    }

  private def decodesWithout[T: JsonDecoder](label: String, field: String) = test(s"$label: $field") {
    readFixture(label).map { body =>
      val without = body.replaceAll(s""""$field"\\s*:\\s*"[^"]*"\\s*,""", "")
      assertTrue(without != body) && decodesAsBefore[T](body, without)
    }
  }

  // Compares the classes because a change that moved a match to another variant would still decode.
  private def decodesAsBefore[T: JsonDecoder](body: String, changed: String) = {
    val decoded = changed.fromJson[T]
    assertTrue(decoded.isRight, decoded.map(_.getClass) == body.fromJson[T].map(_.getClass))
  }

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
    fide = None
  )
}
