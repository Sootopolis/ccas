package ccas.api

import zio.http.URL
import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.subtypes.*

object TestSubtypes extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("TestSubtypes")(
    suiteElo,
    suitePlayerId,
    suiteUsername,
    suiteClubId,
    suiteClubSlug,
    suiteClubMatchId,
    suitePercentage,
    suiteClubAlias,
    suiteTournamentSlug
  )

  // --- Elo (IntCompanion) ---

  private def suiteElo = suite("Elo")(
    test("accepts zero") {
      assertTrue("0".fromJson[Elo] == Right(Elo(0)))
    },
    test("accepts positive") {
      assertTrue("2800".fromJson[Elo] == Right(Elo(2800)))
    },
    test("rejects negative") {
      assertTrue("-1".fromJson[Elo].isLeft)
    },
    test("JSON roundtrip") {
      val elo = Elo(1500)
      assertTrue(elo.toJson.fromJson[Elo] == Right(elo))
    }
  )

  // --- PlayerId (LongCompanion) ---

  private def suitePlayerId = suite("PlayerId")(
    test("accepts zero") {
      assertTrue("0".fromJson[PlayerId] == Right(PlayerId(0)))
    },
    test("rejects negative") {
      assertTrue("-1".fromJson[PlayerId].isLeft)
    },
    test("JSON roundtrip") {
      val pid = PlayerId(12345678L)
      assertTrue(pid.toJson.fromJson[PlayerId] == Right(pid))
    }
  )

  // --- Username (StringKeyCompanion, normalizes to lowercase) ---

  private def suiteUsername = suite("Username")(
    test("normalizes to lowercase") {
      assertTrue(Username("AlIcE") == Username("alice"))
    },
    test("rejects empty string") {
      assertTrue("\"\"".fromJson[Username].isLeft)
    },
    test("JSON decode normalizes") {
      assertTrue("\"BOB\"".fromJson[Username] == Right(Username("bob")))
    },
    test("JSON roundtrip") {
      val u = Username("charlie")
      assertTrue(u.toJson.fromJson[Username] == Right(u))
    }
  )

  // --- ClubId (LongCompanion) ---

  private def suiteClubId = suite("ClubId")(
    test("accepts zero") {
      assertTrue("0".fromJson[ClubId] == Right(ClubId(0)))
    },
    test("rejects negative") {
      assertTrue("-1".fromJson[ClubId].isLeft)
    }
  )

  // --- ClubSlug (StringKeyCompanion, normalizes to lowercase) ---

  private def suiteClubSlug = suite("ClubSlug")(
    test("normalizes to lowercase") {
      assertTrue(ClubSlug("My-Club") == ClubSlug("my-club"))
    },
    test("rejects empty string") {
      assertTrue("\"\"".fromJson[ClubSlug].isLeft)
    },
    test("JSON roundtrip") {
      val s = ClubSlug("devon-chess")
      assertTrue(s.toJson.fromJson[ClubSlug] == Right(s))
    }
  )

  // --- ClubMatchId (LongCompanion + fromUrl) ---

  private def suiteClubMatchId = suite("ClubMatchId")(
    test("accepts zero") {
      assertTrue("0".fromJson[ClubMatchId] == Right(ClubMatchId(0)))
    },
    test("rejects negative") {
      assertTrue("-1".fromJson[ClubMatchId].isLeft)
    },
    test("fromUrl extracts ID from last path segment") {
      val url = URL.decode("https://api.chess.com/pub/match/1650919").toOption.get
      assertTrue(ClubMatchId.fromUrl(url) == ClubMatchId(1650919))
    }
  )

  // --- Percentage (DoubleCompanion) ---

  private def suitePercentage = suite("Percentage")(
    test("accepts 0.0") {
      assertTrue("0.0".fromJson[Percentage] == Right(Percentage(0.0)))
    },
    test("accepts 1.0") {
      assertTrue("1.0".fromJson[Percentage] == Right(Percentage(1.0)))
    },
    test("accepts mid-range") {
      assertTrue("0.5".fromJson[Percentage] == Right(Percentage(0.5)))
    },
    test("rejects below zero") {
      assertTrue("-0.1".fromJson[Percentage].isLeft)
    },
    test("rejects above one") {
      assertTrue("1.1".fromJson[Percentage].isLeft)
    }
  )

  // --- ClubAlias (StringCompanion, no normalization) ---

  private def suiteClubAlias = suite("ClubAlias")(
    test("preserves case") {
      assertTrue(ClubAlias.unwrap(ClubAlias("MyAlias")) == "MyAlias")
    },
    test("rejects empty string") {
      assertTrue("\"\"".fromJson[ClubAlias].isLeft)
    }
  )

  // --- TournamentSlug (StringCompanion + fromUrl, normalizes to lowercase) ---

  private def suiteTournamentSlug = suite("TournamentSlug")(
    test("normalizes to lowercase") {
      assertTrue(TournamentSlug("My-Tourney") == TournamentSlug("my-tourney"))
    },
    test("rejects empty string") {
      assertTrue("\"\"".fromJson[TournamentSlug].isLeft)
    },
    test("fromUrl extracts slug from last path segment") {
      val url = URL.decode("https://api.chess.com/pub/tournament/tourney-abc").toOption.get
      assertTrue(TournamentSlug.fromUrl(url) == TournamentSlug("tourney-abc"))
    }
  )
}
