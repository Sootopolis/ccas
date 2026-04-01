package ccas.api

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.enums.*

object TestEnumCodecs extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("TestEnumCodecs")(
    suiteGameResultDetail,
    suiteGameRule,
    suitePlayerStatus,
    suiteColour,
    suiteLeague,
    suiteClubMatchResult,
    suiteClubMatchStatus,
    suiteTimeClass,
    suiteTitle,
    suiteClubVisibility
  )

  // --- GameResultDetail: custom lookup, one-way decode from API format ---

  private def suiteGameResultDetail = suite("GameResultDetail")(
    test("50move decodes to FiftyMove") {
      assertTrue("\"50move\"".fromJson[GameResultDetail] == Right(GameResultDetail.FiftyMove))
    },
    test("all values decode from API lowercase format") {
      val cases = List(
        "win"                  -> GameResultDetail.Win,
        "stalemate"            -> GameResultDetail.Stalemate,
        "agreed"               -> GameResultDetail.Agreed,
        "repetition"           -> GameResultDetail.Repetition,
        "insufficient"         -> GameResultDetail.Insufficient,
        "timevsinsufficient"   -> GameResultDetail.TimeVsInsufficient,
        "checkmated"           -> GameResultDetail.Checkmated,
        "resigned"             -> GameResultDetail.Resigned,
        "timeout"              -> GameResultDetail.Timeout,
        "lose"                 -> GameResultDetail.Lose,
        "abandoned"            -> GameResultDetail.Abandoned,
        "kingofthehill"        -> GameResultDetail.KingOfTheHill,
        "threecheck"           -> GameResultDetail.ThreeCheck,
        "bughousepartnerlose"  -> GameResultDetail.BughousePartnerLose
      )
      assertTrue(cases.forall { (jsonStr, expected) =>
        s"\"$jsonStr\"".fromJson[GameResultDetail] == Right(expected)
      })
    },
    test("categories are correctly assigned") {
      assertTrue(
        GameResultDetail.Win.category == GameResult.Win,
        GameResultDetail.Stalemate.category == GameResult.Draw,
        GameResultDetail.FiftyMove.category == GameResult.Draw,
        GameResultDetail.Checkmated.category == GameResult.Loss
      )
    },
    test("invalid value produces error") {
      assertTrue("\"nope\"".fromJson[GameResultDetail].isLeft)
    }
  )

  // --- GameRule: custom lookup, one-way decode from API format ---

  private def suiteGameRule = suite("GameRule")(
    test("all values decode from API lowercase format") {
      val cases = List(
        "chess"         -> GameRule.Chess,
        "chess960"      -> GameRule.Chess960,
        "bughouse"      -> GameRule.Bughouse,
        "kingofthehill" -> GameRule.KingOfTheHill,
        "threecheck"    -> GameRule.ThreeCheck,
        "crazyhouse"    -> GameRule.CrazyHouse,
        "oddschess"     -> GameRule.OddsChess
      )
      assertTrue(cases.forall { (jsonStr, expected) =>
        s"\"$jsonStr\"".fromJson[GameRule] == Right(expected)
      })
    },
    test("invalid value produces error") {
      assertTrue("\"checkers\"".fromJson[GameRule].isLeft)
    }
  )

  // --- PlayerStatus: dual-layer fallback (PascalCase first, then custom lookup) ---

  private def suitePlayerStatus = suite("PlayerStatus")(
    test("standard lowercase values") {
      val cases = List(
        "basic"   -> PlayerStatus.Basic,
        "premium" -> PlayerStatus.Premium,
        "mod"     -> PlayerStatus.Mod,
        "staff"   -> PlayerStatus.Staff,
        "closed"  -> PlayerStatus.Closed
      )
      assertTrue(cases.forall { (jsonStr, expected) =>
        s"\"$jsonStr\"".fromJson[PlayerStatus] == Right(expected)
      })
    },
    test("closed:fair_play_violations → Fairplay") {
      assertTrue("\"closed:fair_play_violations\"".fromJson[PlayerStatus] == Right(PlayerStatus.Fairplay))
    },
    test("closed:abuse → Abuse") {
      assertTrue("\"closed:abuse\"".fromJson[PlayerStatus] == Right(PlayerStatus.Abuse))
    },
    test("roundtrip through encode then decode") {
      assertTrue(PlayerStatus.values.forall { v =>
        v.toJson.fromJson[PlayerStatus] == Right(v)
      })
    },
    test("invalid value produces error") {
      assertTrue("\"banned\"".fromJson[PlayerStatus].isLeft)
    }
  )

  // --- Colour: default PascalCase via EnumJson ---

  private def suiteColour = suite("Colour")(
    test("decodes from snake_case") {
      assertTrue(
        "\"white\"".fromJson[Colour] == Right(Colour.White),
        "\"black\"".fromJson[Colour] == Right(Colour.Black)
      )
    },
    test("roundtrip") {
      assertTrue(Colour.values.forall { v =>
        v.toJson.fromJson[Colour] == Right(v)
      })
    }
  )

  // --- League: default PascalCase ---

  private def suiteLeague = suite("League")(
    test("all values decode") {
      val cases = List(
        "wood"     -> League.Wood,
        "stone"    -> League.Stone,
        "bronze"   -> League.Bronze,
        "silver"   -> League.Silver,
        "crystal"  -> League.Crystal,
        "elite"    -> League.Elite,
        "champion" -> League.Champion,
        "legend"   -> League.Legend
      )
      assertTrue(cases.forall { (jsonStr, expected) =>
        s"\"$jsonStr\"".fromJson[League] == Right(expected)
      })
    },
    test("roundtrip") {
      assertTrue(League.values.forall { v =>
        v.toJson.fromJson[League] == Right(v)
      })
    }
  )

  // --- ClubMatchResult ---

  private def suiteClubMatchResult = suite("ClubMatchResult")(
    test("decodes from snake_case") {
      assertTrue(
        "\"win\"".fromJson[ClubMatchResult] == Right(ClubMatchResult.Win),
        "\"draw\"".fromJson[ClubMatchResult] == Right(ClubMatchResult.Draw),
        "\"lose\"".fromJson[ClubMatchResult] == Right(ClubMatchResult.Lose)
      )
    },
    test("roundtrip") {
      assertTrue(ClubMatchResult.values.forall { v =>
        v.toJson.fromJson[ClubMatchResult] == Right(v)
      })
    }
  )

  // --- ClubMatchStatus ---

  private def suiteClubMatchStatus = suite("ClubMatchStatus")(
    test("decodes from snake_case") {
      assertTrue(
        "\"finished\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.Finished),
        "\"in_progress\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.InProgress),
        "\"registration\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.Registration)
      )
    },
    test("roundtrip") {
      assertTrue(ClubMatchStatus.values.forall { v =>
        v.toJson.fromJson[ClubMatchStatus] == Right(v)
      })
    }
  )

  // --- TimeClass ---

  private def suiteTimeClass = suite("TimeClass")(
    test("all values decode") {
      val cases = List(
        "daily"     -> TimeClass.Daily,
        "rapid"     -> TimeClass.Rapid,
        "standard"  -> TimeClass.Standard,
        "blitz"     -> TimeClass.Blitz,
        "lightning" -> TimeClass.Lightning,
        "bullet"    -> TimeClass.Bullet
      )
      assertTrue(cases.forall { (jsonStr, expected) =>
        s"\"$jsonStr\"".fromJson[TimeClass] == Right(expected)
      })
    },
    test("roundtrip") {
      assertTrue(TimeClass.values.forall { v =>
        v.toJson.fromJson[TimeClass] == Right(v)
      })
    }
  )

  // --- Title: all-caps values, API sends as-is ("GM", "WGM" etc.) ---

  private def suiteTitle = suite("Title")(
    test("all values decode from PascalCase/uppercase") {
      // Chess.com API sends titles as uppercase abbreviations; PascalCase is identity for these
      assertTrue(
        Title.values.forall { t =>
          s"\"${t.toString}\"".fromJson[Title] == Right(t)
        }
      )
    },
    test("specific values") {
      assertTrue(
        "\"GM\"".fromJson[Title] == Right(Title.GM),
        "\"WGM\"".fromJson[Title] == Right(Title.WGM),
        "\"NM\"".fromJson[Title] == Right(Title.NM)
      )
    }
  )

  // --- ClubVisibility ---

  private def suiteClubVisibility = suite("ClubVisibility")(
    test("decodes from snake_case") {
      assertTrue(
        "\"public\"".fromJson[ClubVisibility] == Right(ClubVisibility.Public),
        "\"private\"".fromJson[ClubVisibility] == Right(ClubVisibility.Private)
      )
    },
    test("roundtrip") {
      assertTrue(ClubVisibility.values.forall { v =>
        v.toJson.fromJson[ClubVisibility] == Right(v)
      })
    }
  )
}
