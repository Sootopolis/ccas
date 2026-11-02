package ccas.api

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.enums.*
import ccas.server.scheduler.{MisfirePolicy, TriggerType}

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
    suiteClubVisibility,
    suiteTriggerType,
    suiteMisfirePolicy
  )

  // --- GameResultDetail ---

  private def suiteGameResultDetail = suite("GameResultDetail")(
    test("50move decodes to FiftyMove") {
      assertTrue("\"50move\"".fromJson[GameResultDetail] == Right(GameResultDetail.FiftyMove))
    },
    test("all values decode from API lowercase format") {
      val cases = List(
        "win"                 -> GameResultDetail.Win,
        "stalemate"           -> GameResultDetail.Stalemate,
        "agreed"              -> GameResultDetail.Agreed,
        "repetition"          -> GameResultDetail.Repetition,
        "insufficient"        -> GameResultDetail.Insufficient,
        "timevsinsufficient"  -> GameResultDetail.TimeVsInsufficient,
        "checkmated"          -> GameResultDetail.Checkmated,
        "resigned"            -> GameResultDetail.Resigned,
        "timeout"             -> GameResultDetail.Timeout,
        "lose"                -> GameResultDetail.Lose,
        "abandoned"           -> GameResultDetail.Abandoned,
        "kingofthehill"       -> GameResultDetail.KingOfTheHill,
        "threecheck"          -> GameResultDetail.ThreeCheck,
        "bughousepartnerlose" -> GameResultDetail.BughousePartnerLose
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

  // --- GameRule ---

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

  // --- PlayerStatus ---

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
    test("the bare names fairplay and abuse are refused") {
      assertTrue(
        "\"fairplay\"".fromJson[PlayerStatus].isLeft,
        "\"abuse\"".fromJson[PlayerStatus].isLeft
      )
    },
    test("invalid value produces error") {
      assertTrue("\"banned\"".fromJson[PlayerStatus].isLeft)
    }
  )

  // --- Colour ---

  private def suiteColour = suite("Colour")(
    test("decodes from snake_case") {
      assertTrue(
        "\"white\"".fromJson[Colour] == Right(Colour.White),
        "\"black\"".fromJson[Colour] == Right(Colour.Black)
      )
    }
  )

  // --- League ---

  private def suiteLeague = suite("League")(
    test("all eight leagues decode from the capitalised spelling Chess.com sends") {
      val cases = List(
        "Wood"     -> League.Wood,
        "Stone"    -> League.Stone,
        "Bronze"   -> League.Bronze,
        "Silver"   -> League.Silver,
        "Crystal"  -> League.Crystal,
        "Elite"    -> League.Elite,
        "Champion" -> League.Champion,
        "Legend"   -> League.Legend
      )
      assertTrue(cases.forall { (jsonStr, expected) =>
        s"\"$jsonStr\"".fromJson[League] == Right(expected)
      })
    },
    test("a lower-case league is refused") {
      assertTrue("\"wood\"".fromJson[League].isLeft)
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
    }
  )

  // --- ClubMatchStatus ---

  private def suiteClubMatchStatus = suite("ClubMatchStatus")(
    test("decodes from snake_case") {
      assertTrue(
        "\"finished\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.Finished),
        "\"in_progress\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.InProgress),
        "\"registration\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.Registration),
        "\"aborted\"".fromJson[ClubMatchStatus] == Right(ClubMatchStatus.Aborted)
      )
    },
    test("a Scala-cased match status is refused") {
      assertTrue("\"InProgress\"".fromJson[ClubMatchStatus].isLeft)
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
    }
  )

  // --- Title ---

  private def suiteTitle = suite("Title")(
    test("all titles decode from the upper-case spelling Chess.com sends") {
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
    }
  )

  // --- TriggerType ---

  private def suiteTriggerType = suite("TriggerType")(
    test("interval and cron round-trip in snake_case") {
      val cases = List("interval" -> TriggerType.Interval, "cron" -> TriggerType.Cron)
      assertTrue(cases.forall { (wire, value) =>
        value.toJson == s"\"$wire\"" && s"\"$wire\"".fromJson[TriggerType] == Right(value)
      })
    },
    test("a PascalCase trigger type is refused") {
      assertTrue("\"Cron\"".fromJson[TriggerType].isLeft)
    }
  )

  // --- MisfirePolicy ---

  private def suiteMisfirePolicy = suite("MisfirePolicy")(
    test("skip and catch_up round-trip in snake_case") {
      val cases = List("skip" -> MisfirePolicy.Skip, "catch_up" -> MisfirePolicy.CatchUp)
      assertTrue(cases.forall { (wire, value) =>
        value.toJson == s"\"$wire\"" && s"\"$wire\"".fromJson[MisfirePolicy] == Right(value)
      })
    },
    test("a PascalCase misfire policy is refused") {
      assertTrue("\"CatchUp\"".fromJson[MisfirePolicy].isLeft)
    }
  )
}
