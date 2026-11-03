package ccas.api

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.enums.*
import ccas.server.scheduler.{MisfirePolicy, TriggerType}

object TestEnumCodecs extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("TestEnumCodecs")(
    suiteGameResultDetail,
    suitePlayerStatus,
    suiteClubMatchStatus,
    suiteTimeClass,
    suiteTitle,
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
