package ccas.analysis.apps.clubdata

import zio.Chunk
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestClubDataApp extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubDataApp")(
    suiteParseMinAgeArg
  )

  private def suiteParseMinAgeArg = suite("parseMinAgeArg")(
    test("no --min-age flag returns None and unchanged args") {
      val args   = Chunk("club-a", "club-b")
      val result = ClubDataApp.parseMinAgeArg(args)
      assertTrue(result == Right((None, args)))
    },
    test("--min-age with hours returns Some(hours) and strips both") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("club-a", "--min-age", "24"))
      assertTrue(result == Right((Some(24), Chunk("club-a"))))
    },
    test("--min-age at end of args is an error") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("club-a", "--min-age"))
      assertTrue(result.isLeft, result.left.exists(_.contains("--min-age requires")))
    },
    test("--min-age followed by non-integer is an error") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("--min-age", "club-a"))
      assertTrue(result.isLeft, result.left.exists(_.contains("--min-age requires")))
    },
    test("--min-age with zero hours is allowed") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("--min-age", "0"))
      assertTrue(result == Right((Some(0), Chunk.empty)))
    },
    test("--min-age in middle of slug list strips cleanly") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("club-a", "--min-age", "12", "club-b"))
      assertTrue(result == Right((Some(12), Chunk("club-a", "club-b"))))
    }
  )
}
