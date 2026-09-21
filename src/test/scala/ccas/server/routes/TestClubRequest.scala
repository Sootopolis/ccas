package ccas.server.routes

import zio.Chunk
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.ClubQuery
import ccas.api.misc.subtypes.{ClubId, ClubSlug}

/** Pins how a URL names a club: exactly one of `clubId` / `slug`, read back the way the CLI writes it. */
object TestClubRequest extends ZIOSpecDefault {

  private def params(clubIds: String*)(slugs: String*) = ClubRequest.fromParams(Chunk.from(clubIds), Chunk.from(slugs))

  override def spec: Spec[Any, Nothing] = suite("TestClubRequest")(
    test("an id or a name reads back as the query the CLI wrote") {
      val byId   = ClubQuery.ById(ClubId(42L))
      val bySlug = ClubQuery.BySlug(ClubSlug("team-alpha"))
      assertTrue(
        ClubRequest.queryString(byId) == "clubId=42",
        ClubRequest.queryString(bySlug) == "slug=team-alpha",
        params("42")() == Right(byId),
        params()(" Team-Alpha ") == Right(bySlug)
      )
    },
    test("naming no club, naming it twice over, or repeating a parameter is refused rather than narrowed") {
      assertTrue(
        params()().isLeft,
        params("42")("team-alpha").isLeft,
        params("42", "43")().isLeft,
        params()("team-alpha", "team-beta").isLeft
      )
    },
    test("an id that is not a positive integer, or a blank name, is refused") {
      assertTrue(params("0")().isLeft, params("-1")().isLeft, params("abc")().isLeft, params()("  ").isLeft)
    }
  )
}
