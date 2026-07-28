package ccas.cli.config

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.subtypes.ClubId

/** Pure tests for the `current_club` value parser/renderer. Pins the id-vs-slug split: an all-digit prefix before the
  * first colon is the stable id, anything else degrades to a bare slug so legacy and hand-edited values keep working.
  */
object TestCurrentClubRef extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestCurrentClubRef")(
    test("parses <id>:<slug> into the id and slug") {
      val ref = CurrentClubRef.parse("1234:team-alpha")
      assertTrue(ref.clubId.contains(ClubId.wrap(1234L)), ref.slug == "team-alpha")
    },
    test("a bare slug has no id") {
      val ref = CurrentClubRef.parse("team-alpha")
      assertTrue(ref.clubId.isEmpty, ref.slug == "team-alpha")
    },
    test("a non-numeric prefix is treated as a whole slug, not an id") {
      // Slugs never contain a colon, so this only guards hand-edited values; keep the whole thing as the slug.
      val ref = CurrentClubRef.parse("abc:team-alpha")
      assertTrue(ref.clubId.isEmpty, ref.slug == "abc:team-alpha")
    },
    test("a negative prefix is rejected as an id (ClubId is >= 0) and kept as a slug") {
      val ref = CurrentClubRef.parse("-5:team-alpha")
      assertTrue(ref.clubId.isEmpty, ref.slug == "-5:team-alpha")
    },
    test("an empty slug part falls back to the whole value as the slug") {
      val ref = CurrentClubRef.parse("1234:")
      assertTrue(ref.clubId.isEmpty, ref.slug == "1234:")
    },
    test("surrounding whitespace is trimmed") {
      val ref = CurrentClubRef.parse("  99:team-a  ")
      assertTrue(ref.clubId.contains(ClubId.wrap(99L)), ref.slug == "team-a")
    },
    test("render round-trips both forms") {
      assertTrue(
        CurrentClubRef.render(Some(ClubId.wrap(7L)), "team-a") == "7:team-a",
        CurrentClubRef.render(None, "team-a") == "team-a"
      )
    },
    suite("refreshedRef (the write-back decision)")(
      test("id match + renamed slug rewrites to the canonical <id>:<slug>") {
        // current_club is 5:team-a; server says id 5 is now team-a-new (a rename). Target carried the id.
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", Some(5L), Some("team-a-new"))
        assertTrue(next.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a-new")))
      },
      test("slug-only current_club submitted bare backfills the id") {
        // current_club is a bare slug; a bare command resolved it by slug to id 5. Target carried no id.
        val next = CurrentClubRef.refreshedRef(Some("team-a"), targetHasId = false, "team-a", Some(5L), Some("team-a"))
        assertTrue(next.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a")))
      },
      test("an explicit --club naming the current slug still backfills (intended)") {
        // Explicit --club team-a (no id) matches the slug-only current_club; it names the same club, so refresh it.
        val next = CurrentClubRef.refreshedRef(Some("team-a"), targetHasId = false, "team-a", Some(5L), Some("team-a"))
        assertTrue(next.isDefined)
      },
      test("no change when already canonical") {
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", Some(5L), Some("team-a"))
        assertTrue(next.isEmpty)
      },
      test("a different club (id and slug both differ) does not touch current_club") {
        // current_club is 5:team-a; we submitted an explicit --club team-b (id 9). Not the current club.
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = false, "team-b", Some(9L), Some("team-b"))
        assertTrue(next.isEmpty)
      },
      test("a same-slug id-carrying target for a DIFFERENT id does not match via slug") {
        // Target carried an id (from a different current-club form), so the slug branch is off; ids differ → no write.
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", Some(9L), Some("team-a"))
        assertTrue(next.isEmpty)
      },
      test("a resolution miss (no canonical) never writes") {
        assertTrue(
          CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", None, None).isEmpty,
          CurrentClubRef.refreshedRef(None, targetHasId = false, "team-a", Some(5L), Some("team-a")).isEmpty
        )
      }
    )
  )
}
