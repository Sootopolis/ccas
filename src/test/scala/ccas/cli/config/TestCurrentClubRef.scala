package ccas.cli.config

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}

/** Pure tests for the `current_club` pointer: its parser/renderer, and the two decisions a submit's answer feeds —
  * whether the pointer may be refreshed, and whether a club that came back missing is this one. Pins the id-vs-slug
  * split: an all-digit prefix before the first colon is the stable id, anything else degrades to a bare slug so
  * legacy and hand-edited values keep working.
  */
object TestCurrentClubRef extends ZIOSpecDefault {

  private def resolved(clubId: Long, slug: String): Option[NamedClub] = Some(NamedClub(ClubId(clubId), ClubSlug(slug)))

  override def spec: Spec[Any, Nothing] = suite("TestCurrentClubRef")(
    test("parses <id>:<slug> into the id and slug") {
      val ref = CurrentClubRef.parse("1234:team-alpha")
      assertTrue(ref.clubIdOption.contains(ClubId.wrap(1234L)), ref.slug == "team-alpha")
    },
    test("a bare slug has no id") {
      val ref = CurrentClubRef.parse("team-alpha")
      assertTrue(ref.clubIdOption.isEmpty, ref.slug == "team-alpha")
    },
    test("a non-numeric prefix is treated as a whole slug, not an id") {
      // Slugs never contain a colon, so this only guards hand-edited values; keep the whole thing as the slug.
      val ref = CurrentClubRef.parse("abc:team-alpha")
      assertTrue(ref.clubIdOption.isEmpty, ref.slug == "abc:team-alpha")
    },
    test("a negative prefix is rejected as an id (ClubId is >= 0) and kept as a slug") {
      val ref = CurrentClubRef.parse("-5:team-alpha")
      assertTrue(ref.clubIdOption.isEmpty, ref.slug == "-5:team-alpha")
    },
    test("an empty slug part falls back to the whole value as the slug") {
      val ref = CurrentClubRef.parse("1234:")
      assertTrue(ref.clubIdOption.isEmpty, ref.slug == "1234:")
    },
    test("surrounding whitespace is trimmed") {
      val ref = CurrentClubRef.parse("  99:team-a  ")
      assertTrue(ref.clubIdOption.contains(ClubId.wrap(99L)), ref.slug == "team-a")
    },
    test("render round-trips both forms") {
      assertTrue(
        CurrentClubRef(Some(ClubId.wrap(7L)), "team-a").render == "7:team-a",
        CurrentClubRef(None, "team-a").render == "team-a"
      )
    },
    suite("refreshedRef (the write-back decision)")(
      test("id match + renamed slug rewrites to the canonical <id>:<slug>") {
        // current_club is 5:team-a; server says id 5 is now team-a-new (a rename). Target carried the id.
        val next =
          CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", resolved(5, "team-a-new"))
        assertTrue(next.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a-new")))
      },
      test("slug-only current_club submitted bare backfills the id") {
        // current_club is a bare slug; a bare command resolved it by slug to id 5. Target carried no id.
        val next = CurrentClubRef.refreshedRef(Some("team-a"), targetHasId = false, "team-a", resolved(5, "team-a"))
        assertTrue(next.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a")))
      },
      test("an explicit --club naming the current slug still backfills (intended)") {
        // Explicit --club team-a (no id) matches the slug-only current_club; it names the same club, so refresh it.
        val next = CurrentClubRef.refreshedRef(Some("team-a"), targetHasId = false, "team-a", resolved(5, "team-a"))
        assertTrue(next.isDefined)
      },
      test("no change when already canonical") {
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", resolved(5, "team-a"))
        assertTrue(next.isEmpty)
      },
      test("a different club (id and slug both differ) does not touch current_club") {
        // current_club is 5:team-a; we submitted an explicit --club team-b (id 9). Not the current club.
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = false, "team-b", resolved(9, "team-b"))
        assertTrue(next.isEmpty)
      },
      test("a same-slug id-carrying target for a DIFFERENT id does not match via slug") {
        // Target carried an id (from a different current-club form), so the slug branch is off; ids differ → no write.
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", resolved(9, "team-a"))
        assertTrue(next.isEmpty)
      },
      test("an unresolved submit or an unset current_club never writes") {
        assertTrue(
          CurrentClubRef.refreshedRef(Some("5:team-a"), targetHasId = true, "team-a", None).isEmpty,
          CurrentClubRef.refreshedRef(None, targetHasId = false, "team-a", resolved(5, "team-a")).isEmpty
        )
      },
      // The pointer must not follow a name that has moved to someone else's club, or it would silently change which
      // club bare commands mean (#254).
      test("a Moved club is never a refresh target, though every other club that ran is") {
        val ours    = NamedClub(ClubId(5), ClubSlug("team-a"))
        val theirs  = NamedClub(ClubId(9), ClubSlug("team-b"))
        val moved   = ClubResolution.Moved(theirs, ClubSlug("team-a"), List(ClubRef.of(ours)))
        assertTrue(
          CurrentClubRef.refreshTarget(moved).isEmpty,
          CurrentClubRef.refreshTarget(ClubResolution.Known(ours)).contains(ours),
          CurrentClubRef.refreshTarget(ClubResolution.Renamed(ours, ClubSlug("was-team-a"))).contains(ours),
          CurrentClubRef.refreshTarget(ClubResolution.NotLocal(ClubQuery.BySlug(ClubSlug("team-a")))).isEmpty
        )
      }
    ),
    // A pointer with an id is addressed by it, so that is what its miss comes back naming — matching only by slug
    // would miss exactly when the slug is the stale part. A name the pointer also answers to still counts: an
    // explicit `--club` naming the current club says as much about the pointer as a bare command does.
    suite("names (does a missing club mean this pointer?)")(
      test("a pointer carrying an id is named by that id, and by a name it still answers to") {
        val pointer = CurrentClubRef(Some(ClubId(42)), "team-alpha")
        assertTrue(
          pointer.names(ClubQuery.ById(ClubId(42))),
          !pointer.names(ClubQuery.ById(ClubId(43))),
          pointer.names(ClubQuery.BySlug(ClubSlug("team-alpha")))
        )
      },
      test("a bare-slug pointer is named by its slug alone, case-insensitively") {
        val bare = CurrentClubRef(None, "team-alpha")
        assertTrue(
          bare.names(ClubQuery.BySlug(ClubSlug("TEAM-ALPHA"))),
          !bare.names(ClubQuery.BySlug(ClubSlug("other"))),
          !bare.names(ClubQuery.ById(ClubId(42)))
        )
      }
    ),
    // Clearing the pointer when its club stops being managed must not follow a name to someone else's club.
    suite("means (is a removed club this pointer's?)")(
      test("a pointer carrying an id means that club alone, even when the name it was set with has moved on") {
        val pointer = CurrentClubRef(Some(ClubId(42)), "team-alpha")
        val ours    = NamedClub(ClubId(42), ClubSlug("team-alpha-renamed"))
        val theirs  = NamedClub(ClubId(43), ClubSlug("team-alpha"))
        assertTrue(
          pointer.means(ours, ClubQuery.BySlug(ClubSlug("team-alpha-renamed"))),
          !pointer.means(theirs, ClubQuery.BySlug(ClubSlug("team-alpha")))
        )
      },
      test("a bare-slug pointer means the club it names now, or the one it was reached by") {
        val bare    = CurrentClubRef(None, "team-alpha")
        val renamed = NamedClub(ClubId(42), ClubSlug("team-alpha-renamed"))
        assertTrue(
          bare.means(NamedClub(ClubId(42), ClubSlug("team-alpha")), ClubQuery.ById(ClubId(42))),
          bare.means(renamed, ClubQuery.BySlug(ClubSlug("team-alpha"))),
          !bare.means(renamed, ClubQuery.ById(ClubId(42)))
        )
      }
    )
  )
}
