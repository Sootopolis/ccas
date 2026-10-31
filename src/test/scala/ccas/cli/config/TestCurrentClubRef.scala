package ccas.cli.config

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}

/** Pure tests for the `current_club` pointer: its parser/renderer, and the decisions a command's answer feeds — whether
  * a club it reached is this pointer's, to refresh or clear it, and whether a club that came back missing is. Pins the
  * id-vs-slug split: an all-digit prefix before the first colon is the stable id, anything else degrades to a bare slug
  * so legacy and hand-edited values keep working.
  */
object TestCurrentClubRef extends ZIOSpecDefault {

  private def resolved(clubId: Long, slug: String): Option[NamedClub] = Some(NamedClub(ClubId(clubId), ClubSlug(slug)))

  private def byId(clubId: Long): ClubQuery = ClubQuery.ById(ClubId(clubId))

  private def bySlug(slug: String): ClubQuery = ClubQuery.BySlug(ClubSlug(slug))

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
    // Which club the pointer means is `means`'s question, pinned in its own suite; these pin what is written once it
    // has answered.
    suite("refreshedRef (the write-back decision)")(
      test("an id pointer takes its club's new name") {
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), byId(5), resolved(5, "team-a-new"))
        assertTrue(next.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a-new")))
      },
      test("a bare-slug pointer takes the id, and the name now, of the club it means") {
        val backfilled = CurrentClubRef.refreshedRef(Some("team-a"), bySlug("team-a"), resolved(5, "team-a"))
        val healed     = CurrentClubRef.refreshedRef(Some("team-a"), bySlug("team-a"), resolved(5, "team-a-new"))
        assertTrue(
          backfilled.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a")),
          healed.contains(CurrentClubRef(Some(ClubId.wrap(5L)), "team-a-new"))
        )
      },
      test("no change when already canonical") {
        val next = CurrentClubRef.refreshedRef(Some("5:team-a"), byId(5), resolved(5, "team-a"))
        assertTrue(next.isEmpty)
      },
      // An id pointer's slug is only a label, which can lag: a name it still shows may have passed to another club.
      test("a club the pointer does not mean never touches current_club, even one holding the name it shows") {
        assertTrue(
          CurrentClubRef.refreshedRef(Some("5:team-a"), bySlug("team-a"), resolved(9, "team-a")).isEmpty,
          CurrentClubRef.refreshedRef(Some("team-a"), byId(5), resolved(5, "team-x")).isEmpty
        )
      },
      test("an unresolved submit or an unset current_club never writes") {
        assertTrue(
          CurrentClubRef.refreshedRef(Some("5:team-a"), byId(5), None).isEmpty,
          CurrentClubRef.refreshedRef(None, bySlug("team-a"), resolved(5, "team-a")).isEmpty
        )
      },
      // The pointer must not follow a name that has moved to someone else's club, or it would silently change which
      // club bare commands mean (#254).
      test("a Moved club is never a refresh target, though every other club that ran is") {
        val ours   = NamedClub(ClubId(5), ClubSlug("team-a"))
        val theirs = NamedClub(ClubId(9), ClubSlug("team-b"))
        val moved  = ClubResolution.Moved(theirs, ClubSlug("team-a"), List(ClubRef.of(ours)))
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
    // Clearing the pointer when its club stops being managed, and refreshing it when a command reaches its club, must
    // not follow a name to someone else's club.
    suite("means (is a removed or refreshed club this pointer's?)")(
      test("a pointer carrying an id means that club alone, however a command named it") {
        val pointer = CurrentClubRef(Some(ClubId(42)), "team-alpha")
        val ours    = NamedClub(ClubId(42), ClubSlug("team-alpha-renamed"))
        val theirs  = NamedClub(ClubId(43), ClubSlug("team-alpha"))
        assertTrue(
          pointer.means(ours, byId(42)),
          pointer.means(ours, bySlug("team-alpha-renamed")),
          !pointer.means(theirs, bySlug("team-alpha")),
          !pointer.means(theirs, byId(43))
        )
      },
      // A bare pointer sends its name, so it reaches whichever club holds that name now. An `--all` run names each
      // club by id, and from cron it may be the only command that ever backfills the pointer.
      test("a bare-slug pointer means the club holding its name now, however a command named it") {
        val bare   = CurrentClubRef(None, "team-alpha")
        val holder = NamedClub(ClubId(42), ClubSlug("team-alpha"))
        assertTrue(
          bare.means(holder, bySlug("team-alpha")),
          bare.means(holder, byId(42)),
          bare.means(holder, bySlug("team-alpha-old"))
        )
      },
      test("a bare-slug pointer that is a former name means the club a command reached by it") {
        val bare = CurrentClubRef(None, "team-alpha")
        assertTrue(bare.means(NamedClub(ClubId(42), ClubSlug("team-alpha-renamed")), bySlug("team-alpha")))
      },
      // Once the club holds another name, the pointer's name may reach another club, so this one is not provably it.
      test("a bare-slug pointer does not mean a club holding another name that a command named some other way") {
        val bare    = CurrentClubRef(None, "team-alpha")
        val renamed = NamedClub(ClubId(42), ClubSlug("team-alpha-renamed"))
        assertTrue(!bare.means(renamed, byId(42)), !bare.means(renamed, bySlug("team-beta")))
      }
    )
  )
}
