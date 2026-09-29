package ccas.cli

import java.nio.file.Path

import zio.{Console, Duration, ExitCode, Task, UIO, ZIO, ZLayer}
import zio.http.Client

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.cli.config.{ConfigWriter, CurrentClubRef}
import ccas.server.routes.{ClubResult, ClubRoutes}
import ccas.server.routes.ClubRoutes.ClubInfo
import ccas.server.routes.ManagedClubRoutes.ManagedClubResponse
import ccas.utils.client.HttpClientLayer

/** `ccas use-club [slug] [--clear]` — a per-machine pointer at the club that commands target when they omit `--club`.
  *
  * Three modes, chosen by the arguments: no slug prints the current club (the only read path for a value that is
  * otherwise write-only, and which nothing else in the CLI ever echoes); `--clear` drops it; a slug sets it.
  *
  * The config write is the command's identity and always succeeds — it works with the server down, which is why this
  * stays a top-level local command rather than a `ServerCommand` under `club`. On a *set*, and only when a server is
  * reachable within a short timeout, it additionally asks which club the name reaches — as `club show` does — so the
  * pointer can hold that club's id and current name, and fetches the live managed set to refresh the completion cache
  * and say whether that club is one you manage. Both are best-effort and never block the write; only when neither
  * answers does it fall back to the cached club list.
  */
object UseClub {

  // A local pointer write must not hang on a dead server, so the verification probe is tightly bounded; on timeout it is
  // simply treated as "couldn't verify" and the offline path takes over.
  private val VerifyTimeout: Duration = Duration.fromSeconds(2)

  /** Where a set reaches outside the process: the config file holding `current_club`, the completion cache's club list,
    * and the client it asks the server with. Explicit, like `CompletionCache`'s `…In` forms, because [[XdgPaths]] reads
    * environment variables a running JVM cannot rebind — so a test hands it a temp directory and a fake server.
    */
  private[cli] final case class Wiring(configFile: Path, clubsFile: Path, clientLayer: ZLayer[Any, Throwable, Client])

  private[cli] object Wiring {
    def live: Wiring =
      Wiring(configFile = XdgPaths.configFile, clubsFile = XdgPaths.clubsFile, clientLayer = HttpClientLayer.live)
  }

  def run(slugs: List[String], clear: Boolean, currentClubOption: Option[String], server: String): UIO[ExitCode] =
    runWith(Wiring.live)(slugs = slugs, clear = clear, currentClubOption = currentClubOption, server = server)

  private[cli] def runWith(wiring: Wiring)(
    slugs: List[String],
    clear: Boolean,
    currentClubOption: Option[String],
    server: String
  ): UIO[ExitCode] =
    slugs match {
      case Nil if clear      => clearCurrent(wiring, currentClubOption)
      case Nil               => showCurrent(currentClubOption)
      case _ :: Nil if clear => usageError("--clear takes no slug; pass one or the other")
      case single :: Nil     => setCurrent(wiring, single, server)
      case extra             => tooManySlugs(extra)
    }

  // More than one positional is always a mistake, but the *likely* mistake is an option written after the slug, which
  // zio-cli swallows as a positional — so `ccas use-club team-alpha --clear` lands here rather than in the guard above.
  // Naming the offending tokens and the working order turns a silently-wrong action into a fixable message.
  private def tooManySlugs(slugs: List[String]): UIO[ExitCode] = {
    val flagLike = slugs.tail.filter(_.startsWith("-"))
    val hint =
      if (flagLike.nonEmpty) {
        s"; options must come before the slug — try 'ccas use-club ${flagLike.mkString(" ")} ${slugs.head}'"
      } else { "" }
    usageError(s"expected at most one club slug, got ${slugs.size} (${slugs.mkString(", ")})$hint")
  }

  // Exit 2 when unset so a script can branch on it, matching `ConfigCommand.printGet`'s convention for a missing key.
  // A pure read — no network, no cache refresh — so it stays instant and offline.
  private def showCurrent(currentClubOption: Option[String]): UIO[ExitCode] =
    currentClubOption match {
      // Show the human-readable slug, not the stored `<id>:<slug>` form.
      case Some(s) => Console.printLine(CurrentClubRef.parse(s).slug).orDie.as(ExitCode.success)
      case None =>
        Console.printLineError("no current club set; set one with 'ccas use-club <slug>'").orDie.as(ExitCode(2))
    }

  // Clearing an already-absent value is a success, not an error — `--clear` states a desired end state.
  private def clearCurrent(wiring: Wiring, currentClubOption: Option[String]): UIO[ExitCode] =
    currentClubOption match {
      case None => Console.printLine("no current club set").orDie.as(ExitCode.success)
      case Some(s) =>
        ConfigWriter
          .clearCurrentClub(wiring.configFile)
          .foldZIO(saveFailed("clear current club"), _ => cleared(CurrentClubRef.parse(s).slug))
    }

  private def setCurrent(wiring: Wiring, slug: String, server: String): UIO[ExitCode] = {
    // Trim before storing: a slug never has surrounding whitespace, and `ClubSlug.normalize` only lowercases (no trim),
    // so a padded value would otherwise persist and later reach the API path verbatim.
    val s = slug.trim
    if (s.isEmpty) { usageError("club slug must not be blank") }
    else {
      // Write first, unconditionally: the local pointer is the guaranteed effect and must land even offline. It goes in
      // slug-only (no id yet); the verification that follows refreshes the cache, refines the advisory note, and — when
      // the server names the club — upgrades the pointer to the rename-proof `<id>:<slug>` form.
      ConfigWriter
        .setCurrentClub(wiring.configFile, None, s)
        .foldZIO(saveFailed("save current club"), _ => verifyThenConfirm(wiring, s, server))
    }
  }

  private def verifyThenConfirm(wiring: Wiring, slug: String, server: String): UIO[ExitCode] =
    for {
      (resolutionOption, managed) <- resolveName(wiring, server, slug).zipPar(fetchManaged(wiring, server))
      // A live list means the cache is now authoritative for this club too — write it so the false-alarm case (a
      // managed club the cache hadn't yet learned) can't recur, and so completion picks the club up immediately.
      // Only worth caching when the managed set has something in it. Writing an empty list would truncate the cache to
      // zero bytes AND stamp a fresh mtime, which reads as "fresh" to `clubsStale` — suppressing `Dispatcher`'s refresh
      // and its `/api/clubs` fallback for the whole TTL, while `seedClubs` no-ops because the file now exists. That
      // fallback policy is `Dispatcher`'s to apply; this opportunistic write deliberately owns none of it and just
      // steps aside.
      managedSlugs = managed.map(_.map(_.slug)).filter(_.nonEmpty)
      written <- ZIO.foreach(managedSlugs)(CompletionCache.writeClubsIn(wiring.clubsFile, _))
      _       <- ZIO.whenDiscard(written.contains(false))(cacheWriteFailed(wiring.clubsFile))
      settled = settlement(slug, resolutionOption, managed)
      // Best-effort: the slug-only pointer already landed, so a failed upgrade doesn't fail the command.
      _ <- ZIO.foreachDiscard(settled.clubIdOption)(id => upgradePointer(wiring, id, settled.current))
      _ <- ZIO.foreachDiscard(settled.notes)(note)
      _ <- ZIO.whenDiscard(settled.offline)(CompletionCache.readClubsIn(wiring.clubsFile).flatMap(offlineHint(slug, _)))
      _ <- Console.printLine(s"current club set to ${settled.current}").orDie
    } yield ExitCode.success

  /** What a set settles on from the server's two answers: the name the pointer holds, the id it can add, what to tell
    * the user, and whether neither answered — which leaves the cached club list as the only check.
    */
  private final case class Settlement(
    current: String,
    clubIdOption: Option[ClubId],
    notes: List[String],
    offline: Boolean
  )

  // The club the server says the name reaches decides it, under that club's current name, so a former one typed here is
  // not what commands echo afterwards. With no answer to that — a server older than the resolve route, or a resolve
  // still asking Chess.com at the timeout — the managed set is matched by name instead.
  private def settlement(
    slug: String,
    resolutionOption: Option[ClubResolution],
    managed: Option[List[ManagedClubResponse]]
  ): Settlement =
    resolutionOption match {
      case None => byName(slug, managed)
      case Some(resolution) =>
        resolution match {
          case ClubResolution.Known(club)           => reached(club, resolution, managed)
          case ClubResolution.Renamed(club, _)      => reached(club, resolution, managed)
          case ClubResolution.Moved(club, _, _)     => reached(club, resolution, managed)
          case ClubResolution.NotLocal(_)           => unreached(slug, notLocalWarning(slug))
          case ClubResolution.Problematic(_)        => unreached(slug, problematicWarning(slug))
          case ClubResolution.Ambiguous(_, holders) => unreached(slug, ambiguousWarning(slug, holders))
        }
    }

  // Managed status is matched by id, so a former name is not mistaken for an unmanaged club. An unanswered managed list
  // leaves the question open rather than warning, since the club itself is confirmed.
  private def reached(
    club: NamedClub,
    resolution: ClubResolution,
    managed: Option[List[ManagedClubResponse]]
  ): Settlement = {
    val current   = ClubSlug.unwrap(club.slug)
    val unmanaged = managed.exists(clubs => !clubs.exists(_.clubId == ClubId.unwrap(club.clubId)))
    Settlement(
      current = current,
      clubIdOption = Some(club.clubId),
      notes = Dispatcher.nameChangeNote(resolution).toList ++ Option.when(unmanaged)(unmanagedWarning(current)),
      offline = false
    )
  }

  private def byName(slug: String, managed: Option[List[ManagedClubResponse]]): Settlement = {
    val unmatched = managed.exists(clubs => !clubs.exists(namedAs(slug)))
    Settlement(
      current = slug,
      clubIdOption = matchedId(slug, managed),
      notes = Option.when(unmatched)(unmatchedWarning(slug)).toList,
      offline = managed.isEmpty
    )
  }

  private def unreached(slug: String, warning: String): Settlement =
    Settlement(current = slug, clubIdOption = None, notes = List(warning), offline = false)

  // A set never refuses a name, so each warning says what is wrong with it and what that means for the commands that
  // fall back to it: those run without `--club`.
  private def unmanagedWarning(current: String): String =
    s"warning: '$current' is not one of your managed clubs — commands without --club still target it; " +
      s"manage it with 'ccas club add $current'"

  // Matched by name alone, which a former name of a managed club also fails, so this claims no more than it knows.
  private def unmatchedWarning(slug: String): String =
    s"warning: no club you manage is named '$slug' now — commands without --club still target it"

  private def notLocalWarning(slug: String): String =
    s"warning: the server knows no club named '$slug' — check the spelling; commands without --club will fail " +
      "until it does"

  private def problematicWarning(slug: String): String =
    s"warning: the club once named '$slug' has lost its name to another club, and the server hasn't seen what it is " +
      "called now — commands without --club can't reach it until it has"

  // The server's own reason suggests `--club-id`, which `use-club` does not take; it can take a candidate's name.
  private def ambiguousWarning(slug: String, holders: List[ClubRef]): String =
    s"warning: '$slug' was held by ${holders.map(_.display).mkString(", ")}, and nobody holds it now — " +
      "set the one you mean by its current name"

  // The fallback's id: the one managed club showing the typed name. `club.slug` carries no unique index, so two managed
  // clubs can show one name (#254); an ambiguous name leaves the pointer slug-only, for the next submit to backfill.
  private[cli] def matchedId(slug: String, managed: Option[List[ManagedClubResponse]]): Option[ClubId] =
    managed.map(_.filter(namedAs(slug))).collect { case List(club) => ClubId.wrap(club.clubId) }

  // Case-insensitive, since `ClubSlug.normalize` only lowercases: `Team-Alpha` reaches the server as `team-alpha`.
  private def namedAs(slug: String)(club: ManagedClubResponse): Boolean = club.slug.equalsIgnoreCase(slug)

  private def upgradePointer(wiring: Wiring, id: ClubId, slug: String): UIO[Unit] =
    ConfigWriter.setCurrentClub(wiring.configFile, Some(id), slug).ignore

  private def resolveName(wiring: Wiring, server: String, slug: String): UIO[Option[ClubResolution]] =
    probe(wiring, server)(_.getJson[ClubResult[ClubInfo]](ClubRoutes.resolvePath(ClubQuery.BySlug(ClubSlug(slug)))))
      .map(_.map(_.resolution))

  private def fetchManaged(wiring: Wiring, server: String): UIO[Option[List[ManagedClubResponse]]] =
    probe(wiring, server)(_.getJson[List[ManagedClubResponse]]("/api/managed-clubs"))

  // Best-effort: build a client, make one call, bound it, and collapse every failure to None so the caller falls back
  // to the offline path. Each call is bounded on its own, so one slow answer never costs the other.
  //
  // Operator order is load-bearing. `timeout` must sit OUTSIDE `provide`, or the layer's acquire/release runs
  // unbounded; `disconnect` is required because plain `timeout` waits for the interrupted fiber to unwind, and
  // `NettyConnectionPool.createChannel` is uninterruptible — against a black-holed host the "2s" bound measured 31s
  // without it. Same hazard as BodyStore's deadlines, docs/adr/0009-bound-every-body-store-operation.md.
  // `HttpClientLayer` caps connect at 10s (#182), but this probe wants a far tighter interactive cutoff.
  private def probe[A](wiring: Wiring, server: String)(call: CcasApiClient => Task[A]): UIO[Option[A]] =
    CcasApiClient
      .live(server)
      .flatMap(call)
      .provide(wiring.clientLayer)
      .disconnect
      .timeout(VerifyTimeout)
      .catchAllCause(_ => ZIO.none)

  // With no live answer the cache is all we have, so keep its two failure modes apart: a cache we read and that simply
  // doesn't list the slug is a real (if weak) typo hint, while a cache we couldn't read at all is a broken cache — say
  // so rather than staying silent, which would read as "looks fine" in the state we know least about.
  //
  // Both messages say "didn't answer" rather than "was unreachable": `probe` collapses a refused connection, a timeout,
  // a non-2xx status and a decode failure into the same `None`, so asserting unreachability would point an operator
  // away from a server that is up but erroring.
  private[cli] def offlineHint(slug: String, cached: Option[List[String]]): UIO[Unit] =
    cached match {
      case None =>
        note(s"warning: couldn't check '$slug': the server didn't answer, and the club cache couldn't be read")
      case Some(known) =>
        ZIO.whenDiscard(isUnknown(slug, known))(
          note(
            s"warning: the server didn't answer to check '$slug', and it isn't in the cached club list — " +
              "check the spelling"
          )
        )
    }

  // Naming the path is the actionable part: an unwritable cache directory (a root-owned one left by a `sudo ccas` run
  // is the usual cause) otherwise leaves tab-completion silently dead with nothing to pull on. Advisory only — the
  // pointer is already written and the command still succeeds.
  private def cacheWriteFailed(clubsFile: Path): UIO[Unit] =
    note(
      s"warning: could not update the club cache at $clubsFile — " +
        "shell completion won't reflect your managed clubs until that is writable"
    )

  private def cleared(previous: String): UIO[ExitCode] =
    Console.printLine(s"current club cleared (was $previous)").orDie.as(ExitCode.success)

  // An empty cache knows nothing, so it never warns. Case-insensitive because `ClubSlug.normalize` lowercases: a
  // hand-typed `Team-Alpha` reaches the API as `team-alpha` and is not a typo.
  private[cli] def isUnknown(slug: String, known: List[String]): Boolean =
    known.nonEmpty && !known.exists(_.equalsIgnoreCase(slug))

  private def note(message: String): UIO[Unit] = Console.printLineError(message).orDie

  private def usageError(message: String): UIO[ExitCode] =
    Console.printLineError(s"error: $message").orDie.as(ExitCode(2))

  private def saveFailed(action: String)(e: Throwable): UIO[ExitCode] =
    Console.printLineError(s"error: failed to $action: ${rootMessage(e)}").orDie.as(ExitCode(1))

  private def rootMessage(e: Throwable): String = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
}
