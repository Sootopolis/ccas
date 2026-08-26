package ccas.cli

import zio.{Console, Duration, ExitCode, UIO, ZIO}

import ccas.api.misc.subtypes.ClubId
import ccas.cli.config.{ConfigWriter, CurrentClubRef}
import ccas.server.routes.ManagedClubRoutes.ManagedClubResponse
import ccas.utils.client.HttpClientLayer

/** `ccas use-club [slug] [--clear]` — a per-machine pointer at the club that commands target when they omit `--club`.
  *
  * Three modes, chosen by the arguments: no slug prints the current club (the only read path for a value that is
  * otherwise write-only, and which nothing else in the CLI ever echoes); `--clear` drops it; a slug sets it.
  *
  * The config write is the command's identity and always succeeds — it works with the server down, which is why this
  * stays a top-level local command rather than a `ServerCommand` under `club`. On a *set*, and only when a server is
  * reachable within a short timeout, it additionally fetches the live managed set to (a) refresh the completion cache so
  * it isn't stale for the club just chosen, and (b) give a definitive "not one of your managed clubs" note instead of a
  * guess against a possibly-stale cache. Both are best-effort: any transport error, or an unreachable server, falls back
  * to the offline cache hint and never blocks the write. Verification is managed-vs-not only; distinguishing a typo from
  * a real-but-unmanaged club needs a single-club existence check the server does not yet expose.
  */
object UseClub {

  // A local pointer write must not hang on a dead server, so the verification probe is tightly bounded; on timeout it is
  // simply treated as "couldn't verify" and the offline path takes over.
  private val VerifyTimeout: Duration = Duration.fromSeconds(2)

  /** Outcome of the best-effort managed-set check. `Unverified` means the server couldn't be reached (or timed out), not
    * that the club is bad — the offline cache hint stands in for it.
    */
  private[cli] enum Verify {
    case Managed, Unmanaged, Unverified
  }

  def run(slugs: List[String], clear: Boolean, currentClub: Option[String], server: String): UIO[ExitCode] =
    slugs match {
      case Nil if clear       => clearCurrent(currentClub)
      case Nil                => showCurrent(currentClub)
      case _ :: Nil if clear  => usageError("--clear takes no slug; pass one or the other")
      case single :: Nil      => setCurrent(single, server)
      case extra              => tooManySlugs(extra)
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
  private def showCurrent(currentClub: Option[String]): UIO[ExitCode] =
    currentClub match {
      // Show the human-readable slug, not the stored `<id>:<slug>` form.
      case Some(s) => Console.printLine(CurrentClubRef.parse(s).slug).orDie.as(ExitCode.success)
      case None =>
        Console.printLineError("no current club set; set one with 'ccas use-club <slug>'").orDie.as(ExitCode(2))
    }

  // Clearing an already-absent value is a success, not an error — `--clear` states a desired end state.
  private def clearCurrent(currentClub: Option[String]): UIO[ExitCode] =
    currentClub match {
      case None => Console.printLine("no current club set").orDie.as(ExitCode.success)
      case Some(s) =>
        ConfigWriter
          .clearCurrentClub(XdgPaths.configFile)
          .foldZIO(saveFailed("clear current club"), _ => cleared(CurrentClubRef.parse(s).slug))
    }

  private def setCurrent(slug: String, server: String): UIO[ExitCode] = {
    // Trim before storing: a slug never has surrounding whitespace, and `ClubSlug.normalize` only lowercases (no trim),
    // so a padded value would otherwise persist and later reach the API path verbatim.
    val s = slug.trim
    if (s.isEmpty) { usageError("club slug must not be blank") }
    else {
      // Write first, unconditionally: the local pointer is the guaranteed effect and must land even offline. It goes in
      // slug-only (no id yet); the verification that follows refreshes the cache, refines the advisory note, and — when
      // the club turns out to be managed — upgrades the pointer to the rename-proof `<id>:<slug>` form.
      ConfigWriter
        .setCurrentClub(XdgPaths.configFile, None, s)
        .foldZIO(saveFailed("save current club"), _ => verifyThenConfirm(s, server))
    }
  }

  private def verifyThenConfirm(slug: String, server: String): UIO[ExitCode] =
    for {
      managed <- fetchManaged(server)
      slugs = managed.map(_.map(_.slug))
      // If the club is managed, upgrade the pointer to the rename-proof `<id>:<slug>` form. Best-effort: the slug-only
      // pointer already landed, so a write failure here doesn't fail the command. An unmanaged / unverified club stays
      // slug-only and gets its id backfilled on the next successful job submit.
      _ <- ZIO.foreachDiscard(matchedId(slug, managed))(id => upgradePointer(id, slug))
      // A live list means the cache is now authoritative for this club too — write it so the false-alarm case (a
      // managed club the cache hadn't yet learned) can't recur, and so completion picks the club up immediately.
      // Only worth caching when the managed set has something in it. Writing an empty list would truncate the cache to
      // zero bytes AND stamp a fresh mtime, which reads as "fresh" to `clubsStale` — suppressing `Dispatcher`'s refresh
      // and its `/api/clubs` fallback for the whole TTL, while `seedClubs` no-ops because the file now exists. That
      // fallback policy is `Dispatcher`'s to apply; this opportunistic write deliberately owns none of it and just
      // steps aside.
      written <- ZIO.foreach(slugs.filter(_.nonEmpty))(CompletionCache.writeClubs)
      _       <- ZIO.whenDiscard(written.contains(false))(cacheWriteFailed)
      _       <- advise(slug, classify(slug, slugs))
      _       <- Console.printLine(s"current club set to $slug").orDie
    } yield ExitCode.success

  // The managed club matching the typed slug (case-insensitive; `ClubSlug.normalize` only lowercases), and its stable
  // id — the value that upgrades the pointer to rename-proof form.
  private def matchedId(slug: String, managed: Option[List[ManagedClubResponse]]): Option[ClubId] =
    managed.flatMap(_.find(_.slug.equalsIgnoreCase(slug))).map(c => ClubId.wrap(c.clubId))

  private def upgradePointer(id: ClubId, slug: String): UIO[Unit] =
    ConfigWriter.setCurrentClub(XdgPaths.configFile, Some(id), slug).ignore

  // Best-effort: build the client, fetch the managed set, bound it, and collapse every failure to None so the caller
  // falls back to the offline path.
  //
  // Operator order is load-bearing. `timeout` must sit OUTSIDE `provide`, or the layer's acquire/release runs
  // unbounded; `disconnect` is required because plain `timeout` waits for the interrupted fiber to unwind, and
  // `NettyConnectionPool.createChannel` is uninterruptible — against a black-holed host the "2s" bound measured 31s
  // without it. Same hazard as BodyStore's deadlines, docs/adr/0009-bound-every-body-store-operation.md.
  // `HttpClientLayer` caps connect at 10s (#182), but this probe wants a far tighter interactive cutoff.
  private def fetchManaged(server: String): UIO[Option[List[ManagedClubResponse]]] =
    CcasApiClient
      .live(server)
      .flatMap(_.getJson[List[ManagedClubResponse]]("/api/managed-clubs"))
      .provide(HttpClientLayer.live)
      .disconnect
      .timeout(VerifyTimeout)
      .catchAllCause(_ => ZIO.none)

  private[cli] def classify(slug: String, managed: Option[List[String]]): Verify =
    managed match {
      case Some(list) if list.exists(_.equalsIgnoreCase(slug)) => Verify.Managed
      case Some(_)                                             => Verify.Unmanaged
      case None                                               => Verify.Unverified
    }

  private[cli] def advise(slug: String, verify: Verify): UIO[Unit] =
    verify match {
      case Verify.Managed => ZIO.unit
      case Verify.Unmanaged =>
        note(
          s"warning: '$slug' is not one of your managed clubs — bare commands still target it; " +
            s"manage it with 'ccas club add $slug'"
        )
      // Only this branch has no live answer, so only it pays for a cache read.
      case Verify.Unverified => CompletionCache.readClubs.flatMap(offlineHint(slug, _))
    }

  // With no live answer the cache is all we have, so keep its two failure modes apart: a cache we read and that simply
  // doesn't list the slug is a real (if weak) typo hint, while a cache we couldn't read at all is a broken cache — say
  // so rather than staying silent, which would read as "looks fine" in the state we know least about.
  //
  // Both messages say "did not answer" rather than "was unreachable": `fetchManaged` collapses a refused connection, a
  // timeout, a non-2xx status and a decode failure into the same `None`, so asserting unreachability would point an
  // operator away from a server that is up but erroring.
  private[cli] def offlineHint(slug: String, cached: Option[List[String]]): UIO[Unit] =
    cached match {
      case None =>
        note(s"warning: could not verify '$slug': the server did not answer and the club cache could not be read")
      case Some(known) =>
        ZIO.whenDiscard(isUnknown(slug, known))(
          note(s"warning: '$slug' not in the cached club list and the server did not answer to verify (may be stale)")
        )
    }

  // Naming the path is the actionable part: an unwritable cache directory (a root-owned one left by a `sudo ccas` run
  // is the usual cause) otherwise leaves tab-completion silently dead with nothing to pull on. Advisory only — the
  // pointer is already written and the command still succeeds.
  private def cacheWriteFailed: UIO[Unit] =
    note(
      s"warning: could not update the club cache at ${XdgPaths.clubsFile} — " +
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
