package ccas.cli

import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.io.StdIn

import zio.*

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Username}
import ccas.api.player.ApiPlayer
import ccas.cli.config.{ConfigWriter, CurrentClubRef}
import ccas.cli.config.CurrentClubRef.sameSlug
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.BlacklistRoutes.{BlacklistEntryResponse, CreateBlacklistRequest}
import ccas.server.routes.JobRoutes.{
  CancelResult,
  ClubJobResult,
  ConfirmResult,
  HistoryRequest,
  InvitedUsernames,
  JobStatusResponse,
  MembershipRequest,
  RecruitmentRequest,
  StatsRequest
}
import ccas.server.routes.ManagedClubRoutes.{ManagedClubResponse, MarkManagedRequest}
import ccas.server.routes.ScheduleRoutes.{CreateScheduleRequest, CreateScheduleResponse, ScheduleResponse}
import ccas.server.scheduler.{MisfirePolicy, TriggerType}
import ccas.utils.client.HttpClientLayer

/** Maps a parsed [[CliCommand]] to HTTP calls against the local server and renders the result, returning the process
  * exit code. `Serve` is handled in [[Main]] (it boots the server rather than calling it), never here.
  */
object Dispatcher {

  private val MaxJobWait: Duration = 60.minutes
  // Auto-reconnect tuning for a dropped log follow (#161): the server reaps the connection at its 60s read-idle
  // timeout during a silent job phase, so we re-follow. `MaxJobWait` is the real wall-clock bound; `MaxReconnects` is
  // only a busy-loop backstop for a stream that redrops instantly, sized well above any real job's silent-phase count.
  private val ReconnectBackoff: Duration = 2.seconds
  private val MaxReconnects: Int         = 1000

  def dispatch(cmd: CliCommand.ServerCommand, currentClubOption: Option[String]): UIO[ExitCode] =
    CcasApiClient
      .live(cmd.server)
      .flatMap { api =>
        // Bars render only on an interactive terminal (hasTty) and unless `--no-progress` was passed; piped/redirected
        // output stays plain lines regardless.
        val jobFollower = JobFollower(
          api,
          MaxJobWait,
          ReconnectBackoff,
          MaxReconnects,
          showProgress = hasTty && !noProgressFor(cmd)
        )
        runCommand(api, jobFollower, cmd, currentClubOption) <* refreshClubsCache(api)
      }
      // `HttpClientLayer.live` (not `Client.default`) so every Dispatcher call inherits the shared transport's
      // `connectionTimeout(10s)` — a black-holed server now fails connect natively at 10s instead of parking a fiber on
      // zio-http's unbounded 30s default (#182). Bounding connect at the Netty layer, below ZIO interruption, avoids the
      // uninterruptible-unwind problem a ZIO-level `.timeout` hits, and — unlike a blanket request timeout — leaves
      // legitimately-slow synchronous calls (e.g. a large `blacklist` add) and the streaming follows untouched.
      .provide(HttpClientLayer.live)
      .catchAll {
        case e: CliError  => Console.printLineError(s"error: ${e.message}").orDie.as(e.exitCode)
        case e: Throwable => Console.printLineError(s"error: ${rootMessage(e)}").orDie.as(1)
      }
      // Last-resort net for unexpected defects (e.g. an invalid opaque-type construction) so the binary prints a
      // clean message instead of dumping a fiber stack trace.
      .catchAllDefect(d => Console.printLineError(s"error: ${rootMessage(d)}").orDie.as(1))
      .map(ExitCode(_))

  private def rootMessage(e: Throwable): String =
    Option(e.getMessage).getOrElse(e.getClass.getSimpleName)

  // Whether `--no-progress` was set — only the job-following commands carry the flag; the rest never draw bars.
  private def noProgressFor(cmd: CliCommand.ServerCommand): Boolean = cmd match {
    case c: CliCommand.Membership => c.noProgress
    case c: CliCommand.History    => c.noProgress
    case c: CliCommand.Recruit    => c.noProgress
    case c: CliCommand.Stats      => c.noProgress
    case c: CliCommand.Logs       => c.noProgress
    case _                        => false
  }

  // Best-effort, staleness-gated refresh of the completion club-slug cache after a successful command. Fully ignored:
  // it never blocks the result or alters the exit code (the `.tap` runs only on the success channel).
  private def refreshClubsCache(api: CcasApiClient): UIO[Unit] =
    CompletionCache.clubsStale.flatMap(stale => ZIO.whenDiscard(stale)(writeClubsCache(api)))

  // Prefer the managed set: those are the only clubs `--all` expands to and the only ones that keep scheduled jobs, so
  // they're what `--club` completion should offer. `/api/clubs` is every club ever ingested — a few history crawls put
  // it in the thousands, almost all opponents and scouted clubs — so it serves only as a fallback for a fresh install
  // with nothing managed yet, leaving completion useful before the first `club add`.
  private def writeClubsCache(api: CcasApiClient): UIO[Unit] =
    writeClubsCacheResult(api).ignore

  // Reporting variant for the `club add`/`remove` handlers: a *write* failure (cache dir unwritable) is named on stderr,
  // while a *fetch* failure stays silent. The distinction matters — the server call that preceded this just succeeded,
  // so a subsequent managed-set fetch failing is a transient blip, not a cache-dir problem, and blaming the cache would
  // misdirect. Never fails and never changes the exit code: a broken completion cache is not a failed command (#181).
  private def writeClubsCacheReporting(api: CcasApiClient): UIO[Unit] =
    writeClubsCacheResult(api).foldZIO(_ => ZIO.unit, wrote => ZIO.unlessDiscard(wrote)(cacheWriteFailed))

  // Shared core: pick the managed set (falling back to all clubs only for a fresh install with nothing managed), write
  // it, and return whether the write landed. Fails only if the *fetch* fails; `writeClubs` itself never fails.
  private def writeClubsCacheResult(api: CcasApiClient): Task[Boolean] =
    managedSlugs(api)
      .flatMap(slugs =>
        if (slugs.nonEmpty) { ZIO.succeed(slugs) }
        else { allClubSlugs(api) }
      )
      .flatMap(CompletionCache.writeClubs)

  private def cacheWriteFailed: UIO[Unit] =
    Console
      .printLineError(
        s"warning: could not update the club cache at ${XdgPaths.clubsFile} — " +
          "shell completion won't reflect your managed clubs until that is writable"
      )
      .orDie

  private def managedSlugs(api: CcasApiClient): Task[List[String]] =
    api.getJson[List[ManagedClubResponse]]("/api/managed-clubs").map(_.map(_.slug))

  private def allClubSlugs(api: CcasApiClient): Task[List[String]] =
    api.getJson[CompletionCache.ClubsDto]("/api/clubs").map(_.clubs.map(_.slug))

  private def runCommand(
    api: CcasApiClient,
    follower: JobFollower,
    cmd: CliCommand.ServerCommand,
    currentClubOption: Option[String]
  ): Task[Int] = cmd match {
    case CliCommand.Membership(_, clubs, clubIdOption, all, trust, _, detach) =>
      resolveClubs(api, clubs, clubIdOption, all, currentClubOption).flatMap(targets =>
        followEachClub(follower, targets, detach, currentClubOption)(target =>
          api.postJson[MembershipRequest, List[ClubJobResult]](
            "/api/jobs/membership",
            MembershipRequest(NonEmptyChunk.single(target.query), trust)
          )
        )
      )

    case CliCommand.History(_, clubs, clubIdOption, all, full, includeFinished, refresh, refreshMinHours, _, detach) =>
      resolveClubs(api, clubs, clubIdOption, all, currentClubOption).flatMap(targets =>
        followEachClub(follower, targets, detach, currentClubOption)(target =>
          api.postJson[HistoryRequest, List[ClubJobResult]](
            "/api/jobs/history",
            HistoryRequest(
              NonEmptyChunk.single(target.query),
              flag(full),
              flag(includeFinished),
              flag(refresh),
              refreshMinHours            )
          )
        )
      )

    case CliCommand.Recruit(
          _,
          club,
          clubIdOption,
          alias,
          target,
          cumulative,
          sourceClubs,
          timeLimitMinutes,
          explore,
          stdout,
          report,
          runId,
          _
        ) =>
      recruitUsageError(report, runId, club, clubIdOption) match {
        case Some(message)  => ZIO.fail(CliError(message, 2))
        case None if report => reportInvited(api, club, clubIdOption, currentClubOption, runId, stdout)
        case None =>
          // Marking Invited is destructive (a forgotten invite burns the candidate for the cooldown), so it needs
          // positive intent: only `--stdout` (programmatic consumption) auto-confirms from the CLI. An interactive
          // run prompts; any other non-interactive run (e.g. stdout redirected to a file) defers rather than silently
          // inviting: `autoConfirm = Some(false)` for all but `--stdout` has the server leave them Deferred.
          val interactiveConfirm = !stdout && hasTty
          for {
            clubTarget <- resolveClub(club, clubIdOption, currentClubOption)
            result <- sendSettlingAmbiguity(clubTarget, (r: ClubJobResult) => List(r.resolution)) { chosen =>
              api.postJson[RecruitmentRequest, ClubJobResult](
                "/api/jobs/recruitment",
                RecruitmentRequest(
                  club = chosen.query,
                  alias = alias,
                  target = target,
                  cumulative = flag(cumulative),
                  sourceClubs = Option.when(sourceClubs.nonEmpty)(sourceClubs.map(ClubSlug(_))),
                  timeLimitMinutes = timeLimitMinutes,
                  explore = explore,
                  autoConfirm = Option.unless(stdout)(false)
                )
              )
            }
            _    <- noteResolutions(currentClubOption, clubTarget, List(jobOutcome(result)))
            code <- handleRecruitResult(follower, api, result, stdout, interactiveConfirm)
          } yield code
      }

    case CliCommand.Stats(_, club, clubIdOption, since, until, _, detach) =>
      for {
        target <- resolveClub(club, clubIdOption, currentClubOption)
        result <- sendSettlingAmbiguity(target, (r: ClubJobResult) => List(r.resolution)) { chosen =>
          api.postJson[StatsRequest, ClubJobResult](
            "/api/jobs/stats",
            StatsRequest(chosen.query, since, until)
          )
        }
        _    <- noteResolutions(currentClubOption, target, List(jobOutcome(result)))
        code <-
          if (detach) { reportDetachedClub(result) }
          else { follower.handleClub(result, logsToStderr = false, _ => ZIO.unit) }
      } yield code

    case CliCommand.Jobs(_, limit) =>
      api.getJson[List[JobStatusResponse]]("/api/jobs").flatMap(all => printJobs(limit.fold(all)(all.take)).as(0))

    case CliCommand.Logs(_, jobId, _) =>
      follower.followJob(jobId)

    // A 404 (no running job to cancel) surfaces via the client's decode path as a CliError, rendered by `dispatch`'s
    // catchAll as "error: …" with exit 1 — so success here means the interrupt was dispatched.
    case CliCommand.Cancel(_, jobId) =>
      api.postEmpty[CancelResult](s"/api/jobs/$jobId/cancel")
        *> Console.printLine(s"cancellation requested for job $jobId").orDie.as(0)

    case CliCommand.BlacklistAdd(_, club, clubIdOption, usernames, reason, months) =>
      for {
        target <- resolveClub(club, clubIdOption, currentClubOption)
        (resolved, blacklisted) <- actOnClub(currentClubOption, target) { chosen =>
          api.postJson[CreateBlacklistRequest, ClubResult[List[String]]](
            "/api/blacklist",
            CreateBlacklistRequest(chosen.query, usernames.map(Username(_)), reason, months)
          )
        }
        _ <- Console.printLine(s"blacklisted ${blacklisted.mkString(", ")} for ${ClubSlug.unwrap(resolved.slug)}").orDie
      } yield 0

    case CliCommand.BlacklistList(_, club, clubIdOption) =>
      for {
        target <- resolveClub(club, clubIdOption, currentClubOption)
        (_, entries) <- actOnClub(currentClubOption, target) { chosen =>
          api.getJson[ClubResult[List[BlacklistEntryResponse]]](
            s"/api/blacklist?${ClubRequest.queryString(chosen.query)}"
          )
        }
        _ <- printBlacklist(entries)
      } yield 0

    case CliCommand.BlacklistRemove(_, club, clubIdOption, username) =>
      for {
        target <- resolveClub(club, clubIdOption, currentClubOption)
        (resolved, removed) <- actOnClub(currentClubOption, target) { chosen =>
          api.deleteJson[ClubResult[Boolean]](s"/api/blacklist/$username?${ClubRequest.queryString(chosen.query)}")
        }
        name = ClubSlug.unwrap(resolved.slug)
        _ <- Console.printLine(
          if (removed) { s"removed $username from $name blacklist" } else { s"$username was not blacklisted for $name" }
        ).orDie
      } yield 0

    case CliCommand.ScheduleList(_) =>
      api.getJson[List[ScheduleResponse]]("/api/schedules").flatMap(schedules => printSchedules(schedules).as(0))

    case CliCommand.ScheduleAdd(_, kind, intervalHours, cron, tz, misfire, club, clubIdOption, params) =>
      // --interval-hours and --cron are mutually exclusive (exactly one required); the server enforces the XOR
      // and returns a clean 400 if both/neither are given. triggerType follows whether --cron is present.
      for {
        mp <- ZIO.foreach(misfire)(parseMisfire)
        triggerType = if (cron.isDefined) TriggerType.Cron else TriggerType.Interval
        targetOption <- ClubResolver.optional(club, clubIdOption)
        s <- createSchedule(currentClubOption, targetOption) { clubOption =>
          api.postJson[CreateScheduleRequest, CreateScheduleResponse](
            "/api/schedules",
            CreateScheduleRequest(
              kind = kind,
              clubOption = clubOption,
              params = params,
              triggerType = Some(triggerType),
              intervalHours = intervalHours,
              cron = cron,
              timezone = tz,
              misfire = mp
            )
          )
        }
        _ <- Console.printLine(s"created schedule ${s.id} (${s.kind}, ${triggerSummary(s)})").orDie
      } yield 0

    case CliCommand.ScheduleRemove(_, id) =>
      api.delete(s"/api/schedules/$id") *> Console.printLine(s"deleted schedule $id").orDie.as(0)

    // Both mutate the managed set, which IS the completion cache's source, so they repopulate it right here rather than
    // leaning on the post-command `.tap` refresh. The explicit write is REPORTED: a failure to write is surfaced on
    // stderr (#181), because setting up club context is exactly when a broken cache dir should be named instead of
    // leaving completion silently dead. The direct overwrite supersedes the old `invalidate`-then-tap-repopulate — a
    // failed *delete* is a poor signal for an unwritable dir (deleting an absent file succeeds), whereas the write is
    // the operation that actually fails. The trailing `.tap` then no-ops on the now-fresh file.
    case CliCommand.ClubsAdd(_, slugs, clubIdOption) =>
      for {
        target <- ClubResolver.operand(slugs, clubIdOption)
        (club, added) <- actOnClub(currentClubOption, target) { chosen =>
          api.postJson[MarkManagedRequest, ClubResult[Boolean]]("/api/managed-clubs", MarkManagedRequest(chosen.query))
        }
        name = ClubSlug.unwrap(club.slug)
        _ <- Console.printLine(if (added) { s"now managing $name" } else { s"already managing $name" }).orDie
        _ <- writeClubsCacheReporting(api)
      } yield 0

    case CliCommand.ClubsRemove(_, slugs, clubIdOption) =>
      for {
        target <- ClubResolver.operand(slugs, clubIdOption)
        // No pointer refresh: it only ever rewrites a `current_club` naming this club, which the removal then clears.
        (club, removed) <- settleAndNote(target, noteOutcomes(currentClubOption, _)) { chosen =>
          api.deleteJson[ClubResult[Boolean]](s"/api/managed-clubs?${ClubRequest.queryString(chosen.query)}")
        }
        name = ClubSlug.unwrap(club.slug)
        _ <- Console.printLine(if (removed) { s"stopped managing $name" } else { s"$name was not managed" }).orDie
        _ <- clearCurrentIfRemoved(club, target.query, currentClubOption)
        _ <- writeClubsCacheReporting(api)
      } yield 0

    case CliCommand.ClubsList(_) =>
      api
        .getJson[List[ManagedClubResponse]]("/api/managed-clubs")
        .flatMap(clubs => printManagedClubs(clubs, currentClubOption).as(0))
  }

  // Unmanaging leaves the `club` row intact and submission gates on that row, not on managed status, so a
  // `current_club` still pointing at the removed club would keep running real jobs against it. Clear it so the next
  // bare command fails loudly with `ClubResolver.NoClubError`.
  private def clearCurrentIfRemoved(removed: ClubRef, query: ClubQuery, currentClubOption: Option[String]): UIO[Unit] =
    ZIO.whenDiscard(currentClubOption.map(CurrentClubRef.parse).exists(_.means(removed, query))) {
      ConfigWriter
        .clearCurrentClub(XdgPaths.configFile)
        .foldZIO(clearFailed, _ => currentClubCleared)
    }

  private def currentClubCleared: UIO[Unit] =
    Console.printLine("that was your current club; cleared it — set a new one with 'ccas use-club <slug>'").orDie

  // Don't fail the command: the removal itself succeeded server-side. Say what's left dangling and how to fix it.
  private def clearFailed(e: Throwable): UIO[Unit] =
    Console
      .printLineError(
        s"warning: that was your current club, but clearing it failed (${rootMessage(e)}); " +
          "clear it with 'ccas use-club --clear'"
      )
      .orDie

  // `current_club` is stored as `<id>:<slug>` (or a bare slug), so anything comparing or displaying it must go through
  // the ref rather than treating the raw value as a slug.
  private def currentSlug(currentClubOption: Option[String]): Option[String] =
    currentClubOption.map(raw => CurrentClubRef.parse(raw).slug)

  // Does `current_club` name this club? By id when the pointer carries one (rename-proof), else by slug.
  private def currentMatches(currentClubOption: Option[String], clubId: Long, slug: String): Boolean =
    currentClubOption.exists { raw =>
      val ref = CurrentClubRef.parse(raw)
      ref.clubIdOption.exists(id => ClubId.unwrap(id) == clubId) || sameSlug(ref.slug, slug)
    }

  // The warning is deliberately outside the branch: an empty managed set is precisely when "your current club isn't
  // managed" is *certainly* true, so suppressing it there hid the clearest case (and disagreed with `use-club`, which
  // warns in that same server state).
  private def printManagedClubs(clubs: List[ManagedClubResponse], currentClubOption: Option[String]): UIO[Unit] = {
    val listing =
      if (clubs.isEmpty) { Console.printLine("no managed clubs").orDie }
      else { ZIO.foreachDiscard(clubs)(c => Console.printLine(clubLine(c, currentClubOption)).orDie) }
    listing *> warnUnmanagedCurrent(clubs, currentClubOption)
  }

  // `*` marks the club bare commands target, so `club list` answers "which one am I on?" as well as "which do I have?".
  // Matches by id when `current_club` carries one, so a renamed current club is still marked against its new slug.
  private def clubLine(c: ManagedClubResponse, currentClubOption: Option[String]): String = {
    val marker = if (currentMatches(currentClubOption, c.clubId, c.slug)) { "*" }
    else { " " }
    s"$marker ${c.slug}  ${c.name}  marked=${c.markedAt}"
  }

  // A current club outside the managed set still resolves today, so it can't be silently omitted from the listing
  // without leaving the user wondering which club their bare commands actually hit.
  private def warnUnmanagedCurrent(clubs: List[ManagedClubResponse], currentClubOption: Option[String]): UIO[Unit] =
    ZIO.foreachDiscard(
      currentSlug(currentClubOption)
        .filterNot(_ => clubs.exists(c => currentMatches(currentClubOption, c.clubId, c.slug)))
    )(cur =>
      Console
        .printLineError(
          s"warning: current club '$cur' is not in this list; add it with 'ccas club add $cur' " +
            "or switch with 'ccas use-club <slug>'"
        )
        .orDie
    )

  // Run each club through submit-then-follow ONE at a time: the next club isn't submitted until the current one's
  // follow completes, and overall exit is 0 only if every club succeeded. Each `submitOne` posts a single-club job
  // (the batch routes accept a one-element list) whose result `handleBatch` follows. With `--detach` (#170) the
  // follow is skipped but the same loop is reused, so error handling and scoring stay identical.
  //
  // Why serialized rather than concurrent, and why each club is best-effort:
  // docs/adr/0011-cli-locality-and-the-current-club-pointer.md.
  private def followEachClub(
    follower: JobFollower,
    targets: NonEmptyChunk[ClubTarget],
    detach: Boolean,
    currentClubOption: Option[String]
  )(
    submitOne: ClubTarget => Task[List[ClubJobResult]]
  ): Task[Int] = {
    val handle: List[ClubJobResult] => Task[Int] =
      if (detach) { results => ZIO.foreach(results)(reportDetachedClub).map(JobFollower.overallExitCode) }
      else { follower.handleBatch }
    ZIO
      .foreach(targets.toChunk.toList)(target =>
        (for {
          results <- sendSettlingAmbiguity(target, (rs: List[ClubJobResult]) => rs.map(_.resolution))(submitOne)
          _       <- noteResolutions(currentClubOption, target, results.map(jobOutcome))
          code    <- handle(results)
        } yield code)
          .catchAll(e => Console.printLineError(s"${target.label}: ${rootMessage(e)}").orDie.as(1))
      )
      .map(JobFollower.overallExitCode)
  }

  // Detached submit of a single club-scoped job (#170): print its id (or its submit error) and DON'T follow. Cache the
  // id for `ccas logs`/`ccas cancel` completion, and note the reattach command. Exit 1 for a per-club submit failure so
  // a partial `--all --detach` batch still scores as failed overall (mirrors `handleClub`'s error scoring).
  private def reportDetachedClub(result: ClubJobResult): UIO[Int] =
    JobFollower.whenSubmitted(result.club, result.failure, result.jobIdOption) { id =>
      CompletionCache.appendJob(id) *>
        Console.printLine(s"${result.club} submitted (detached): $id — follow with 'ccas logs $id'").orDie.as(0)
    }

  /** How a request that named a club was answered: the resolution, and whether the server then acted on the club —
    * started a job, or did what a synchronous command asked.
    */
  private[cli] final case class ClubOutcome(resolution: ClubResolution, acted: Boolean)

  private def jobOutcome(result: ClubJobResult): ClubOutcome =
    ClubOutcome(resolution = result.resolution, acted = result.jobIdOption.isDefined)

  // Every request that names a club acts on its resolutions the same way: a missing club busts the cache and may hint,
  // a former name gets a note, and a resolved current club refreshes `current_club`.
  private def noteResolutions(
    currentClubOption: Option[String],
    target: ClubTarget,
    outcomes: List[ClubOutcome]
  ): UIO[Unit] =
    noteOutcomes(currentClubOption, outcomes) *>
      ZIO.foreachDiscard(outcomes.headOption)(o => maybeRefreshCurrentClub(currentClubOption, target, o.resolution))

  private def noteOutcomes(currentClubOption: Option[String], outcomes: List[ClubOutcome]): UIO[Unit] =
    noteMissingClubs(missingFrom(outcomes), currentClubOption) *>
      noteNameChanges(renamedFrom(outcomes).map(renamedNote) ++ movedFrom(outcomes).map(movedNote))

  // The synchronous club commands resolve on the server just as a submit does, so they settle an ambiguous name, note
  // a changed one and refresh the pointer the same way; a club the server could not act on fails the command.
  private def actOnClub[A](currentClubOption: Option[String], target: ClubTarget)(
    send: ClubTarget => Task[ClubResult[A]]
  ): Task[(ClubRef, A)] =
    settleAndNote(target, noteResolutions(currentClubOption, target, _))(send)

  private def settleAndNote[A](target: ClubTarget, note: List[ClubOutcome] => UIO[Unit])(
    send: ClubTarget => Task[ClubResult[A]]
  ): Task[(ClubRef, A)] =
    for {
      result <- sendSettlingAmbiguity(target, (r: ClubResult[A]) => List(r.resolution))(send)
      _      <- note(List(ClubOutcome(resolution = result.resolution, acted = result.resultOption.isDefined)))
      acted  <- ZIO.fromEither(result.toEither).mapError(CliError(_, 1))
    } yield acted

  // A schedule names a club only for the kinds that take one, and without one there is nothing to settle or note.
  private def createSchedule(currentClubOption: Option[String], targetOption: Option[ClubTarget])(
    send: Option[ClubQuery] => Task[CreateScheduleResponse]
  ): Task[ScheduleResponse] =
    targetOption match {
      case None => send(None).flatMap(scheduleOf)
      case Some(target) =>
        for {
          created <- sendSettlingAmbiguity(target, (r: CreateScheduleResponse) => r.resolutionOption.toList)(chosen =>
            send(Some(chosen.query))
          )
          outcomeOption = created.resolutionOption.map(ClubOutcome(_, created.scheduleOption.isDefined))
          _        <- noteResolutions(currentClubOption, target, outcomeOption.toList)
          schedule <- scheduleOf(created)
        } yield schedule
    }

  private def scheduleOf(created: CreateScheduleResponse): IO[CliError, ScheduleResponse] =
    ZIO
      .fromOption(created.scheduleOption)
      .orElseFail(
        CliError(created.resolutionOption.flatMap(_.runnable.left.toOption).getOrElse("no schedule was created"), 1)
      )

  // Ambiguity is the one resolution the user can settle on the spot, so an interactive run lists the candidates and
  // sends again for the one picked — the same answer `--club-id` gives a headless run (#254). Every other
  // resolution, and an abort, is returned exactly as it came back.
  private def sendSettlingAmbiguity[A](
    target: ClubTarget,
    resolutionsOf: A => List[ClubResolution]
  )(submit: ClubTarget => Task[A]): Task[A] =
    submit(target).flatMap { result =>
      resolutionsOf(result).collectFirst { case ambiguous: ClubResolution.Ambiguous => ambiguous } match {
        case None => ZIO.succeed(result)
        case Some(ambiguous) =>
          promptForClub(ambiguous, interactive = hasTty).flatMap {
            case None         => ZIO.succeed(result)
            case Some(clubId) => submit(ClubTarget.byId(clubId))
          }
      }
    }

  // Nothing is prompted without a terminal: the error already names `--club-id`, which is this same choice made
  // up front. The prompt goes to stderr so a piped stdout still carries only the command's own output, and an answer
  // that isn't one of the offered numbers aborts rather than guessing at a club.
  private[cli] def promptForClub(ambiguous: ClubResolution.Ambiguous, interactive: Boolean): UIO[Option[ClubId]] =
    if (!interactive) { ZIO.none }
    else {
      val holders = ambiguous.holders
      for {
        _ <- Console.printLineError(
          s"'${ClubSlug.unwrap(ambiguous.requested)}' was held by ${holders.size} clubs, and nobody holds it now:"
        ).orDie
        _ <- ZIO.foreachDiscard(holders.zipWithIndex) { case (club, index) =>
          Console.printLineError(s"  ${index + 1}) ${club.display}").orDie
        }
        _      <- Console.printError(s"which club did you mean? [1-${holders.size}, blank to abort] ").orDie
        answer <- Console.readLine.orElseSucceed("")
        pickedOption = answer.trim.toIntOption.filter(n => n >= 1 && n <= holders.size)
        _ <- ZIO.whenDiscard(pickedOption.isEmpty)(Console.printLineError("aborted").orDie)
      } yield pickedOption.map(n => holders(n - 1).clubId)
    }

  // A club-scoped result is "missing" — worth busting the completion cache and hinting a stranded `current_club` — when
  // the server has no usable club for what was asked. Answers with what was asked, so the caller can tell whether the
  // club that went missing was the current one. `Renamed` ran its job and `Ambiguous` needs no hint.
  private[cli] def missingQuery(resolution: ClubResolution): Option[ClubQuery] =
    resolution match {
      case ClubResolution.NotLocal(requested)    => Some(requested)
      case ClubResolution.Problematic(requested) => Some(requested)
      case _                                     => None
    }

  private def missingFrom(outcomes: List[ClubOutcome]): List[ClubQuery] =
    outcomes.flatMap(o => missingQuery(o.resolution))

  // Drop the cache so the post-command refresh repopulates it — the 6h TTL would otherwise re-suggest the same dead
  // slug on an immediate retry — and call out a stranded `current_club`, which nothing else ever repoints.
  private def noteMissingClubs(missing: List[ClubQuery], currentClubOption: Option[String]): UIO[Unit] =
    ZIO.whenDiscard(missing.nonEmpty) {
      val pointerOption = currentClubOption
        .map(CurrentClubRef.parse)
        .filter(ref => missing.exists(ref.names))
      CompletionCache.invalidate *> ZIO.foreachDiscard(pointerOption)(ref => staleCurrentClubHint(ref.slug))
    }

  // (requested, current) for each request that acted on a club it reached by a former name. One that did nothing (a
  // job already running) gets no note, since the note says which club was acted on.
  private[cli] def renamedFrom(outcomes: List[ClubOutcome]): List[(String, String)] =
    outcomes.collect { case ClubOutcome(ClubResolution.Renamed(club, requested), true) =>
      ClubSlug.unwrap(requested) -> ClubSlug.unwrap(club.slug)
    }

  // (requested, holder, previous holders) for each request that acted on a club whose name was taken from another.
  private[cli] def movedFrom(outcomes: List[ClubOutcome]): List[(String, ClubRef, List[ClubRef])] =
    outcomes.collect { case ClubOutcome(ClubResolution.Moved(club, requested, previous), true) =>
      (ClubSlug.unwrap(requested), club, previous)
    }

  private def renamedNote(renamed: (String, String)): String = {
    val (requested, current) = renamed
    s"note: '$requested' is a former name of '$current'; using '$current'"
  }

  // Chess.com's answer wins, so say whose name this was here before acting on another club entirely (#254).
  private def movedNote(moved: (String, ClubRef, List[ClubRef])): String = {
    val (requested, holder, previous) = moved
    s"note: '$requested' now belongs to club ${holder.display} on Chess.com, not to " +
      s"${previous.map(_.display).mkString(", ")}; using the club that holds it"
  }

  // A name that reached the server attached to a different club than it names now means the completion cache may still
  // be offering the old one, so drop the cache alongside the note.
  private def noteNameChanges(notes: List[String]): UIO[Unit] =
    ZIO.whenDiscard(notes.nonEmpty) {
      CompletionCache.invalidate *> ZIO.foreachDiscard(notes)(note => Console.printLineError(note).orDie)
    }

  private def staleCurrentClubHint(slug: String): UIO[Unit] =
    Console
      .printLineError(
        s"note: '$slug' is your current club but the server doesn't know it — it may have been renamed on Chess.com. " +
          "Point at the new slug with 'ccas use-club <slug>', or drop it with 'ccas use-club --clear'."
      )
      .orDie

  // Resolution lives in the pure, testable `ClubResolver`; the `--all` expansion's network call is injected here. A
  // `ClubTarget` carries the slug plus, when sourced from `current_club`, the stable id to resolve by (rename-proof).
  private def resolveClub(
    explicit: Option[String],
    clubIdOption: Option[Long],
    currentClubOption: Option[String]
  ): IO[CliError, ClubTarget] =
    ClubResolver.single(explicit, clubIdOption, currentClubOption)

  private def resolveClubs(
    api: CcasApiClient,
    explicit: List[String],
    clubIdOption: Option[Long],
    all: Boolean,
    currentClubOption: Option[String]
  ): Task[NonEmptyChunk[ClubTarget]] =
    ClubResolver.multi(
      fetchManaged = api.getJson[List[ManagedClubResponse]]("/api/managed-clubs").map(_.map(_.slug)),
      explicit = explicit,
      clubIdOption = clubIdOption,
      all = all,
      currentClubOption = currentClubOption
    )

  // Freshen `current_club` after a submit whose resolved club is the current one — the decision (is-it-current + a real
  // change) lives in the pure, tested `CurrentClubRef.refreshedRef`; here we just persist its verdict. Best-effort
  // (`.ignore`): keeping names fresh must never fail a command.
  private def maybeRefreshCurrentClub(
    currentClubOption: Option[String],
    target: ClubTarget,
    resolution: ClubResolution
  ): UIO[Unit] = {
    val next = CurrentClubRef.refreshedRef(
      stored = currentClubOption,
      targetHasId = target.addressedById,
      targetSlug = target.label,
      resolvedOption = CurrentClubRef.refreshTarget(resolution)
    )
    ZIO.foreachDiscard(next)(ref => ConfigWriter.setCurrentClub(XdgPaths.configFile, ref.clubIdOption, ref.slug).ignore)
  }

  // Absent flag -> None (server defaults to false); present -> Some(true). Avoids sending a redundant `false`.
  private def flag(b: Boolean): Option[Boolean] = Option.when(b)(true)

  // Is this an interactive terminal? Fully qualified because `import zio.*` brings `zio.System` into scope. Null when
  // either stdin or stdout is redirected, which is exactly when we must not prompt or emit an OSC 52 clipboard escape.
  private def hasTty: Boolean = java.lang.System.console() != null

  // Deliver a completed scout's result per mode. All three route the job log to stderr so stdout carries only the
  // username payload (clean for a pipe or a redirect):
  //   --stdout        : bare invited usernames to stdout (auto-confirmed; for piping)
  //   interactive TTY : deferred-confirm — show the found candidates, prompt to mark them Invited and copy
  //   non-interactive : deferred, NOT invited — print the deferred list and note how to confirm (nothing burned)
  private def handleRecruitResult(
    follower: JobFollower,
    api: CcasApiClient,
    result: ClubJobResult,
    stdout: Boolean,
    interactiveConfirm: Boolean
  ): Task[Int] =
    if (stdout) { follower.handleClub(result, logsToStderr = true, deliverInvited(api, _)) }
    else if (interactiveConfirm) { follower.handleClub(result, logsToStderr = false, confirmFlow(api, _)) }
    else { follower.handleClub(result, logsToStderr = true, deferReport(api, _)) }

  // Fetch a completed auto-confirm scout's invited usernames and print them bare to stdout. A fetch/decode failure is
  // not propagated (the scout already succeeded and is persisted): `deliveryFailed` notes it and the command exits 0.
  private def deliverInvited(api: CcasApiClient, jobId: String): Task[Unit] =
    api.getJson[InvitedUsernames](s"/api/jobs/$jobId/recruitment/invited")
      .flatMap(r => printBare(r.usernames))
      .catchAll(deliveryFailed)

  // Bare newline list to stdout — the pipe payload for `--stdout` (logs already routed to stderr).
  private def printBare(names: List[String]): Task[Unit] =
    ZIO.foreachDiscard(names)(n => Console.printLine(n).orDie)

  // `<username> <profile-url>` per line — the human-review form. Delegates to the same `ApiPlayer.profileLine` the
  // recruitment out file uses for its detail lines, so the CLI and the out file render players identically.
  private def profileUrlLine(name: String): String = ApiPlayer.profileLine(Username.wrap(name))

  // The out-file review block for a list of raw usernames — a space-separated username line then one
  // `<username> <profile-url>` line per player. Reused for `--report` / confirm delivery so console, clipboard, and the
  // out files all render identically (via `ApiPlayer.profileReviewBlock`). `Username.wrap` canonicalizes to the DB /
  // out-file form (lowercase); server usernames already are, so this is a no-op that just satisfies the `Username` API.
  private def reviewBlock(names: List[String]): String = ApiPlayer.profileReviewBlock(names.map(Username.wrap))

  // Deferred-confirm flow: the scout left candidates Deferred. Show them, ask, and only then flip to Invited + copy.
  // Declining (or a read failure defaulting to "n") leaves them Deferred — nobody is burned, and they resurface.
  private def confirmFlow(api: CcasApiClient, jobId: String): Task[Unit] =
    api.getJson[InvitedUsernames](s"/api/jobs/$jobId/recruitment/found").flatMap { found =>
      if (found.usernames.isEmpty) { Console.printLine("No candidates found.").orDie }
      else { confirmPrompt(api, jobId, found.usernames) }
    }.catchAll(confirmFailed)

  private def confirmPrompt(api: CcasApiClient, jobId: String, names: List[String]): Task[Unit] =
    for {
      _ <- Console.printLine(s"\nFound ${names.size} candidates:").orDie
      _ <- ZIO.foreachDiscard(names)(n => Console.printLine(s"  ${profileUrlLine(n)}").orDie)
      answer <- ZIO.attemptBlocking(
        StdIn.readLine(s"\nMark all ${names.size} as Invited and copy to clipboard? [Y/n] ")
      )
        .orElseSucceed("n")
      _ <-
        if (isYes(answer)) { confirmAndCopy(api, jobId) }
        else { Console.printLine(s"Left ${names.size} as deferred; they'll resurface on the next run.").orDie }
    } yield ()

  private def confirmAndCopy(api: CcasApiClient, jobId: String): Task[Unit] =
    api.postEmpty[ConfirmResult](s"/api/jobs/$jobId/recruitment/confirm").flatMap(reportConfirmed)

  private def reportConfirmed(r: ConfirmResult): Task[Unit] =
    if (r.usernames.isEmpty) { Console.printLine(s"Marked ${r.marked} as invited.").orDie }
    else {
      // Canonicalize once and share it between the printed line and the copied block so they can't diverge. The prompt
      // already listed the players with URLs; here print just the paste-ready space-separated line and copy the full
      // review block (space list + profile URLs) so a paste carries both.
      val names = r.usernames.map(Username.wrap)
      for {
        _ <- Console.printLine(s"Marked ${r.marked} as invited.").orDie
        _ <- Console.printLine(names.mkString(" ")).orDie
        _ <- copyToClipboard(ApiPlayer.profileReviewBlock(names), s"${names.size} usernames + profile URLs")
      } yield ()
    }

  // Non-interactive run with no --stdout: the server deferred the candidates (nothing invited). We can't prompt, so
  // print the deferred list to stdout (useful when redirected to a file) and note on stderr how to confirm them.
  private def deferReport(api: CcasApiClient, jobId: String): Task[Unit] =
    api.getJson[InvitedUsernames](s"/api/jobs/$jobId/recruitment/found").flatMap { found =>
      val names = found.usernames
      if (names.isEmpty) { Console.printLineError("No candidates found.").orDie }
      else {
        printBare(names) *>
          Console.printLineError(
            s"note: ${names.size} candidates deferred, NOT invited (non-interactive run). Re-run in a terminal to " +
              "confirm, or with --stdout to auto-confirm; otherwise they resurface on the next scout."
          ).orDie
      }
    }.catchAll(deliveryFailed)

  // Confirmation didn't complete cleanly. It may have committed server-side before the response was lost, so don't
  // assert an outcome — point the operator at --report to check. Anything genuinely unconfirmed stays Deferred (safe).
  private def confirmFailed(e: Throwable): UIO[Unit] =
    Console.printLineError(
      s"note: confirmation may not have completed (${rootMessage(e)}); check with 'ccas recruit --report'. " +
        "Any unconfirmed candidates stay deferred and resurface on the next run."
    ).orDie

  // A run id is read-only, so without `--report` it would be silently dropped and launch a fresh scout — with
  // `--stdout`, auto-confirming that scout's invites. It also names its run outright, so a club beside it would be
  // silently ignored. Both are refused before anything is sent.
  private[cli] def recruitUsageError(
    report: Boolean,
    runId: Option[Int],
    club: Option[String],
    clubIdOption: Option[Long]
  ): Option[String] =
    if (runId.isDefined && !report) { Some("a run id can only be given with --report") }
    else if (runId.isDefined && (club.isDefined || clubIdOption.isDefined)) {
      Some("a run id already names the run; pass it without --club or --club-id")
    } else { None }

  // Report a past run's invited usernames (`ccas recruit --report`). --run picks a specific run; otherwise the club's
  // latest. `--stdout` prints a bare list for piping; otherwise it prints `<username> <profile-url>` per line (so the
  // operator can vet players) and, on an interactive terminal, copies the bare usernames (paste-ready for invites).
  private def reportInvited(
    api: CcasApiClient,
    club: Option[String],
    clubIdOption: Option[Long],
    currentClubOption: Option[String],
    runId: Option[Int],
    stdout: Boolean
  ): Task[Int] =
    for {
      invited <- runId match {
        case Some(id) => api.getJson[InvitedUsernames](s"/api/recruitment/runs/$id/invited")
        case None =>
          for {
            target <- resolveClub(club, clubIdOption, currentClubOption)
            (_, latest) <- actOnClub(currentClubOption, target) { chosen =>
              api.getJson[ClubResult[InvitedUsernames]](
                s"/api/recruitment/latest/invited?${ClubRequest.queryString(chosen.query)}"
              )
            }
          } yield latest
      }
      _ <- renderReport(invited.usernames, stdout)
    } yield 0

  private def renderReport(names: List[String], stdout: Boolean): Task[Unit] =
    if (names.isEmpty) { Console.printLineError("no invited usernames for that run").orDie }
    // --stdout: bare newline list, the pipe payload. Otherwise: the out-file review block (space-separated usernames
    // then `<username> <profile-url>` lines) to the console, and the same block to the clipboard on a TTY.
    else if (stdout) { printBare(names) }
    else {
      val block = reviewBlock(names)
      Console.printLine(block).orDie *>
        ZIO.whenDiscard(hasTty)(copyToClipboard(block, s"${names.size} usernames + profile URLs"))
    }

  // Default-yes: bare Enter or a y* answer confirms; a read failure defaulted to "n" above so this stays false there.
  private def isYes(answer: String): Boolean =
    answer == null || answer.trim.isEmpty || answer.trim.toLowerCase.startsWith("y")

  // The scout itself succeeded and is persisted; only the follow-up username fetch failed. Don't propagate (that
  // would render as "error: …" and invite a wasteful re-scout) — the invited usernames already streamed in the job
  // log above, so point the operator there and let the command exit 0.
  private def deliveryFailed(e: Throwable): UIO[Unit] =
    Console.printLineError(
      s"note: recruit completed but fetching the invited usernames failed (${rootMessage(e)}); " +
        "they are listed in the job log above."
    ).orDie

  // Copy `payload` to the system clipboard, printing "Copied <summary>." only on success. A native clipboard tool is
  // preferred: its forked daemon keeps owning the selection after this short-lived CLI exits, whereas an in-process JVM
  // clipboard (AWT) would lose the contents the moment we exit. The OSC 52 terminal escape is only a fallback for a
  // remote/SSH session whose *local* terminal implements it — many terminals (notably GNOME Terminal / VTE) silently
  // ignore OSC 52 clipboard writes, which is why the old escape-only path copied nothing.
  private def copyToClipboard(payload: String, summary: String): Task[Unit] =
    ZIO.whenDiscard(payload.nonEmpty) {
      clipboardCommand match {
        case None => copyFallback(payload)
        case Some(cmd) =>
          copyViaTool(cmd, payload).flatMap { copied =>
            if (copied) Console.printLine(s"Copied $summary.").orDie
            else copyFallback(payload)
          }
      }
    }

  // The native clipboard command for this environment, if any: macOS -> pbcopy; Wayland -> wl-copy; X11 -> xclip.
  // None on a headless / unknown session, which routes to the OSC 52 fallback.
  private def clipboardCommand: Option[List[String]] =
    if (isMac) { Some(List("pbcopy")) }
    else if (envSet("WAYLAND_DISPLAY")) { Some(List("wl-copy")) }
    else if (envSet("DISPLAY")) { Some(List("xclip", "-selection", "clipboard")) }
    else { None }

  private def isMac: Boolean                = sys.props.getOrElse("os.name", "").toLowerCase.contains("mac")
  private def envSet(name: String): Boolean = Option(java.lang.System.getenv(name)).exists(_.nonEmpty)

  // Feed `payload` to the clipboard tool's stdin, then wait. Returns true only if the process
  // launched and exited 0; a launch failure (tool not installed -> IOException) or non-zero exit yields false so the
  // caller falls back. Closing stdin sends EOF, which wl-copy / xclip / pbcopy wait for before taking ownership.
  private def copyViaTool(cmd: List[String], payload: String): UIO[Boolean] =
    ZIO.attemptBlocking {
      val pb = new ProcessBuilder(cmd*)
      // Discard the tool's stdout/stderr rather than leaving an un-drained pipe that could wedge waitFor if the tool
      // ever got chatty (wl-copy / xclip / pbcopy are silent on success, but this is robust regardless).
      pb.redirectOutput(ProcessBuilder.Redirect.DISCARD)
      pb.redirectError(ProcessBuilder.Redirect.DISCARD)
      val process = pb.start()
      val stdin   = process.getOutputStream
      try stdin.write(payload.getBytes(StandardCharsets.UTF_8))
      finally stdin.close()
      process.waitFor() == 0
    }.orElseSucceed(false)

  // No working native clipboard tool. The list was already printed by the caller (renderReport / reportConfirmed), so
  // don't reprint it — on a TTY emit the OSC 52 escape as a best-effort copy (works only if the terminal honours
  // it, e.g. an SSH session into kitty / tmux with `set -g set-clipboard on`) and note the caveat on stderr. ESC/BEL as
  // \u escapes per the repo's no-raw-control-bytes convention.
  private def copyFallback(payload: String): Task[Unit] =
    if (hasTty) {
      val b64   = Base64.getEncoder.encodeToString(payload.getBytes(StandardCharsets.UTF_8))
      val osc52 = "\u001b]52;c;" + b64 + "\u0007"
      Console.print(osc52).orDie *>
        Console.printLineError(
          "no clipboard tool found (install wl-copy or xclip); sent an OSC 52 copy escape as a fallback — it only " +
            "works if your terminal supports it. The list is shown above."
        ).orDie
    } else {
      Console.printLineError(
        "clipboard unavailable (no clipboard tool and not an interactive terminal); the list is shown above."
      ).orDie
    }

  private def parseMisfire(s: String): IO[CliError, MisfirePolicy] =
    s.trim.toLowerCase match {
      case "skip"                 => ZIO.succeed(MisfirePolicy.Skip)
      case "catch_up" | "catchup" => ZIO.succeed(MisfirePolicy.CatchUp)
      case other                  => ZIO.fail(CliError(s"invalid --misfire '$other' (expected skip or catch_up)", 2))
    }

  // One-line trigger description for a schedule response (interval vs cron).
  private def triggerSummary(s: ScheduleResponse): String =
    s.cron match {
      case Some(c) => s"cron '$c' (${s.timezone.getOrElse("UTC")}, misfire ${s.misfire.getOrElse("skip")})"
      case None    => s"every ${s.intervalHours.getOrElse(0)}h"
    }

  private def printJobs(jobs: List[JobStatusResponse]): UIO[Unit] =
    if (jobs.isEmpty) { Console.printLine("no jobs").orDie }
    else {
      ZIO.foreachDiscard(jobs)(j =>
        Console
          .printLine(s"${j.id}  ${j.kind}  ${j.status}  started=${j.startedAt}${j.error.fold("")(e => s"  error=$e")}")
          .orDie
      )
    }

  private def printSchedules(schedules: List[ScheduleResponse]): UIO[Unit] =
    if (schedules.isEmpty) { Console.printLine("no schedules").orDie }
    else {
      ZIO.foreachDiscard(schedules)(s =>
        Console
          .printLine(
            s"#${s.id}  ${s.kind}  ${triggerSummary(s)}  enabled=${s.enabled}" +
              s.clubIdOption.fold("")(c => s"  club=$c")
          )
          .orDie
      )
    }

  private def printBlacklist(entries: List[BlacklistEntryResponse]): UIO[Unit] =
    if (entries.isEmpty) { Console.printLine("blacklist is empty").orDie }
    else {
      ZIO.foreachDiscard(entries)(e =>
        Console
          .printLine(
            s"${e.username.getOrElse(s"player#${e.playerId}")}  added=${e.addedAt}" +
              s"${e.expiresAt.fold("")(x => s"  expires=$x")}${e.reason.fold("")(r => s"  reason=$r")}"
          )
          .orDie
      )
    }
}
