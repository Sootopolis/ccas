package ccas.cli

import java.nio.charset.StandardCharsets
import java.util.Base64

import zio.*
import zio.http.Client

import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.server.routes.BlacklistRoutes.{BlacklistEntryResponse, CreateBlacklistRequest}
import ccas.server.routes.ManagedClubRoutes.{ManagedClubResponse, MarkManagedRequest}
import ccas.server.routes.JobRoutes.{
  ClubJobResult,
  ConfirmResult,
  HistoryRequest,
  InvitedUsernames,
  JobResult,
  JobStatusResponse,
  MembershipRequest,
  RecruitmentRequest,
  StatsRequest
}
import ccas.server.routes.ScheduleRoutes.{CreateScheduleRequest, ScheduleResponse}
import ccas.server.scheduler.{MisfirePolicy, TriggerType}

/** Maps a parsed [[CliCommand]] to HTTP calls against the local server and renders the result, returning the process
  * exit code. `Serve` is handled in [[Main]] (it boots the server rather than calling it), never here.
  */
object Dispatcher {

  private val MaxJobWait: Duration = 60.minutes

  def dispatch(cmd: CliCommand.ServerCommand, currentClub: Option[String]): UIO[ExitCode] =
    CcasApiClient
      .live(cmd.server)
      .flatMap(api => runCommand(api, JobFollower(api, MaxJobWait), cmd, currentClub).tap(_ => refreshClubsCache(api)))
      .provide(Client.default)
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

  // Best-effort, staleness-gated refresh of the completion club-slug cache after a successful command. Fully ignored:
  // it never blocks the result or alters the exit code (the `.tap` runs only on the success channel).
  private def refreshClubsCache(api: CcasApiClient): UIO[Unit] =
    CompletionCache.clubsStale.flatMap { stale =>
      ZIO.whenDiscard(stale) {
        api
          .getJson[CompletionCache.ClubsDto]("/api/clubs")
          .map(_.clubs.map(_.slug))
          .flatMap(CompletionCache.writeClubs)
          .ignore
      }
    }

  private def runCommand(
    api: CcasApiClient,
    follower: JobFollower,
    cmd: CliCommand.ServerCommand,
    currentClub: Option[String]
  ): Task[Int] = cmd match {
    case CliCommand.Membership(_, clubs, all, trust) =>
      resolveClubs(api, clubs, all, currentClub).flatMap(slugs =>
        api.postJson[MembershipRequest, List[ClubJobResult]](
          "/api/jobs/membership",
          MembershipRequest(slugs, trust)
        ).flatMap(follower.handleBatch)
      )

    case CliCommand.History(_, clubs, all, full, includeFinished, refresh, refreshMinHours) =>
      resolveClubs(api, clubs, all, currentClub).flatMap(slugs =>
        api.postJson[HistoryRequest, List[ClubJobResult]](
          "/api/jobs/history",
          HistoryRequest(slugs, flag(full), flag(includeFinished), flag(refresh), refreshMinHours)
        ).flatMap(follower.handleBatch)
      )

    case CliCommand.Recruit(_, club, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore, stdout, report, runId) =>
      // Guard the read-only flag: a run-id argument without `--report` would otherwise be silently dropped and launch
      // a fresh scout — and with `--stdout` it would auto-confirm that scout's invites. Fail before submitting.
      if (runId.isDefined && !report) { ZIO.fail(CliError("a run id can only be given with --report", 2)) }
      else if (report) { reportInvited(api, club, currentClub, runId, stdout) }
      else {
        // Marking Invited is destructive (a forgotten invite burns the candidate for the cooldown), so it needs
        // positive intent: only `--stdout` (programmatic consumption) auto-confirms from the CLI. An interactive run
        // prompts; any other non-interactive run (e.g. stdout redirected to a file) defers rather than silently
        // inviting. `autoConfirm = Some(false)` for everything but `--stdout` tells the server to leave them Deferred.
        val interactiveConfirm = !stdout && hasTty
        resolveClub(club, currentClub).flatMap(slug =>
          api.postJson[RecruitmentRequest, JobResult](
            "/api/jobs/recruitment",
            RecruitmentRequest(
              slug,
              alias,
              target,
              flag(cumulative),
              Option.when(sourceClubs.nonEmpty)(sourceClubs.map(ClubSlug(_))),
              timeLimitMinutes,
              explore,
              Option.unless(stdout)(false)
            )
          ).flatMap(result =>
            handleRecruitResult(follower, api, ClubSlug.unwrap(slug), result, stdout, interactiveConfirm)
          )
        )
      }

    case CliCommand.Stats(_, club, since, until) =>
      resolveClub(club, currentClub).flatMap(slug =>
        api.postJson[StatsRequest, ClubJobResult](
          "/api/jobs/stats",
          StatsRequest(slug, since, until)
        ).flatMap(follower.handleClubSingle)
      )

    case CliCommand.Jobs(_, limit) =>
      api.getJson[List[JobStatusResponse]]("/api/jobs").flatMap(all => printJobs(limit.fold(all)(all.take)).as(0))

    case CliCommand.Logs(_, jobId) =>
      follower.followJob(jobId)

    case CliCommand.BlacklistAdd(_, club, usernames, reason, months) =>
      resolveClub(club, currentClub).flatMap(slug =>
        api.postUnit[CreateBlacklistRequest](
          "/api/blacklist",
          CreateBlacklistRequest(slug, usernames.map(Username(_)), reason, months)
        ) *> Console.printLine(s"blacklisted ${usernames.mkString(", ")} for ${ClubSlug.unwrap(slug)}").orDie.as(0)
      )

    case CliCommand.BlacklistList(_, club) =>
      resolveClub(club, currentClub).flatMap(slug =>
        api.getJson[List[BlacklistEntryResponse]](s"/api/blacklist/${ClubSlug.unwrap(slug)}")
          .flatMap(entries => printBlacklist(entries).as(0))
      )

    case CliCommand.BlacklistRemove(_, club, username) =>
      resolveClub(club, currentClub).flatMap(slug =>
        api.delete(s"/api/blacklist/${ClubSlug.unwrap(slug)}/$username") *>
          Console.printLine(s"removed $username from ${ClubSlug.unwrap(slug)} blacklist").orDie.as(0)
      )

    case CliCommand.ScheduleList(_) =>
      api.getJson[List[ScheduleResponse]]("/api/schedules").flatMap(schedules => printSchedules(schedules).as(0))

    case CliCommand.ScheduleAdd(_, kind, intervalHours, cron, tz, misfire, club, params) =>
      // --interval-hours and --cron are mutually exclusive (exactly one required); the server enforces the XOR
      // and returns a clean 400 if both/neither are given. triggerType follows whether --cron is present.
      for {
        mp <- ZIO.foreach(misfire)(parseMisfire)
        triggerType = if (cron.isDefined) TriggerType.Cron else TriggerType.Interval
        s <- api.postJson[CreateScheduleRequest, ScheduleResponse](
          "/api/schedules",
          CreateScheduleRequest(kind, club, params, Some(triggerType), intervalHours, cron, tz, mp)
        )
        _ <- Console.printLine(s"created schedule ${s.id} (${s.kind}, ${triggerSummary(s)})").orDie
      } yield 0

    case CliCommand.ScheduleRemove(_, id) =>
      api.delete(s"/api/schedules/$id") *> Console.printLine(s"deleted schedule $id").orDie.as(0)

    case CliCommand.ClubsAdd(_, slug) =>
      val add = ClubSlug(slug.trim)
      api.postUnit[MarkManagedRequest]("/api/managed-clubs", MarkManagedRequest(add)) *>
        Console.printLine(s"now managing ${ClubSlug.unwrap(add)}").orDie.as(0)

    case CliCommand.ClubsRemove(_, slug) =>
      val remove = ClubSlug(slug.trim)
      api.delete(s"/api/managed-clubs/${ClubSlug.unwrap(remove)}") *>
        Console.printLine(s"stopped managing ${ClubSlug.unwrap(remove)}").orDie.as(0)

    case CliCommand.ClubsList(_) =>
      api.getJson[List[ManagedClubResponse]]("/api/managed-clubs").flatMap(clubs => printManagedClubs(clubs).as(0))
  }

  private def printManagedClubs(clubs: List[ManagedClubResponse]): UIO[Unit] =
    if (clubs.isEmpty) { Console.printLine("no managed clubs").orDie }
    else {
      ZIO.foreachDiscard(clubs)(c => Console.printLine(s"${c.slug}  ${c.name}  marked=${c.markedAt}").orDie)
    }

  // Resolution lives in the pure, testable `ClubResolver`; the `--all` expansion's network call is injected here.
  private def resolveClub(explicit: Option[String], currentClub: Option[String]): IO[CliError, ClubSlug] =
    ClubResolver.single(explicit, currentClub)

  private def resolveClubs(
    api: CcasApiClient,
    explicit: List[String],
    all: Boolean,
    currentClub: Option[String]
  ): Task[NonEmptyChunk[ClubSlug]] =
    ClubResolver.multi(
      api.getJson[List[ManagedClubResponse]]("/api/managed-clubs").map(_.map(_.slug)),
      explicit,
      all,
      currentClub
    )

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
    label: String,
    result: JobResult,
    stdout: Boolean,
    interactiveConfirm: Boolean
  ): Task[Int] =
    if (stdout) { follower.handleRecruit(label, result, logsToStderr = true, deliverInvited(api, _)) }
    else if (interactiveConfirm) { follower.handleRecruit(label, result, logsToStderr = false, confirmFlow(api, _)) }
    else { follower.handleRecruit(label, result, logsToStderr = true, deferReport(api, _)) }

  // Fetch a completed auto-confirm scout's invited usernames and print them bare to stdout. A fetch/decode failure is
  // not propagated (the scout already succeeded and is persisted): `deliveryFailed` notes it and the command exits 0.
  private def deliverInvited(api: CcasApiClient, jobId: String): Task[Unit] =
    api.getJson[InvitedUsernames](s"/api/jobs/$jobId/recruitment/invited")
      .flatMap(r => printBare(r.usernames))
      .catchAll(deliveryFailed)

  // Bare newline list to stdout — the pipe payload for `--stdout` (logs already routed to stderr).
  private def printBare(names: List[String]): Task[Unit] =
    ZIO.foreachDiscard(names)(n => Console.printLine(n).orDie)

  // Deferred-confirm flow: the scout left candidates Deferred. Show them, ask, and only then flip to Invited + copy.
  // Declining (or a read failure defaulting to "n") leaves them Deferred — nobody is burned, and they resurface.
  private def confirmFlow(api: CcasApiClient, jobId: String): Task[Unit] =
    api.getJson[InvitedUsernames](s"/api/jobs/$jobId/recruitment/found").flatMap { found =>
      if (found.usernames.isEmpty) { Console.printLine("No candidates found.").orDie }
      else { confirmPrompt(api, jobId, found.usernames) }
    }.catchAll(confirmFailed)

  private def confirmPrompt(api: CcasApiClient, jobId: String, names: List[String]): Task[Unit] =
    for {
      _      <- Console.printLine(s"\nFound ${names.size} candidates:").orDie
      _      <- ZIO.foreachDiscard(names)(n => Console.printLine(s"  $n").orDie)
      answer <- ZIO.attemptBlocking(scala.io.StdIn.readLine(s"\nMark all ${names.size} as Invited and copy to clipboard? [Y/n] "))
        .orElseSucceed("n")
      _ <-
        if (isYes(answer)) { confirmAndCopy(api, jobId) }
        else { Console.printLine(s"Left ${names.size} as deferred; they'll resurface on the next run.").orDie }
    } yield ()

  private def confirmAndCopy(api: CcasApiClient, jobId: String): Task[Unit] =
    api.postEmpty[ConfirmResult](s"/api/jobs/$jobId/recruitment/confirm").flatMap { r =>
      Console.printLine(s"Marked ${r.marked} as invited.").orDie *> copyToClipboard(r.usernames)
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

  // Report a past run's invited usernames (`ccas recruit --report`). --run picks a specific run; otherwise the club's
  // latest. Prints the list to stdout and, on an interactive terminal (no --stdout), also copies it.
  private def reportInvited(
    api: CcasApiClient,
    club: Option[String],
    currentClub: Option[String],
    runId: Option[Int],
    stdout: Boolean
  ): Task[Int] =
    for {
      path <- runId match {
        case Some(id) => ZIO.succeed(s"/api/recruitment/runs/$id/invited")
        case None =>
          resolveClub(club, currentClub).map(slug => s"/api/recruitment/clubs/${ClubSlug.unwrap(slug)}/latest/invited")
      }
      invited <- api.getJson[InvitedUsernames](path)
      _       <- renderReport(invited.usernames, stdout)
    } yield 0

  private def renderReport(names: List[String], stdout: Boolean): Task[Unit] =
    if (names.isEmpty) { Console.printLineError("no invited usernames for that run").orDie }
    else {
      printBare(names) *> ZIO.whenDiscard(!stdout && hasTty)(copyToClipboard(names))
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

  // OSC 52 clipboard write: `ESC ] 52 ; c ; <base64 payload> BEL`. The terminal itself owns the clipboard, so this
  // works over SSH (tmux needs `set -g set-clipboard on`). Payloads are a handful of usernames, far under any
  // terminal's OSC 52 size cap. ESC/BEL as \u escapes per the repo's no-raw-control-bytes convention.
  private def copyToClipboard(names: List[String]): Task[Unit] =
    if (names.isEmpty) { Console.printLineError("no invited usernames to copy").orDie }
    // Callers only reach here on a TTY, but guard defensively: an OSC 52 escape to a non-terminal would corrupt the
    // stream and no terminal is there to honour it, so fall back to just printing the usernames.
    else if (!hasTty) {
      Console.printLineError("clipboard unavailable (not an interactive terminal); printing usernames instead").orDie *>
        printBare(names)
    } else {
      val b64   = Base64.getEncoder.encodeToString(names.mkString("\n").getBytes(StandardCharsets.UTF_8))
      val osc52 = "\u001b]52;c;" + b64 + "\u0007"
      Console.print(osc52).orDie *> Console.printLine(s"Copied ${names.size} usernames.").orDie
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
            s"#${s.id}  ${s.kind}  ${triggerSummary(s)}  enabled=${s.enabled}${s.clubId.fold("")(c => s"  club=$c")}"
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
