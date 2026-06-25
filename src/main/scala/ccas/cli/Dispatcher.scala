package ccas.cli

import zio.*
import zio.http.Client

import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.server.routes.BlacklistRoutes.{BlacklistEntryResponse, CreateBlacklistRequest}
import ccas.server.routes.ManagedClubRoutes.{ManagedClubResponse, MarkManagedRequest}
import ccas.server.routes.JobRoutes.{
  ClubJobResult,
  HistoryRequest,
  JobResult,
  JobStatusResponse,
  MembershipRequest,
  RecruitmentRequest,
  StatsRequest
}
import ccas.server.routes.ScheduleRoutes.{CreateScheduleRequest, ScheduleResponse}

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

    case CliCommand.Recruit(_, club, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore) =>
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
            explore
          )
        ).flatMap(follower.handleSingle(ClubSlug.unwrap(slug), _))
      )

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

    case CliCommand.ScheduleAdd(_, kind, intervalHours, club, params) =>
      api.postJson[CreateScheduleRequest, ScheduleResponse](
        "/api/schedules",
        CreateScheduleRequest(kind, club, params, intervalHours)
      ).flatMap(s => Console.printLine(s"created schedule ${s.id} (${s.kind}, every ${s.intervalHours}h)").orDie.as(0))

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
            s"#${s.id}  ${s.kind}  every ${s.intervalHours}h  enabled=${s.enabled}${s.clubId.fold("")(c => s"  club=$c")}"
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
