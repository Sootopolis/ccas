package ccas.cli

import zio.*
import zio.http.Client

import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.server.routes.BlacklistRoutes.{BlacklistEntryResponse, CreateBlacklistRequest}
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

  private val PollInterval: Duration = 2.seconds
  private val MaxJobWait: Duration   = 60.minutes

  def dispatch(cmd: CliCommand): URIO[Any, ExitCode] =
    CcasApiClient
      .live(cmd.server)
      .flatMap(api => runCommand(api, JobPoller(api, PollInterval, MaxJobWait), cmd))
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

  private def runCommand(api: CcasApiClient, poller: JobPoller, cmd: CliCommand): Task[Int] = cmd match {
    case _: CliCommand.Serve => ZIO.succeed(0) // handled in Main

    case CliCommand.Membership(_, slugs, trust) =>
      api.postJson[MembershipRequest, List[ClubJobResult]](
        "/api/jobs/membership",
        MembershipRequest(toSlugs(slugs), trust)
      ).flatMap(poller.handleBatch)

    case CliCommand.History(_, slugs, full, includeFinished, refresh, refreshMinHours) =>
      api.postJson[HistoryRequest, List[ClubJobResult]](
        "/api/jobs/history",
        HistoryRequest(toSlugs(slugs), flag(full), flag(includeFinished), flag(refresh), refreshMinHours)
      ).flatMap(poller.handleBatch)

    case CliCommand.Recruit(_, slug, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore) =>
      api.postJson[RecruitmentRequest, JobResult](
        "/api/jobs/recruitment",
        RecruitmentRequest(
          ClubSlug(slug),
          alias,
          target,
          flag(cumulative),
          Option.when(sourceClubs.nonEmpty)(sourceClubs.map(ClubSlug(_))),
          timeLimitMinutes,
          explore
        )
      ).flatMap(poller.handleSingle(slug, _))

    case CliCommand.Stats(_, slug, since, until) =>
      api.postJson[StatsRequest, ClubJobResult](
        "/api/jobs/stats",
        StatsRequest(ClubSlug(slug), since, until)
      ).flatMap(poller.handleClubSingle)

    case CliCommand.Jobs(_, limit) =>
      api.getJson[List[JobStatusResponse]]("/api/jobs").flatMap(all => printJobs(limit.fold(all)(all.take)).as(0))

    case CliCommand.Logs(_, jobId) =>
      poller.pollOne(jobId)

    case CliCommand.BlacklistAdd(_, slug, usernames, reason, months) =>
      api.postUnit[CreateBlacklistRequest](
        "/api/blacklist",
        CreateBlacklistRequest(ClubSlug(slug), usernames.map(Username(_)), reason, months)
      ) *> Console.printLine(s"blacklisted ${usernames.mkString(", ")} for $slug").orDie.as(0)

    case CliCommand.BlacklistList(_, slug) =>
      api.getJson[List[BlacklistEntryResponse]](s"/api/blacklist/$slug").flatMap(entries => printBlacklist(entries).as(0))

    case CliCommand.BlacklistRemove(_, slug, username) =>
      api.delete(s"/api/blacklist/$slug/$username") *>
        Console.printLine(s"removed $username from $slug blacklist").orDie.as(0)

    case CliCommand.ScheduleList(_) =>
      api.getJson[List[ScheduleResponse]]("/api/schedules").flatMap(schedules => printSchedules(schedules).as(0))

    case CliCommand.ScheduleAdd(_, kind, intervalHours, club, params) =>
      api.postJson[CreateScheduleRequest, ScheduleResponse](
        "/api/schedules",
        CreateScheduleRequest(kind, club, params, intervalHours)
      ).flatMap(s => Console.printLine(s"created schedule ${s.id} (${s.kind}, every ${s.intervalHours}h)").orDie.as(0))

    case CliCommand.ScheduleRemove(_, id) =>
      api.delete(s"/api/schedules/$id") *> Console.printLine(s"deleted schedule $id").orDie.as(0)
  }

  // zio-cli's `repeat1` guarantees a non-empty list; the empty branch is unreachable (and caught by dispatch's
  // defect net if a future parser change ever violated that).
  private def toSlugs(slugs: List[String]): NonEmptyChunk[ClubSlug] =
    NonEmptyChunk
      .fromIterableOption(slugs.map(ClubSlug(_)))
      .getOrElse(throw new IllegalStateException("membership/history require at least one slug"))

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
