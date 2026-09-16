package ccas.server.routes

import java.nio.charset.StandardCharsets
import java.time.Instant

import scala.util.chaining.*

import zio.{IO, NonEmptyChunk, RIO, URIO, ZIO}
import zio.http.*
import zio.json.{jsonField, DeriveJsonCodec, EncoderOps, JsonCodec}
import zio.stream.ZStream

import ccas.analysis.apps.ClubResolution
import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.{Club, Player, RecruitmentCandidate, RecruitmentRun, RunTrigger}
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId, Username}
import ccas.server.jobs.*
import ccas.server.routes.RouteHelpers.*
import ccas.utils.TimeParser
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, ConflictException, ErrorResponse}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

object JobRoutes {

  // --- Request types ---

  // Recruitment caps (`JobCaps.MaxTarget` / `JobCaps.MaxTimeLimitMinutes`) are shared with the scheduled-job
  // path (`ScheduleParams`) so both submission routes apply identical bounds.

  // `clubId` (on every club-scoped request) is the target club's stable Chess.com id, sent by the CLI when it has one
  // cached for the resolved `current_club`. When present the server resolves by id — rename-proof — and runs the job
  // against the club's canonical slug rather than the (possibly stale) `clubSlug` the CLI echoed; absent, it resolves
  // `clubSlug` as a current or former name (ADR 0016). Optional so a raw API caller still works (#176, #180).
  private[ccas] case class RecruitmentRequest(
    clubSlug: ClubSlug,
    alias: Option[String],
    target: Option[Int],
    cumulative: Option[Boolean],
    sourceClubs: Option[List[ClubSlug]],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean],
    // When Some(false) the scout leaves candidates Deferred for the CLI to confirm; absent/Some(true) auto-confirms
    // (scheduler, raw API, and non-interactive `ccas recruit`). The interactive CLI sends false.
    autoConfirm: Option[Boolean],
    @jsonField("clubId") clubIdOption: Option[ClubId] = None
  )
  object RecruitmentRequest {
    given JsonCodec[RecruitmentRequest] = DeriveJsonCodec.gen
  }

  // The batch DTOs carry a slug list, but the CLI always submits ONE club per call, so `clubId` is a scalar honoured
  // only when `clubSlugs` is single-element (a genuine multi-club batch — which the CLI never sends — leaves it None and
  // resolves each by slug). See [[RecruitmentRequest]] for the id-vs-slug resolution rationale.
  private[ccas] case class MembershipRequest(
    clubSlugs: NonEmptyChunk[ClubSlug],
    trustUsernames: Option[Boolean],
    @jsonField("clubId") clubIdOption: Option[ClubId] = None
  )
  object MembershipRequest {
    given JsonCodec[MembershipRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class HistoryRequest(
    clubSlugs: NonEmptyChunk[ClubSlug],
    full: Option[Boolean],
    includeFinished: Option[Boolean],
    refresh: Option[Boolean],
    refreshMinHours: Option[Int],
    @jsonField("clubId") clubIdOption: Option[ClubId] = None
  )
  object HistoryRequest {
    given JsonCodec[HistoryRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class StatsRequest(
    clubSlug: ClubSlug,
    since: Option[String],
    until: Option[String],
    @jsonField("clubId") clubIdOption: Option[ClubId] = None
  )
  object StatsRequest {
    given JsonCodec[StatsRequest] = DeriveJsonCodec.gen
  }

  // --- Response types ---

  /** Result of submitting the club-less matchref job. */
  private[ccas] case class JobResult(@jsonField("jobId") jobIdOption: Option[String], error: Option[String])
  object JobResult {
    given JsonCodec[JobResult] = DeriveJsonCodec.gen
  }

  /** Result of submitting a club-scoped job (recruitment, membership, history, stats). `clubSlug` echoes the requested
    * slug, which the CLI matches results and invalidates its cache by. `resolution` is how that slug resolved: the club
    * the job runs against, which the CLI reads to freshen a stale `current_club`, or why it did not run. `error`
    * carries only failures past resolution, such as a job already running.
    */
  private[ccas] case class ClubJobResult(
    clubSlug: String,
    @jsonField("jobId") jobIdOption: Option[String],
    error: Option[String],
    resolution: ClubResolution
  ) {

    /** Why the job did not start, if it did not. */
    def failure: Option[String] = error.orElse(resolution.runnable.left.toOption)
  }
  object ClubJobResult {
    given JsonCodec[ClubJobResult] = DeriveJsonCodec.gen
  }

  /** Paste-ready invited usernames for a completed recruitment job (drives `ccas recruit --stdout` and the interactive
    * confirm/report flows). */
  private[ccas] case class InvitedUsernames(usernames: List[String])
  object InvitedUsernames {
    given JsonCodec[InvitedUsernames] = DeriveJsonCodec.gen
  }

  /** Confirm-endpoint result: `marked` is the actual number of rows flipped Deferred→Invited (may exceed `usernames`
    * if some invited player id doesn't resolve to a handle), `usernames` is the paste-ready resolved subset. */
  private[ccas] case class ConfirmResult(marked: Int, usernames: List[String])
  object ConfirmResult {
    given JsonCodec[ConfirmResult] = DeriveJsonCodec.gen
  }

  /** Result of a cancel request: `jobId` echoed back. A 200 means the interrupt was dispatched to a live job fiber
    * ("cancellation requested"); a missing / already-terminal job is a 404 instead. */
  private[ccas] case class CancelResult(jobId: String)
  object CancelResult {
    given JsonCodec[CancelResult] = DeriveJsonCodec.gen
  }

  private[ccas] case class JobStatusResponse(
    id: String,
    kind: String,
    status: String,
    @jsonField("clubId") clubIdOption: Option[Long],
    startedAt: String,
    completedAt: Option[String],
    error: Option[String],
    trigger: String
  )
  object JobStatusResponse {
    given JsonCodec[JobStatusResponse] = DeriveJsonCodec.gen

    def fromJobRun(jr: JobRun): JobStatusResponse =
      JobStatusResponse(
        id = JobRunId.unwrap(jr.id),
        kind = jr.kind.toString,
        status = jr.status.toString,
        clubIdOption = jr.clubIdOption.map(ClubId.unwrap),
        startedAt = jr.startedAt.toString,
        completedAt = jr.completedAt.map(_.toString),
        error = jr.error,
        trigger = jr.trigger.toString
      )
  }

  // --- Routes ---

  val routes: Routes[JobRunner & ChessComClient & PostgresClient, Nothing] = Routes(
    Method.POST / "api" / "jobs" / "recruitment" ->
      handler((req: Request) => submitRecruitment(req)),
    Method.POST / "api" / "jobs" / "membership" ->
      handler((req: Request) => submitMembership(req)),
    Method.POST / "api" / "jobs" / "matchref" ->
      handler(submitMatchRef),
    Method.POST / "api" / "jobs" / "history" ->
      handler((req: Request) => submitHistory(req)),
    Method.POST / "api" / "jobs" / "stats" ->
      handler((req: Request) => submitStats(req)),
    Method.GET / "api" / "jobs" ->
      handler(listJobs),
    Method.GET / "api" / "jobs" / string("jobId") ->
      handler((jobId: String, _: Request) => jobStatus(jobId)),
    Method.POST / "api" / "jobs" / string("jobId") / "cancel" ->
      handler((jobId: String, _: Request) => cancelJob(jobId)),
    Method.GET / "api" / "jobs" / string("jobId") / "logs" ->
      handler((jobId: String, _: Request) => jobLogs(jobId)),
    Method.GET / "api" / "jobs" / string("jobId") / "progress" ->
      handler((jobId: String, _: Request) => jobProgress(jobId)),
    Method.GET / "api" / "jobs" / string("jobId") / "recruitment" / "invited" ->
      handler((jobId: String, _: Request) => invitedForJob(jobId)),
    Method.GET / "api" / "jobs" / string("jobId") / "recruitment" / "found" ->
      handler((jobId: String, _: Request) => foundForJob(jobId)),
    Method.POST / "api" / "jobs" / string("jobId") / "recruitment" / "confirm" ->
      handler((jobId: String, _: Request) => confirmForJob(jobId)),
    Method.GET / "api" / "recruitment" / "clubs" / string("slug") / "latest" / "invited" ->
      handler((slug: String, _: Request) => latestInvitedForClub(slug)),
    Method.GET / "api" / "recruitment" / "runs" / string("runId") / "invited" ->
      handler((runId: String, _: Request) => invitedForRun(runId))
  )

  // --- Job submission ---

  private def submitRecruitment(req: Request): URIO[JobRunner & PostgresClient, Response] =
    (for {
      body   <- parseJsonBody[RecruitmentRequest](req)
      runner <- ZIO.service[JobRunner]
      result <- submitClubJob(runner, JobKind.Recruitment, body.clubIdOption, body.clubSlug, Some(body.toJson)) {
        (club, jobRunIdOption) =>
          RecruitmentApp.recruit(
            clubSlug = club.slug,
            expectedClubIdOption = Some(club.clubId),
            alias = body.alias.getOrElse("default"),
            target = body.target.map(_ min JobCaps.MaxTarget),
            cumulative = body.cumulative.getOrElse(false),
            sourceClubs = body.sourceClubs.getOrElse(Nil),
            timeLimitMinutes = body.timeLimitMinutes.map(_ min JobCaps.MaxTimeLimitMinutes),
            explore = body.explore.getOrElse(true),
            trigger = RunTrigger.Api,
            autoConfirm = body.autoConfirm.getOrElse(true),
            jobRunIdOption = jobRunIdOption
          )
      }
    } yield jsonResponse(Status.Ok, result)).pipe(withErrorHandling)

  private def submitMembership(req: Request): URIO[JobRunner & PostgresClient, Response] =
    (for {
      body   <- parseJsonBody[MembershipRequest](req)
      runner <- ZIO.service[JobRunner]
      results <- submitClubJobs(runner, JobKind.Membership, body.clubSlugs, body.clubIdOption, Some(body.toJson)) {
        (club, jobRunIdOption) =>
          MembershipApp.reconcileAndReport(
            clubSlug = club.slug,
            expectedClubIdOption = Some(club.clubId),
            trustUsernames = body.trustUsernames.getOrElse(true),
            trigger = RunTrigger.Api,
            jobRunIdOption = jobRunIdOption
          )
      }
    } yield jsonResponse(Status.Ok, results)).pipe(withErrorHandling)

  private def submitMatchRef: URIO[JobRunner & PostgresClient, Response] =
    (for {
      runner <- ZIO.service[JobRunner]
      submitted <- jobIdOrConflict(
        runner.submit(
          kind = JobKind.MatchRef,
          clubIdOption = None,
          params = None,
          trigger = RunTrigger.Api,
          effect = _ => RefApp.populate(forceSkipped = false, upgradeRefs = false)
        )
      )
    } yield jsonResponse(Status.Ok, JobResult(jobIdOption = submitted.toOption, error = submitted.left.toOption)))
      .pipe(withErrorHandling)

  private def submitHistory(req: Request): URIO[JobRunner & PostgresClient, Response] =
    (for {
      body   <- parseJsonBody[HistoryRequest](req)
      runner <- ZIO.service[JobRunner]
      refreshMinHours = body.refreshMinHours.orElse(body.refresh.filter(identity).map(_ => 0))
      results <- submitClubJobs(runner, JobKind.History, body.clubSlugs, body.clubIdOption, Some(body.toJson)) {
        (club, jobRunIdOption) =>
          HistoryApp.discover(
            clubSlug = club.slug,
            expectedClubIdOption = Some(club.clubId),
            full = body.full.getOrElse(false),
            includeFinished = body.includeFinished.getOrElse(false),
            refreshMinHours = refreshMinHours,
            trigger = RunTrigger.Api,
            jobRunIdOption = jobRunIdOption
          )
      }
    } yield jsonResponse(Status.Ok, results)).pipe(withErrorHandling)

  private def submitStats(req: Request): URIO[JobRunner & PostgresClient, Response] =
    (for {
      body         <- parseJsonBody[StatsRequest](req)
      runner       <- ZIO.service[JobRunner]
      periodOption <- statsPeriod(body)
      result <- submitClubJob(runner, JobKind.Stats, body.clubIdOption, body.clubSlug, Some(body.toJson)) { (club, _) =>
        periodOption match {
          // minGames=1 mirrors the CLI default; StatsRequest carries no min-games field.
          case Some((since, until)) => StatsApp.playerOfPeriodAndReport(club.clubId, since, until, 1)
          case None                 => StatsApp.memberStatsAndReport(club.clubId)
        }
      }
    } yield jsonResponse(Status.Ok, result)).pipe(withErrorHandling)

  private def statsPeriod(body: StatsRequest): IO[BadRequestException, Option[(Instant, Instant)]] =
    (body.since, body.until) match {
      case (Some(sinceString), Some(untilString)) =>
        for {
          since <- TimeParser.parseInstantZIO(sinceString).mapError(e => BadRequestException(s"Invalid 'since': $e"))
          until <- TimeParser.parseInstantZIO(untilString).mapError(e => BadRequestException(s"Invalid 'until': $e"))
        } yield Some((since, until))
      case (None, None) => ZIO.none
      case _            => ZIO.fail(BadRequestException("Both 'since' and 'until' are required for period stats"))
    }

  // --- Job inspection ---

  private def listJobs: URIO[JobRunner & PostgresClient, Response] =
    (for {
      runner <- ZIO.service[JobRunner]
      jobs   <- runner.recentJobs(50)
    } yield jsonResponse(Status.Ok, jobs.map(JobStatusResponse.fromJobRun))).pipe(withErrorHandling)

  private def jobStatus(jobId: String): URIO[JobRunner & PostgresClient, Response] =
    (for {
      runner    <- ZIO.service[JobRunner]
      jobOption <- runner.status(JobRunId.wrap(jobId))
    } yield jobOption match {
      case Some(job) => jsonResponse(Status.Ok, JobStatusResponse.fromJobRun(job))
      case None      => jsonResponse(Status.NotFound, ErrorResponse(s"Job $jobId not found"))
    }).pipe(withErrorHandling)

  /** Interrupts a running job's fiber (best-effort, async — the job records `Cancelled` itself as it unwinds). 200 if a
    * live job fiber was found and interrupted; 404 if the id is unknown, already terminal, or (unsupported
    * multi-server) owned by another instance. POST, not DELETE: the row is retained, this is a state transition.
    */
  private def cancelJob(jobId: String): URIO[JobRunner, Response] =
    (for {
      runner    <- ZIO.service[JobRunner]
      cancelled <- runner.cancel(JobRunId.wrap(jobId))
    } yield
      if (cancelled) { jsonResponse(Status.Ok, CancelResult(jobId)) }
      else { jsonResponse(Status.NotFound, ErrorResponse(s"No running job $jobId to cancel")) }
    ).pipe(withErrorHandling)

  /** Chunked `text/plain` stream of a job's log lines. Stays open while the job runs (lines arrive as emitted) and
    * closes once the job is terminal and the tail reaches EOF, so a client can treat body-close as "job finished".
    */
  private def jobLogs(jobId: String): URIO[JobRunner & PostgresClient, Response] =
    (for {
      runner <- ZIO.service[JobRunner]
      logs   <- runner.logStream(JobRunId.wrap(jobId))
    } yield logs match {
      case JobLogs.NoSuchJob => Response.text(s"Job $jobId not found").status(Status.NotFound)
      // 410 rather than 404: the job is real and its row is still queryable, only its log is not. Retention is the
      // usual cause but not the only one — a sink that never opened writes no file either, so the text hedges.
      case JobLogs.Expired =>
        Response
          .text(s"Job $jobId has no log available — aged out of job_log_retention_days, or never written")
          .status(Status.Gone)
      case JobLogs.Streaming(lines) => lineStream(lines)
    }).pipe(withErrorHandling)

  /** Chunked NDJSON stream of a job's live progress: one `ProgressSnapshot` per line (latest-wins), merging the job's
    * app bars with the shared client's API gauge. Live-only — closes when the job is terminal. The following CLI opens
    * this only when it wants bars (interactive TTY, not `--no-progress`); it never affects the `/logs` follow.
    */
  private def jobProgress(jobId: String): URIO[JobRunner & PostgresClient, Response] =
    (for {
      runner       <- ZIO.service[JobRunner]
      framesOption <- runner.progressStream(JobRunId.wrap(jobId))
    } yield framesOption match {
      case None         => Response.text(s"Job $jobId not found").status(Status.NotFound)
      case Some(frames) => lineStream(frames)
    }).pipe(withErrorHandling)

  // Interleaves a keepalive tick so a job phase silent for >50s can't idle the follower's connection shut (#150).
  private def lineStream(lines: ZStream[Any, Throwable, String]): Response =
    Response(
      status = Status.Ok,
      headers = Headers(Header.ContentType(MediaType.text.`plain`, charset = Some(StandardCharsets.UTF_8))),
      body = Body.fromCharSequenceStreamChunked(JobLogStream.withKeepAlive(lines).map(_ + "\n"), StandardCharsets.UTF_8)
    )

  // --- Recruitment results ---

  /** Invited usernames for the recruitment run linked to a job — the paste-ready payload the CLI fetches once the job
    * is terminal (the `ccas recruit --stdout` auto-confirm path). 404 if the job id has no recruitment run. Scope is
    * THIS run only, deliberately: a `--cumulative` top-up returns just its new invites so the operator doesn't
    * re-paste players already invited earlier today.
    */
  private def invitedForJob(jobId: String): URIO[PostgresClient, Response] =
    candidatesByJobResponse(jobId, RecruitmentCandidate.selectInvitedByRun).pipe(withErrorHandling)

  /** Still-deferred candidates for a job's recruitment run — shown by interactive `ccas recruit` before the operator
    * confirms (a deferred-confirm run leaves everything Deferred). 404 if the job has no recruitment run.
    */
  private def foundForJob(jobId: String): URIO[PostgresClient, Response] =
    candidatesByJobResponse(jobId, RecruitmentCandidate.selectDeferredByRun).pipe(withErrorHandling)

  /** Confirms a deferred-confirm run: flips its Deferred candidates to Invited, records the count, returns the
    * confirmed usernames. A re-POST finds nothing deferred (flipped = 0) and returns the same already-invited list.
    */
  private def confirmForJob(jobId: String): URIO[PostgresClient, Response] =
    withRunForJob(jobId) { run =>
      for {
        // Flip and count-update share one transaction so a crash can't leave candidates Invited with the run's
        // candidates_found still 0 (which would make a later --cumulative run undercount and over-invite).
        flipped <- withTransaction {
          for {
            f <- RecruitmentCandidate.confirmDeferredByRun(run.runId)
            _ <- ZIO.whenDiscard(f > 0)(RecruitmentRun.setCandidatesFound(run.runId, f))
          } yield f
        }
        usernames <- RecruitmentCandidate.selectInvitedByRun(run.runId).flatMap(usernamesFor)
      } yield jsonResponse(Status.Ok, ConfirmResult(flipped, usernames))
    }.pipe(withErrorHandling)

  /** The latest recruitment run's invited usernames for a club — `ccas recruit --report`. */
  private def latestInvitedForClub(slug: String): URIO[PostgresClient, Response] =
    Club.selectBySlug(ClubSlug.wrap(slug)).flatMap {
      case None => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"Club not found: $slug")))
      case Some(club) =>
        RecruitmentRun.selectLatest(club.clubId).flatMap {
          case None      => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"No recruitment runs for $slug")))
          case Some(run) => invitedRunResponse(run.runId)
        }
    }.pipe(withErrorHandling)

  /** A specific recruitment run's invited usernames — `ccas recruit --report --run N`. */
  private def invitedForRun(runId: String): URIO[PostgresClient, Response] =
    (runId.toLongOption match {
      case None => ZIO.succeed(jsonResponse(Status.BadRequest, ErrorResponse(s"Invalid run id: $runId")))
      case Some(id) =>
        val rid = RecruitmentRunId.wrap(id)
        RecruitmentRun.selectId(rid).flatMap {
          case None    => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"Run $runId not found")))
          case Some(_) => invitedRunResponse(rid)
        }
    }).pipe(withErrorHandling)

  // --- Recruitment result helpers ---

  /** Resolve candidate rows to their bare usernames, dropping any that don't resolve — a `[pid=N]` placeholder is not
    * an invitable Chess.com handle, and these lists are meant to be pasted into invites.
    */
  private def usernamesFor(candidates: List[RecruitmentCandidate]): RIO[PostgresClient, List[String]] =
    Player.resolveUsernames(candidates.map(_.playerId)).map { resolved =>
      candidates.flatMap(c => resolved.get(c.playerId)).map(Username.unwrap)
    }

  /** Resolve the recruitment run linked to a job, or 404. Shared by the invited/found/confirm job-scoped endpoints. */
  private def withRunForJob(jobId: String)(f: RecruitmentRun => RIO[PostgresClient, Response]): RIO[PostgresClient, Response] =
    RecruitmentRun.selectByJobRunId(JobRunId.wrap(jobId)).flatMap {
      case None      => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"No recruitment run for job $jobId")))
      case Some(run) => f(run)
    }

  /** Wrap a candidate selection as a 200 response of bare resolved usernames. */
  private def usernamesResponse(select: RIO[PostgresClient, List[RecruitmentCandidate]]): RIO[PostgresClient, Response] =
    select.flatMap(usernamesFor).map(u => jsonResponse(Status.Ok, InvitedUsernames(u)))

  /** 404-or-usernames for a job-linked run, selecting candidate rows with `select` (invited or deferred). */
  private def candidatesByJobResponse(
    jobId: String,
    select: RecruitmentRunId => RIO[PostgresClient, List[RecruitmentCandidate]]
  ): RIO[PostgresClient, Response] =
    withRunForJob(jobId)(run => usernamesResponse(select(run.runId)))

  private def invitedRunResponse(runId: RecruitmentRunId): RIO[PostgresClient, Response] =
    usernamesResponse(RecruitmentCandidate.selectInvitedByRun(runId))

  // --- Helpers ---

  /** [[submitClubJob]] for each club of a batch request. Its `clubIdOption` applies only to a single-club submit: for a
    * genuine multi-club batch (which the CLI never sends) an id can't be shared across slugs, so it's dropped and each
    * club resolves by slug.
    */
  private def submitClubJobs(
    runner: JobRunner,
    kind: JobKind,
    requestedSlugs: NonEmptyChunk[ClubSlug],
    clubIdOption: Option[ClubId],
    params: Option[String]
  )(effect: ClubJobEffect): RIO[PostgresClient, List[ClubJobResult]] = {
    val singleClubIdOption = clubIdOption.filter(_ => requestedSlugs.size == 1)
    ZIO.foreach(requestedSlugs.toChunk.toList)(submitClubJob(runner, kind, singleClubIdOption, _, params)(effect))
  }

  /** Resolves a club (by id when the caller has one, else by the requested slug) and submits a single job built from
    * the *resolved* club, so a job addressed by a former name runs on the club's current slug instead of 404-ing on the
    * stale one.
    */
  private def submitClubJob(
    runner: JobRunner,
    kind: JobKind,
    clubIdOption: Option[ClubId],
    requestedSlug: ClubSlug,
    params: Option[String]
  )(effect: ClubJobEffect): RIO[PostgresClient, ClubJobResult] =
    ClubResolution.resolve(clubIdOption, requestedSlug).flatMap { resolution =>
      val unsubmitted = ClubJobResult(
        clubSlug = ClubSlug.unwrap(requestedSlug),
        jobIdOption = None,
        error = None,
        resolution = resolution
      )
      resolution.runnable match {
        case Left(_) => ZIO.succeed(unsubmitted)
        case Right(club) =>
          jobIdOrConflict(runner.submit(kind, Some(club.clubId), params, RunTrigger.Api, effect(club, _)))
            .map(submitted => unsubmitted.copy(jobIdOption = submitted.toOption, error = submitted.left.toOption))
      }
    }

  // A job already running is an answer for the caller, not a failed request, so its message goes in the result.
  private def jobIdOrConflict(submit: RIO[PostgresClient, JobRunId]): RIO[PostgresClient, Either[String, String]] =
    submit.map(id => Right(JobRunId.unwrap(id))).catchSome { case e: ConflictException => ZIO.left(e.getMessage) }
}
