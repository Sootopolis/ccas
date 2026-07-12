package ccas.server.routes

import java.nio.charset.StandardCharsets

import scala.util.chaining.*

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{NonEmptyChunk, RIO, ZIO}
import zio.http.*
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec}

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
import ccas.utils.{ProgressDisplay, TimeParser}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, ConflictException, ErrorResponse}

object JobRoutes {

  // --- Request types ---

  // Recruitment caps (`JobCaps.MaxTarget` / `JobCaps.MaxTimeLimitMinutes`) are shared with the scheduled-job
  // path (`ScheduleParams`) so both submission routes apply identical bounds.

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
    autoConfirm: Option[Boolean]
  )
  object RecruitmentRequest {
    given JsonCodec[RecruitmentRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class MembershipRequest(clubSlugs: NonEmptyChunk[ClubSlug], trustUsernames: Option[Boolean])
  object MembershipRequest {
    given JsonCodec[MembershipRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class HistoryRequest(
    clubSlugs: NonEmptyChunk[ClubSlug],
    full: Option[Boolean],
    includeFinished: Option[Boolean],
    refresh: Option[Boolean],
    refreshMinHours: Option[Int]
  )
  object HistoryRequest {
    given JsonCodec[HistoryRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class StatsRequest(
    clubSlug: ClubSlug,
    since: Option[String],
    until: Option[String]
  )
  object StatsRequest {
    given JsonCodec[StatsRequest] = DeriveJsonCodec.gen
  }

  // --- Response types ---

  /** Result of submitting a single job (recruitment, matchref). */
  private[ccas] case class JobResult(jobId: Option[String], error: Option[String])
  object JobResult {
    given JsonCodec[JobResult] = DeriveJsonCodec.gen
  }

  /** Result of submitting a club-specific job within a batch (membership, history). */
  private[ccas] case class ClubJobResult(clubSlug: String, jobId: Option[String], error: Option[String])
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

  private[ccas] case class JobStatusResponse(
    id: String,
    kind: String,
    status: String,
    clubId: Option[Long],
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
        clubId = jr.clubId.map(ClubId.unwrap),
        startedAt = jr.startedAt.toString,
        completedAt = jr.completedAt.map(_.toString),
        error = jr.error,
        trigger = jr.trigger.toString
      )
  }

  // --- Routes ---

  def routes: Routes[JobRunner & ChessComClient & PostgresClient, Nothing] = Routes(
    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[RecruitmentRequest](req)
        runner <- ZIO.service[JobRunner]
        result <- Club.selectBySlug(body.clubSlug).flatMap {
          case None => ZIO.succeed(JobResult(None, Some(s"Club not found: ${body.clubSlug}")))
          case Some(club) =>
            val cappedTarget       = body.target.map(_ min JobCaps.MaxTarget)
            val effectiveTimeLimit = body.timeLimitMinutes.map(_ min JobCaps.MaxTimeLimitMinutes)
            val effect = (jobRunId: Option[JobRunId]) =>
              RecruitmentApp.recruit(
                body.clubSlug,
                body.alias.getOrElse("default"),
                target = cappedTarget,
                cumulative = body.cumulative.getOrElse(false),
                sourceClubs = body.sourceClubs.getOrElse(Nil),
                timeLimitMinutes = effectiveTimeLimit,
                explore = body.explore.getOrElse(true),
                trigger = RunTrigger.Api,
                autoConfirm = body.autoConfirm.getOrElse(true),
                jobRunId = jobRunId
              )
            runner.submit(JobKind.Recruitment, Some(club.clubId), Some(body.toJson), RunTrigger.Api, effect)
              .map(id => JobResult(Some(JobRunId.unwrap(id)), None))
              .catchSome { case e: ConflictException =>
                ZIO.succeed(JobResult(None, Some(e.getMessage)))
              }
        }
      } yield jsonResponse(Status.Ok, result)).pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "membership" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[MembershipRequest](req)
        runner <- ZIO.service[JobRunner]
        results <- ZIO.foreach(body.clubSlugs.toChunk.toList)(slug =>
          submitClubJob(
            runner,
            JobKind.Membership,
            slug,
            Some(body.toJson),
            jobRunId =>
              MembershipApp.reconcileAndReport(
                slug,
                body.trustUsernames.getOrElse(true),
                RunTrigger.Api,
                jobRunId
              ).unit
          )
        )
      } yield jsonResponse(Status.Ok, results)).pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "matchref" -> handler {
      (for {
        runner <- ZIO.service[JobRunner]
        result <- runner
          .submit(
            JobKind.MatchRef,
            None,
            None,
            RunTrigger.Api,
            _ => RefApp.populate(forceSkipped = false, upgradeRefs = false).unit
          )
          .map(id => JobResult(Some(JobRunId.unwrap(id)), None))
          .catchSome { case e: ConflictException =>
            ZIO.succeed(JobResult(None, Some(e.getMessage)))
          }
      } yield jsonResponse(Status.Ok, result)).pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "history" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[HistoryRequest](req)
        runner <- ZIO.service[JobRunner]
        effectiveRefresh = body.refreshMinHours.orElse(body.refresh.filter(identity).map(_ => 0))
        results <- ZIO.foreach(body.clubSlugs.toChunk.toList)(slug =>
          submitClubJob(
            runner,
            JobKind.History,
            slug,
            Some(body.toJson),
            jobRunId =>
              HistoryApp.discover(
                slug,
                body.full.getOrElse(false),
                body.includeFinished.getOrElse(false),
                effectiveRefresh,
                RunTrigger.Api,
                jobRunId = jobRunId
              ).unit
          )
        )
      } yield jsonResponse(Status.Ok, results)).pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "stats" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[StatsRequest](req)
        runner <- ZIO.service[JobRunner]
        parsed <- (body.since, body.until) match {
          case (Some(sinceStr), Some(untilStr)) =>
            for {
              since <- TimeParser.parseInstantZIO(sinceStr)
                .mapError(e => BadRequestException(s"Invalid 'since': $e"))
              until <- TimeParser.parseInstantZIO(untilStr)
                .mapError(e => BadRequestException(s"Invalid 'until': $e"))
            } yield Some((since, until))
          case (None, None) => ZIO.none
          case _ => ZIO.fail(BadRequestException("Both 'since' and 'until' are required for period stats"))
        }
        result <- submitClubJob(
          runner,
          JobKind.Stats,
          body.clubSlug,
          Some(body.toJson),
          _ => parsed match {
            case Some((since, until)) =>
              // minGames=1 mirrors the CLI default; StatsRequest carries no min-games field.
              StatsApp.playerOfPeriodAndReport(body.clubSlug, since, until, 1).unit
            case None =>
              StatsApp.memberStatsAndReport(body.clubSlug).unit
          }
        )
      } yield jsonResponse(Status.Ok, result)).pipe(withErrorHandling)
    },
    Method.GET / "api" / "jobs" -> handler {
      (for {
        runner <- ZIO.service[JobRunner]
        jobs   <- runner.recentJobs(50)
      } yield jsonResponse(Status.Ok, jobs.map(JobStatusResponse.fromJobRun)))
        .pipe(withErrorHandling)
    },
    Method.GET / "api" / "jobs" / string("jobId") -> handler { (jobId: String, _: Request) =>
      (for {
        runner <- ZIO.service[JobRunner]
        id = JobRunId.wrap(jobId)
        jobOpt <- runner.status(id)
      } yield jobOpt match {
        case Some(job) => jsonResponse(Status.Ok, JobStatusResponse.fromJobRun(job))
        case None      => jsonResponse(Status.NotFound, ErrorResponse(s"Job $jobId not found"))
      }).pipe(withErrorHandling)
    },
    // Chunked `text/plain` stream of a job's log lines. Stays open while the job runs (lines arrive as emitted) and
    // closes once the job is terminal and the tail reaches EOF, so a client can treat body-close as "job finished".
    Method.GET / "api" / "jobs" / string("jobId") / "logs" -> handler { (jobId: String, _: Request) =>
      (for {
        runner    <- ZIO.service[JobRunner]
        streamOpt <- runner.logStream(JobRunId.wrap(jobId))
      } yield streamOpt match {
        case None => Response.text(s"Job $jobId not found").status(Status.NotFound)
        case Some(lines) =>
          Response(
            status = Status.Ok,
            headers = Headers(Header.ContentType(MediaType.text.`plain`, charset = Some(StandardCharsets.UTF_8))),
            // Interleave a keepalive tick so a >50s silent job phase can't idle the follower's connection shut (#150).
            body = Body.fromCharSequenceStreamChunked(
              JobLogStream.withKeepAlive(lines).map(_ + "\n"),
              StandardCharsets.UTF_8
            )
          )
      }).pipe(withErrorHandling)
    },
    // Invited usernames for the recruitment run linked to a job — the paste-ready payload the CLI fetches once the
    // job is terminal (the `ccas recruit --stdout` auto-confirm path). 404 if the job id has no recruitment run.
    // Scope is THIS run only, deliberately: a `--cumulative` top-up returns just its new invites so the operator
    // doesn't re-paste players already invited earlier today.
    Method.GET / "api" / "jobs" / string("jobId") / "recruitment" / "invited" -> handler { (jobId: String, _: Request) =>
      candidatesByJobResponse(jobId, RecruitmentCandidate.selectInvitedByRun).pipe(withErrorHandling)
    },
    // Still-deferred candidates for a job's recruitment run — shown by interactive `ccas recruit` before the operator
    // confirms (a deferred-confirm run leaves everything Deferred). 404 if the job has no recruitment run.
    Method.GET / "api" / "jobs" / string("jobId") / "recruitment" / "found" -> handler { (jobId: String, _: Request) =>
      candidatesByJobResponse(jobId, RecruitmentCandidate.selectDeferredByRun).pipe(withErrorHandling)
    },
    // Confirm a deferred-confirm run: flip its Deferred candidates to Invited, record the count, return the confirmed
    // usernames. A re-POST finds nothing deferred (flipped = 0) and returns the same already-invited list.
    Method.POST / "api" / "jobs" / string("jobId") / "recruitment" / "confirm" -> handler { (jobId: String, _: Request) =>
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
    },
    // Report the latest recruitment run's invited usernames for a club — `ccas recruit --report`.
    Method.GET / "api" / "recruitment" / "clubs" / string("slug") / "latest" / "invited" -> handler {
      (slug: String, _: Request) =>
        (Club.selectBySlug(ClubSlug.wrap(slug)).flatMap {
          case None       => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"Club not found: $slug")))
          case Some(club) =>
            RecruitmentRun.selectLatest(club.clubId).flatMap {
              case None      => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"No recruitment runs for $slug")))
              case Some(run) => invitedRunResponse(run.runId)
            }
        }).pipe(withErrorHandling)
    },
    // Report a specific recruitment run's invited usernames — `ccas recruit --report --run N`.
    Method.GET / "api" / "recruitment" / "runs" / string("runId") / "invited" -> handler { (runId: String, _: Request) =>
      (runId.toLongOption match {
        case None     => ZIO.succeed(jsonResponse(Status.BadRequest, ErrorResponse(s"Invalid run id: $runId")))
        case Some(id) =>
          val rid = RecruitmentRunId.wrap(id)
          RecruitmentRun.selectId(rid).flatMap {
            case None    => ZIO.succeed(jsonResponse(Status.NotFound, ErrorResponse(s"Run $runId not found")))
            case Some(_) => invitedRunResponse(rid)
          }
      }).pipe(withErrorHandling)
    }
  )

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

  /** Resolve a club slug and submit a single job. Returns a [[ClubJobResult]] with either a job ID or an error. */
  private def submitClubJob(
    runner: JobRunner,
    kind: JobKind,
    slug: ClubSlug,
    params: Option[String],
    effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
  ): RIO[PostgresClient, ClubJobResult] =
    Club.selectBySlug(slug).flatMap {
      case None => ZIO.succeed(ClubJobResult(ClubSlug.unwrap(slug), None, Some("Club not found")))
      case Some(club) =>
        runner.submit(kind, Some(club.clubId), params, RunTrigger.Api, effect)
          .map(id => ClubJobResult(ClubSlug.unwrap(slug), Some(JobRunId.unwrap(id)), None))
          .catchSome { case e: ConflictException =>
            ZIO.succeed(ClubJobResult(ClubSlug.unwrap(slug), None, Some(e.getMessage)))
          }
    }
}
