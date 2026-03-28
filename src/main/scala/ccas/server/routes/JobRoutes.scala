package ccas.server.routes

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.http.*
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec, JsonDecoder, JsonEncoder}
import zio.ZIO

import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.{BlacklistApp, RecruitmentApp}
import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.{ClubSlug, Username}
import ccas.server.jobs.*
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.{ChessComClient, HttpStatusException}
import ccas.utils.errors.{NotFoundException, UserFacingException}

object JobRoutes {

  // --- Request types ---

  private val MaxTarget           = 40
  private val MaxTimeLimitMinutes = 30

  case class RecruitmentRequest(
    clubSlug: ClubSlug,
    alias: Option[String],
    target: Option[Int],
    cumulative: Option[Boolean],
    sourceClubs: Option[List[ClubSlug]],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean]
  )
  object RecruitmentRequest {
    given JsonCodec[RecruitmentRequest] = DeriveJsonCodec.gen
  }

  case class MembershipRequest(clubSlug: ClubSlug, trustUsernames: Option[Boolean])
  object MembershipRequest {
    given JsonCodec[MembershipRequest] = DeriveJsonCodec.gen
  }

  case class HistoryRequest(clubSlug: ClubSlug, full: Option[Boolean], refresh: Option[Boolean])
  object HistoryRequest {
    given JsonCodec[HistoryRequest] = DeriveJsonCodec.gen
  }

  case class BlacklistRequest(
    clubSlug: ClubSlug,
    username: Username,
    reason: Option[String],
    expiresAt: Option[Instant]
  )
  object BlacklistRequest {
    given JsonCodec[BlacklistRequest] = DeriveJsonCodec.gen
  }

  // --- Response types ---

  case class JobResponse(jobId: String, status: String)
  object JobResponse {
    given JsonCodec[JobResponse] = DeriveJsonCodec.gen
  }

  case class JobStatusResponse(
    id: String,
    kind: String,
    status: String,
    clubSlug: Option[String],
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
        clubSlug = jr.clubSlug.map(ClubSlug.unwrap),
        startedAt = jr.startedAt.toString,
        completedAt = jr.completedAt.map(_.toString),
        error = jr.error,
        trigger = jr.trigger.toString
      )
  }

  // --- Helpers ---

  // ISO-8601 string codecs for the REST API, overriding the global
  // epoch-seconds codecs in ccas.utils.json (which match Chess.com's format).
  private given JsonEncoder[Instant] = JsonEncoder.string.contramap(_.toString)
  private given JsonDecoder[Instant] = JsonDecoder.string.mapOrFail { s =>
    try Right(Instant.parse(s))
    catch { case e: Exception => Left(s"Invalid instant: $s (${e.getMessage})") }
  }

  private def handleJobError(error: Throwable): Response = error match {
    case e: JobConflictException => jsonResponse(Status.Conflict, ErrorResponse(e.getMessage))
    case e: HttpStatusException  => jsonResponse(Status.BadGateway, ErrorResponse(e.getMessage))
    case e: NotFoundException    => jsonResponse(Status.NotFound, ErrorResponse(e.getMessage))
    case e: UserFacingException  => jsonResponse(Status.BadRequest, ErrorResponse(e.getMessage))
    case _                       => jsonResponse(Status.InternalServerError, ErrorResponse("Internal server error"))
  }

  // --- Routes ---

  def routes: Routes[JobRunner & ChessComClient & Transactor, Nothing] = Routes(
    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[RecruitmentRequest](req)
        runner <- ZIO.service[JobRunner]
        cappedTarget       = body.target.map(_ min MaxTarget)
        effectiveTimeLimit = body.timeLimitMinutes.map(_ min MaxTimeLimitMinutes)
        effect = RecruitmentApp.recruit(
          body.clubSlug,
          body.alias.getOrElse("default"),
          target = cappedTarget,
          cumulative = body.cumulative.getOrElse(false),
          sourceClubs = body.sourceClubs.getOrElse(Nil),
          timeLimitMinutes = effectiveTimeLimit,
          explore = body.explore.getOrElse(true),
          trigger = RunTrigger.Api
        )
        paramsStr = body.toJson
        params    = if (paramsStr.length > 1024) paramsStr.take(1024) + "..." else paramsStr
        jobId <- runner.submit(JobKind.Recruitment, Some(body.clubSlug), Some(params), RunTrigger.Api, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },
    Method.POST / "api" / "jobs" / "membership" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[MembershipRequest](req)
        runner <- ZIO.service[JobRunner]
        effect = MembershipApp.reconcile(body.clubSlug, body.trustUsernames.getOrElse(true), trigger = RunTrigger.Api)
        jobId <- runner.submit(JobKind.Membership, Some(body.clubSlug), None, RunTrigger.Api, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },
    Method.POST / "api" / "jobs" / "matchref" -> handler {
      (for {
        runner <- ZIO.service[JobRunner]
        jobId  <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Api, RefApp.populate(RunTrigger.Api))
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },
    Method.POST / "api" / "jobs" / "history" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[HistoryRequest](req)
        runner <- ZIO.service[JobRunner]
        effect = HistoryApp.discover(body.clubSlug, body.full.getOrElse(false), body.refresh.getOrElse(false), RunTrigger.Api)
        jobId <- runner.submit(JobKind.History, Some(body.clubSlug), Some(body.toJson), RunTrigger.Api, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },
    Method.POST / "api" / "jobs" / "blacklist" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[BlacklistRequest](req)
        _    <- BlacklistApp.addToBlacklist(body.clubSlug, body.username, body.reason, body.expiresAt)
      } yield Response.ok)
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },
    Method.GET / "api" / "jobs" -> handler {
      (for {
        runner <- ZIO.service[JobRunner]
        jobs   <- runner.recentJobs(50)
      } yield jsonResponse(Status.Ok, jobs.map(JobStatusResponse.fromJobRun)))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },
    Method.GET / "api" / "jobs" / string("jobId") -> handler { (jobId: String, _: Request) =>
      (for {
        runner <- ZIO.service[JobRunner]
        id = JobRunId.wrap(jobId)
        jobOpt <- runner.status(id)
      } yield jobOpt match {
        case Some(job) => jsonResponse(Status.Ok, JobStatusResponse.fromJobRun(job))
        case None      => jsonResponse(Status.NotFound, ErrorResponse(s"Job $jobId not found"))
      }).catchAll(e => ZIO.succeed(handleJobError(e)))
    }
  )
}
