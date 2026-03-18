package ccas.server.routes

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.ZIO
import zio.http.*
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec, JsonDecoder, JsonEncoder}

import ccas.analysis.apps.matchref.MatchRefApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.{BlacklistApp, RecruitmentApp}
import ccas.api.misc.subtypes.{ClubUrlName, Username}
import ccas.server.jobs.*
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException

object JobRoutes {

  // --- Request types ---

  private val MaxTarget = 40
  private val MaxTimeLimitMinutes = 30

  case class RecruitmentRequest(
      clubUrlName: ClubUrlName,
      alias: Option[String],
      target: Option[Int],
      cumulative: Option[Boolean],
      sourceClubs: Option[List[ClubUrlName]],
      timeLimitMinutes: Option[Int],
      explore: Option[Boolean]
  )
  object RecruitmentRequest {
    given JsonCodec[RecruitmentRequest] = DeriveJsonCodec.gen
  }

  case class MembershipRequest(clubUrlName: ClubUrlName, trustUsernames: Option[Boolean])
  object MembershipRequest {
    given JsonCodec[MembershipRequest] = DeriveJsonCodec.gen
  }

  case class BlacklistRequest(
      clubUrlName: ClubUrlName,
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
      clubUrlName: Option[String],
      startedAt: String,
      completedAt: Option[String],
      error: Option[String]
  )
  object JobStatusResponse {
    given JsonCodec[JobStatusResponse] = DeriveJsonCodec.gen

    def fromJobRun(jr: JobRun): JobStatusResponse =
      JobStatusResponse(
        id = JobRunId.unwrap(jr.id),
        kind = jr.kind.toString,
        status = jr.status.toString,
        clubUrlName = jr.clubUrlName.map(ClubUrlName.unwrap),
        startedAt = jr.startedAt.toString,
        completedAt = jr.completedAt.map(_.toString),
        error = jr.error
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

  private def handleJobError(error: Throwable): Response = error match
    case e: JobConflictException => jsonResponse(Status.Conflict, ErrorResponse(e.getMessage))
    case e: ExternalException    => jsonResponse(Status.BadRequest, ErrorResponse(e.getMessage))
    case e                       => jsonResponse(Status.InternalServerError, ErrorResponse(e.getMessage))

  // --- Routes ---

  def routes: Routes[JobRunner & ChessComClient & Transactor, Nothing] = Routes(

    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[RecruitmentRequest](req)
        runner <- ZIO.service[JobRunner]
        cappedTarget          = body.target.map(_ min MaxTarget)
        effectiveTimeLimit    = body.timeLimitMinutes.map(_ min MaxTimeLimitMinutes)
        effect  = RecruitmentApp.recruit(
                    body.clubUrlName,
                    body.alias.getOrElse("default"),
                    target = cappedTarget,
                    cumulative = body.cumulative.getOrElse(false),
                    sourceClubs = body.sourceClubs.getOrElse(Nil),
                    timeLimitMinutes = effectiveTimeLimit,
                    explore = body.explore.getOrElse(true)
                  )
        paramsStr = body.toJson
        params    = if (paramsStr.length > 1024) paramsStr.take(1024) + "..." else paramsStr
        jobId  <- runner.submit(JobKind.Recruitment, Some(body.clubUrlName), Some(params), effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },

    Method.POST / "api" / "jobs" / "membership" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[MembershipRequest](req)
        runner <- ZIO.service[JobRunner]
        effect  = MembershipApp.reconcile(body.clubUrlName, body.trustUsernames.getOrElse(true))
        jobId  <- runner.submit(JobKind.Membership, Some(body.clubUrlName), None, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },

    Method.POST / "api" / "jobs" / "matchref" -> handler {
      (for {
        runner <- ZIO.service[JobRunner]
        jobId  <- runner.submit(JobKind.MatchRef, None, None, MatchRefApp.populate)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },

    Method.POST / "api" / "jobs" / "blacklist" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[BlacklistRequest](req)
        _    <- BlacklistApp.addToBlacklist(body.clubUrlName, body.username, body.reason, body.expiresAt)
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
        id      = JobRunId.wrap(jobId)
        jobOpt <- runner.status(id)
      } yield jobOpt match
        case Some(job) => jsonResponse(Status.Ok, JobStatusResponse.fromJobRun(job))
        case None      => jsonResponse(Status.NotFound, ErrorResponse(s"Job $jobId not found"))
      ).catchAll(e => ZIO.succeed(handleJobError(e)))
    }
  )
}
