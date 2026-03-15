package ccas.server.routes

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.ZIO
import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

import ccas.analysis.apps.matchref.MatchRefApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.{BlacklistApp, RecruitmentApp}
import ccas.api.misc.subtypes.{ClubUrlName, Username}
import ccas.server.jobs.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException

object JobRoutes {

  // --- Request types ---

  private val MaxInviteCap = 40
  private val MaxTimeLimitMinutes = 30

  case class RecruitmentRequest(
      clubUrlName: ClubUrlName,
      configName: Option[String],
      inviteCap: Option[Int],
      sourceClubs: Option[List[ClubUrlName]],
      timeLimitMinutes: Option[Int]
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

  case class ErrorResponse(error: String)
  object ErrorResponse {
    given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  }

  // --- Helpers ---

  // ISO-8601 string codecs for the REST API, overriding the global
  // epoch-seconds codecs in ccas.utils.json (which match Chess.com's format).
  private given JsonEncoder[Instant] = JsonEncoder.string.contramap(_.toString)
  private given JsonDecoder[Instant] = JsonDecoder.string.mapOrFail { s =>
    try Right(Instant.parse(s))
    catch { case _: Exception => Left(s"Invalid instant: $s") }
  }

  private def jsonResponse[A: JsonEncoder](status: Status, body: A): Response =
    Response.json(summon[JsonEncoder[A]].encodeJson(body, None).toString).status(status)

  private def handleJobError(error: Throwable): Response = error match
    case e: JobConflictException => jsonResponse(Status.Conflict, ErrorResponse(e.getMessage))
    case e: ExternalException    => jsonResponse(Status.BadRequest, ErrorResponse(e.getMessage))
    case e                       => jsonResponse(Status.InternalServerError, ErrorResponse(e.getMessage))

  // --- Routes ---

  def routes: Routes[JobRunner & ChessComClient & Transactor, Nothing] = Routes(

    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      (for {
        body   <- req.body.asString.flatMap(s => ZIO.fromEither(summon[JsonDecoder[RecruitmentRequest]].decodeJson(s)).mapError(e => new ExternalException(e)))
        runner <- ZIO.service[JobRunner]
        effectiveInviteCap    = body.inviteCap.map(_ min MaxInviteCap).getOrElse(30)
        effectiveTimeLimit    = body.timeLimitMinutes.map(_ min MaxTimeLimitMinutes)
        effect  = RecruitmentApp.recruit(
                    body.clubUrlName,
                    body.configName.getOrElse("default"),
                    effectiveInviteCap,
                    body.sourceClubs.getOrElse(Nil),
                    effectiveTimeLimit
                  )
        jobId  <- runner.submit(JobKind.Recruitment, Some(body.clubUrlName), Some(req.body.toString), effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .catchAll(e => ZIO.succeed(handleJobError(e)))
    },

    Method.POST / "api" / "jobs" / "membership" -> handler { (req: Request) =>
      (for {
        body   <- req.body.asString.flatMap(s => ZIO.fromEither(summon[JsonDecoder[MembershipRequest]].decodeJson(s)).mapError(e => new ExternalException(e)))
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
        body <- req.body.asString.flatMap(s => ZIO.fromEither(summon[JsonDecoder[BlacklistRequest]].decodeJson(s)).mapError(e => new ExternalException(e)))
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
