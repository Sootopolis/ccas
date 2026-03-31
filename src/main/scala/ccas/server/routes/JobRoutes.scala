package ccas.server.routes

import scala.util.chaining.*

import com.augustnagro.magnum.Transactor
import zio.http.*
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec}
import zio.ZIO

import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.jobs.*
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.ChessComClient

object JobRoutes {

  // --- Request types ---

  private val MaxTarget           = 40 // cap to avoid runaway API usage per recruitment run
  private val MaxTimeLimitMinutes = 30 // keep individual jobs within a reasonable wall-clock window

  private[server] case class RecruitmentRequest(
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

  private[server] case class MembershipRequest(clubSlug: ClubSlug, trustUsernames: Option[Boolean])
  object MembershipRequest {
    given JsonCodec[MembershipRequest] = DeriveJsonCodec.gen
  }

  private[server] case class HistoryRequest(clubSlug: ClubSlug, full: Option[Boolean], refresh: Option[Boolean])
  object HistoryRequest {
    given JsonCodec[HistoryRequest] = DeriveJsonCodec.gen
  }

  // --- Response types ---

  private[server] case class JobResponse(jobId: String, status: String)
  object JobResponse {
    given JsonCodec[JobResponse] = DeriveJsonCodec.gen
  }

  private[server] case class JobStatusResponse(
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

  def routes: Routes[JobRunner & ChessComClient & Transactor, Nothing] = Routes(
    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[RecruitmentRequest](req)
        runner <- ZIO.service[JobRunner]
        club <- Club.selectBySlug(body.clubSlug)
          .someOrFail(new Exception(s"Club not found: ${body.clubSlug}"))
        cappedTarget       = body.target.map(_ min MaxTarget)
        effectiveTimeLimit = body.timeLimitMinutes.map(_ min MaxTimeLimitMinutes)
        effect = (jobRunId: Option[String]) =>
          RecruitmentApp.recruit(
            body.clubSlug,
            body.alias.getOrElse("default"),
            target = cappedTarget,
            cumulative = body.cumulative.getOrElse(false),
            sourceClubs = body.sourceClubs.getOrElse(Nil),
            timeLimitMinutes = effectiveTimeLimit,
            explore = body.explore.getOrElse(true),
            trigger = RunTrigger.Api,
            jobRunId = jobRunId
          )
        paramsStr = body.toJson
        params    = if (paramsStr.length > 1024) paramsStr.take(1024) + "..." else paramsStr
        jobId <- runner.submit(JobKind.Recruitment, Some(club.clubId), Some(params), RunTrigger.Api, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "membership" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[MembershipRequest](req)
        runner <- ZIO.service[JobRunner]
        club <- Club.selectBySlug(body.clubSlug)
          .someOrFail(new Exception(s"Club not found: ${body.clubSlug}"))
        effect = (jobRunId: Option[String]) =>
          MembershipApp.reconcile(
            body.clubSlug,
            body.trustUsernames.getOrElse(true),
            trigger = RunTrigger.Api,
            jobRunId = jobRunId
          )
        jobId <- runner.submit(JobKind.Membership, Some(club.clubId), None, RunTrigger.Api, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "matchref" -> handler {
      (for {
        runner <- ZIO.service[JobRunner]
        jobId <- runner.submit(
          JobKind.MatchRef,
          None,
          None,
          RunTrigger.Api,
          _ => RefApp.populate(RunTrigger.Api, forceSkipped = false, upgradeRefs = false)
        )
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "jobs" / "history" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[HistoryRequest](req)
        runner <- ZIO.service[JobRunner]
        club <- Club.selectBySlug(body.clubSlug)
          .someOrFail(new Exception(s"Club not found: ${body.clubSlug}"))
        effect = (jobRunId: Option[String]) =>
          HistoryApp.discover(
            body.clubSlug,
            body.full.getOrElse(false),
            body.refresh.getOrElse(false),
            RunTrigger.Api,
            jobRunId = jobRunId
          )
        jobId <- runner.submit(JobKind.History, Some(club.clubId), Some(body.toJson), RunTrigger.Api, effect)
      } yield jsonResponse(Status.Accepted, JobResponse(JobRunId.unwrap(jobId), "running")))
        .pipe(withErrorHandling)
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
    }
  )
}
