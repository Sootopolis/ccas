package ccas.server.routes

import scala.util.chaining.*

import ccas.utils.sql.PostgresClient
import zio.{NonEmptyChunk, RIO, ZIO}
import zio.http.*
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec}

import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.ref.RefApp
import ccas.analysis.apps.stats.StatsApp
import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.jobs.*
import ccas.server.routes.RouteHelpers.*
import ccas.utils.{ProgressDisplay, TimeParser}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.{BadRequestException, ConflictException, ErrorResponse}

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

  private[server] case class MembershipRequest(clubSlugs: NonEmptyChunk[ClubSlug], trustUsernames: Option[Boolean])
  object MembershipRequest {
    given JsonCodec[MembershipRequest] = DeriveJsonCodec.gen
  }

  private[server] case class HistoryRequest(
    clubSlugs: NonEmptyChunk[ClubSlug],
    full: Option[Boolean],
    refresh: Option[Boolean],
    refreshMinHours: Option[Int]
  )
  object HistoryRequest {
    given JsonCodec[HistoryRequest] = DeriveJsonCodec.gen
  }

  private[server] case class StatsRequest(
    clubSlug: ClubSlug,
    since: Option[String],
    until: Option[String]
  )
  object StatsRequest {
    given JsonCodec[StatsRequest] = DeriveJsonCodec.gen
  }

  // --- Response types ---

  /** Result of submitting a single job (recruitment, matchref). */
  private[server] case class JobResult(jobId: Option[String], error: Option[String])
  object JobResult {
    given JsonCodec[JobResult] = DeriveJsonCodec.gen
  }

  /** Result of submitting a club-specific job within a batch (membership, history). */
  private[server] case class ClubJobResult(clubSlug: String, jobId: Option[String], error: Option[String])
  object ClubJobResult {
    given JsonCodec[ClubJobResult] = DeriveJsonCodec.gen
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

  def routes: Routes[JobRunner & ChessComClient & PostgresClient, Nothing] = Routes(
    Method.POST / "api" / "jobs" / "recruitment" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[RecruitmentRequest](req)
        runner <- ZIO.service[JobRunner]
        result <- Club.selectBySlug(body.clubSlug).flatMap {
          case None => ZIO.succeed(JobResult(None, Some(s"Club not found: ${body.clubSlug}")))
          case Some(club) =>
            val cappedTarget       = body.target.map(_ min MaxTarget)
            val effectiveTimeLimit = body.timeLimitMinutes.map(_ min MaxTimeLimitMinutes)
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
              MembershipApp.reconcile(
                slug,
                body.trustUsernames.getOrElse(true),
                trigger = RunTrigger.Api,
                jobRunId = jobRunId
              )
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
              StatsApp.playerOfPeriod(body.clubSlug, since, until).unit
            case None =>
              StatsApp.memberStats(body.clubSlug).unit
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
    }
  )

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
