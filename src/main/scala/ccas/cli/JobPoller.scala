package ccas.cli

import zio.*

import ccas.server.routes.JobRoutes.{ClubJobResult, JobResult, JobStatusResponse}

/** Drives a submitted job to completion by polling `GET /api/jobs/{id}`.
  *
  * `pollInterval` and `maxWait` are injected (no defaults) so tests can run fast; production wires a couple of seconds
  * and a generous wall-clock cap. Exit codes: 0 when the job reaches `Completed`, 1 when it reaches `Failed`, could
  * not be submitted, hit an unexpected status, or did not finish within `maxWait`.
  */
final class JobPoller(api: CcasApiClient, pollInterval: Duration, maxWait: Duration) {

  def pollOne(jobId: String): Task[Int] =
    poll(jobId, "").timeout(maxWait).flatMap {
      case Some(code) => ZIO.succeed(code)
      case None =>
        Console
          .printLineError(s"$jobId: still running after ${maxWait.toMinutes}m — check 'ccas jobs' / 'ccas logs $jobId'")
          .orDie
          .as(1)
    }

  // Only `Running` continues the loop; any other status is terminal. An unexpected value (server adds a new status)
  // exits non-zero rather than spinning forever.
  private def poll(jobId: String, lastStatus: String): Task[Int] =
    api.getJson[JobStatusResponse](s"/api/jobs/$jobId").flatMap { job =>
      val announce = ZIO.whenDiscard(job.status != lastStatus)(Console.printLine(s"$jobId: ${job.status}").orDie)
      job.status match {
        case "Completed" => announce.as(0)
        case "Failed" =>
          Console.printLineError(s"$jobId failed: ${job.error.getOrElse("unknown error")}").orDie.as(1)
        case "Running" => announce *> ZIO.sleep(pollInterval) *> poll(jobId, job.status)
        case other     => Console.printLineError(s"$jobId: unexpected status '$other'").orDie.as(1)
      }
    }

  /** Single-job submit result (recruit, matchref). The POST returns HTTP 200 even on failure, so branch on the body. */
  def handleSingle(label: String, result: JobResult): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"$label: $err").orDie.as(1)
      case (_, Some(id))  => Console.printLine(s"$label submitted: $id").orDie *> pollOne(id)
      case _              => Console.printLineError(s"$label: server returned no job id").orDie.as(1)
    }

  /** Club-scoped single-job submit result (stats). */
  def handleClubSingle(result: ClubJobResult): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"${result.clubSlug}: $err").orDie.as(1)
      case (_, Some(id))  => Console.printLine(s"${result.clubSlug} submitted: $id").orDie *> pollOne(id)
      case _              => Console.printLineError(s"${result.clubSlug}: server returned no job id").orDie.as(1)
    }

  /** Batch submit result (membership, history): poll each club's job, fail overall if any job failed. */
  def handleBatch(results: List[ClubJobResult]): Task[Int] =
    ZIO.foreach(results)(handleClubSingle).map(codes => if (codes.forall(_ == 0)) { 0 } else { 1 })
}
