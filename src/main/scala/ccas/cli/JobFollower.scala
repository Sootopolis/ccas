package ccas.cli

import zio.*

import ccas.server.routes.JobRoutes.{ClubJobResult, JobResult, JobStatusResponse}

/** Drives a submitted job to completion by **following its log stream** (`GET /api/jobs/{id}/logs`), printing each
  * line as it arrives. The server holds the response open until the job is terminal and the tail reaches EOF, so the
  * stream closing means "job finished" — at which point one `GET /api/jobs/{id}` resolves the exit code.
  *
  * `maxWait` is injected (no default) so tests run fast; production wires a generous wall-clock cap. Exit codes: 0 when
  * the job reaches `Completed`, 1 when it reaches `Failed`, could not be submitted, or did not finish within `maxWait`.
  */
final class JobFollower(api: CcasApiClient, maxWait: Duration) {

  def followJob(jobId: String): Task[Int] =
    api
      .streamLines(s"/api/jobs/$jobId/logs")(line => Console.printLine(line).orDie)
      .timeout(maxWait)
      .flatMap {
        case Some(_) => finalExitCode(jobId)
        case None =>
          Console
            .printLineError(s"$jobId: still running after ${maxWait.toMinutes}m — check 'ccas jobs' / 'ccas logs $jobId'")
            .orDie
            .as(1)
      }
      // A mid-follow stream drop (e.g. a long silent phase idling the connection shut) doesn't stop the detached job —
      // it keeps running server-side. Report honestly and point the user back at it rather than failing loudly (#150).
      .catchSome { case StreamDropped(cause) =>
        Console
          .printLineError(
            s"$jobId: lost the log stream ($cause). The job keeps running on the server — " +
              s"reattach with 'ccas logs $jobId' or check 'ccas jobs'."
          )
          .orDie
          .as(1)
      }

  // The stream closing tells us the job finished but not whether it succeeded; read the terminal status once to decide.
  // An unexpected status (server adds a new terminal kind, or a stale Running) exits non-zero rather than passing.
  private def finalExitCode(jobId: String): Task[Int] =
    api.getJson[JobStatusResponse](s"/api/jobs/$jobId").flatMap { job =>
      job.status match {
        case "Completed" => ZIO.succeed(0)
        case "Failed"    => Console.printLineError(s"$jobId failed: ${job.error.getOrElse("unknown error")}").orDie.as(1)
        case other       => Console.printLineError(s"$jobId: unexpected status '$other'").orDie.as(1)
      }
    }

  /** Single-job submit result (recruit, matchref). The POST returns HTTP 200 even on failure, so branch on the body. */
  def handleSingle(label: String, result: JobResult): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"$label: $err").orDie.as(1)
      case (_, Some(id)) =>
        CompletionCache.appendJob(id) *> Console.printLine(s"$label submitted: $id").orDie *> followJob(id)
      case _ => Console.printLineError(s"$label: server returned no job id").orDie.as(1)
    }

  /** Club-scoped single-job submit result (stats). */
  def handleClubSingle(result: ClubJobResult): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"${result.clubSlug}: $err").orDie.as(1)
      case (_, Some(id)) =>
        CompletionCache.appendJob(id) *> Console.printLine(s"${result.clubSlug} submitted: $id").orDie *> followJob(id)
      case _ => Console.printLineError(s"${result.clubSlug}: server returned no job id").orDie.as(1)
    }

  /** Batch submit result (membership, history): follow each club's job in turn, fail overall if any job failed. */
  def handleBatch(results: List[ClubJobResult]): Task[Int] =
    ZIO.foreach(results)(handleClubSingle).map(codes => if (codes.forall(_ == 0)) { 0 } else { 1 })
}
