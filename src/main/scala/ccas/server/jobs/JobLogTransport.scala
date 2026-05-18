package ccas.server.jobs

import zio.stream.ZStream

import ccas.api.misc.subtypes.JobRunId

/** Stream of log lines for a previously submitted job.
  *
  * Interface only at this point — the `FileTail` implementation that reads from `${job-logs.directory}/<jobId>.log`
  * (and closes when the corresponding `JobRun` reaches a terminal status) lands with the `GET /api/jobs/{id}/logs`
  * route in Sootopolis/ccas#47.
  */
trait JobLogTransport {
  def subscribe(jobId: JobRunId): ZStream[Any, Throwable, String]
}
