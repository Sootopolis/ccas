package ccas.server.jobs

import zio.RIO

import ccas.analysis.apps.NamedClub
import ccas.api.misc.subtypes.JobRunId
import ccas.utils.ProgressDisplay
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

/** The services every job runs with. */
type JobEnv = ProgressDisplay & ChessComClient & PostgresClient

/** What a submitted job runs, given the id of the `job_run` row tracking it. */
type JobEffect = Option[JobRunId] => RIO[JobEnv, Any]

/** A [[JobEffect]] for a club-scoped job, given the club it was resolved to. */
type ClubJobEffect = (NamedClub, Option[JobRunId]) => RIO[JobEnv, Any]
