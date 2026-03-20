package ccas.server.jobs

import com.github.f4b6a3.ulid.UlidCreator

import ccas.utils.opaque.StringCompanion

type JobRunId = JobRunId.Type

object JobRunId extends StringCompanion {
  def generate(): JobRunId = wrap(UlidCreator.getMonotonicUlid().toString)
}
