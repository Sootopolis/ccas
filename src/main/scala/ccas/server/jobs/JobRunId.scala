package ccas.server.jobs

import com.augustnagro.magnum.DbCodec
import com.github.f4b6a3.ulid.UlidCreator
import zio.json.JsonCodec

type JobRunId = JobRunId.Type

object JobRunId {
  opaque type Type = String

  def wrap(s: String): JobRunId    = s
  def unwrap(id: JobRunId): String = id

  def generate(): JobRunId = UlidCreator.getMonotonicUlid().toString

  given DbCodec[JobRunId]   = DbCodec[String].biMap(wrap, unwrap)
  given JsonCodec[JobRunId] = JsonCodec.string.transform(wrap, unwrap)
}
