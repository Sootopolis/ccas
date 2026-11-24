package ccas.server.scheduler

import zio.{Task, ZIO}
import zio.json.JsonCodec

/** Reads a schedule's free-text `params` column as its kind's options (`JobOptions`). */
object ScheduleParams {

  /** Decode `params` into `A`, falling back to `default` when absent or blank. A malformed JSON body fails
    * with a descriptive error, which the scheduler's per-schedule guard isolates (the bad row logs and the
    * poll loop continues) — and because decode runs before `submit`, `last_run_at` is not advanced.
    */
  def decode[A](params: Option[String], default: A)(using codec: JsonCodec[A]): Task[A] =
    params match {
      case None                      => ZIO.succeed(default)
      case Some(s) if s.trim.isEmpty => ZIO.succeed(default)
      case Some(s) =>
        ZIO
          .fromEither(codec.decoder.decodeJson(s))
          .mapError(err => new IllegalArgumentException(s"invalid params JSON: $err"))
    }
}
