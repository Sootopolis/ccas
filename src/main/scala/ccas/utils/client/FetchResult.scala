package ccas.utils.client

import zio.{Task, ZIO}

import ccas.analysis.tables.subtypes.ApiResponseBodyId

/** What a fetch concluded: the resource is absent, or it is present and this is how we obtained it.
  *
  * [[FetchResult.Missing]] is an answer, not a failure — Chess.com reported that nothing lives at this URL, and
  * only the caller knows whether that is acceptable. Malfunctions (transport, 5xx, a 404 carrying an internal-error
  * body, decode) stay in the error channel, where the retry schedules and the failure window act on them.
  *
  * Why absence is a variant rather than an error: `docs/adr/0019-a-reported-404-is-an-answer.md`. Why the other
  * four exist, and why `getValue` is lazy for the three hit variants but eager for [[FetchResult.Changed]]:
  * `docs/adr/0007-response-caching-in-postgres.md`.
  */
sealed trait FetchResult[+T] {
  /** Load and decode the value, failing with the reported 404 when the resource is [[FetchResult.Missing]].
    *
    * Cheap for [[FetchResult.Changed]] (value is already in memory). For [[FetchResult.Fresh]] and
    * [[FetchResult.Revalidated]] this may hit the DB to load the cached body and run the JSON decoder. For
    * [[FetchResult.IdenticalBody]] the body is already in memory but still decoded lazily.
    */
  def getValue: Task[T]

  /** Total branch over the three outcomes. Neither hit branch forces `getValue`, so a caller that only asks "did it
    * change" pays no body-load or decode cost.
    *
    * Dispatched via `final` overrides rather than a pattern match, which avoids an erased
    * `case c: Changed[T @unchecked]` cast and lets the compiler verify each branch end-to-end.
    */
  def foldZIO[R, E >: Throwable, A](
    ifMissing: FetchResult.Missing => ZIO[R, E, A],
    ifUnchanged: FetchResult.Unchanged[T] => ZIO[R, E, A],
    ifChanged: T => ZIO[R, E, A]
  ): ZIO[R, E, A]

  /** [[foldZIO]] for a caller whose answer to absence is "fail": it raises the reported 404, which is what the
    * rename-recovery combinators key on. A decision, not a default — the total fold is there for callers whose
    * answer is anything else.
    */
  final def foldPresentZIO[R, E >: Throwable, A](
    ifUnchanged: FetchResult.Unchanged[T] => ZIO[R, E, A],
    ifChanged: T => ZIO[R, E, A]
  ): ZIO[R, E, A] =
    foldZIO(missing => ZIO.fail(missing.cause), ifUnchanged, ifChanged)
}

object FetchResult {

  /** Chess.com reported no such resource (a [[ReportedNotFound]] body). Carries the exception so a caller that
    * treats absence as failure — anything relying on rename recovery, which keys on the error channel — can raise
    * exactly what it would have caught before.
    */
  final case class Missing(cause: ReportedNotFound) extends FetchResult[Nothing] {
    override val getValue: Task[Nothing] = ZIO.fail(cause)

    override def foldZIO[R, E >: Throwable, A](
      ifMissing: Missing => ZIO[R, E, A],
      ifUnchanged: Unchanged[Nothing] => ZIO[R, E, A],
      ifChanged: Nothing => ZIO[R, E, A]
    ): ZIO[R, E, A] = ifMissing(this)
  }

  /** The cache-hit variants, which differ only in how the layer concluded "unchanged". */
  sealed trait Unchanged[+T] extends FetchResult[T] {
    def bodyId: ApiResponseBodyId

    final override def foldZIO[R, E >: Throwable, A](
      ifMissing: Missing => ZIO[R, E, A],
      ifUnchanged: Unchanged[T] => ZIO[R, E, A],
      ifChanged: T => ZIO[R, E, A]
    ): ZIO[R, E, A] = ifUnchanged(this)
  }

  /** Served from cache without a network call — the cache entry was within `Cache-Control: max-age`. No body was
    * loaded from DB during the initial dispatch; the `getValue` Task performs a `SELECT body FROM api_response_body`
    * on demand and runs the JSON decoder.
    */
  final case class Fresh[T] private[client] (
    bodyId: ApiResponseBodyId,
    getValue: Task[T]
  ) extends Unchanged[T]

  /** Sent a conditional GET (`If-None-Match` / `If-Modified-Since`) and the server returned `304 Not Modified`.
    * The cached row's `fetched_at` is refreshed as a side effect. No body was loaded from DB; `getValue` does it
    * on demand, same as [[Fresh]].
    */
  final case class Revalidated[T] private[client] (
    bodyId: ApiResponseBodyId,
    getValue: Task[T]
  ) extends Unchanged[T]

  /** Server returned `200` but the new body is byte-identical to what we had — `ApiResponseBody.putBody` deduped
    * via its SHA-256 hash and we got back the same `body_id`. The body is already in memory (we received it over
    * the wire) so `getValue` just runs the decoder on that in-memory string — no DB read required.
    */
  final case class IdenticalBody[T] private[client] (
    bodyId: ApiResponseBodyId,
    getValue: Task[T]
  ) extends Unchanged[T]

  /** First fetch, or cache was stale and the server returned a new body. Already decoded at construction time so
    * any decode error surfaces from the fetch path rather than from `getValue`.
    */
  final case class Changed[T] private[client] (value: T) extends FetchResult[T] {
    override val getValue: Task[T] = ZIO.succeed(value)

    override def foldZIO[R, E >: Throwable, A](
      ifMissing: Missing => ZIO[R, E, A],
      ifUnchanged: Unchanged[T] => ZIO[R, E, A],
      ifChanged: T => ZIO[R, E, A]
    ): ZIO[R, E, A] = ifChanged(value)
  }
}
