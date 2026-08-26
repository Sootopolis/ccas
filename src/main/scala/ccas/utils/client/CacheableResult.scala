package ccas.utils.client

import zio.{Task, ZIO}

import ccas.analysis.tables.subtypes.ApiResponseBodyId

/** Result of [[ChessComClient.getCacheable]]: whether a response came from the cache, why, and a [[getValue]] that
  * defers body load and JSON decode until the caller asks.
  *
  * Why four variants rather than one `Unchanged`, and why `getValue` is lazy for the three hit variants but eager
  * for [[Changed]]: `docs/adr/0007-response-caching-in-postgres.md`.
  */
sealed trait CacheableResult[+T] {
  /** `true` if this response is known to be unchanged since the last fetch; the caller may skip downstream
    * processing of the value without loading it.
    */
  def isUnchanged: Boolean

  /** Load and decode the value. Cheap for [[CacheableResult.Changed]] (value is already in memory). For
    * [[CacheableResult.Fresh]] and [[CacheableResult.Revalidated]] this may hit the DB to load the cached body and
    * run the JSON decoder. For [[CacheableResult.IdenticalBody]] the body is already in memory but still decoded
    * lazily.
    */
  def getValue: Task[T]

  /** Symmetric branch on cache-hit-vs-change. Neither branch forces `getValue` on an unchanged variant, so a
    * caller that only asks "did it change" pays no body-load or decode cost.
    *
    * Dispatched via `final` overrides rather than a pattern match, which avoids an erased
    * `case c: Changed[T @unchecked]` cast and lets the compiler verify each branch end-to-end.
    */
  def foldZIO[R, E >: Throwable, A](
    ifUnchanged: CacheableResult.Unchanged[T] => ZIO[R, E, A]
  )(
    ifChanged: T => ZIO[R, E, A]
  ): ZIO[R, E, A]

  /** Discarding shortcut: run `zio` only when the response actually changed. Intended for one-sided effects
    * like "log on change" or "notify on change" where no value is needed.
    */
  def unlessUnchangedDiscard[R, E](zio: => ZIO[R, E, Any]): ZIO[R, E, Unit]
}

object CacheableResult {

  /** The cache-hit variants, which differ only in how the layer concluded "unchanged". */
  sealed trait Unchanged[+T] extends CacheableResult[T] {
    def bodyId: ApiResponseBodyId
    final val isUnchanged: Boolean = true

    final def foldZIO[R, E >: Throwable, A](
      ifUnchanged: Unchanged[T] => ZIO[R, E, A]
    )(
      ifChanged: T => ZIO[R, E, A]
    ): ZIO[R, E, A] = ifUnchanged(this)

    final def unlessUnchangedDiscard[R, E](zio: => ZIO[R, E, Any]): ZIO[R, E, Unit] =
      ZIO.unit
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
  final case class Changed[T] private[client] (value: T) extends CacheableResult[T] {
    val isUnchanged: Boolean = false
    val getValue: Task[T]    = ZIO.succeed(value)

    def foldZIO[R, E >: Throwable, A](
      ifUnchanged: Unchanged[T] => ZIO[R, E, A]
    )(
      ifChanged: T => ZIO[R, E, A]
    ): ZIO[R, E, A] = ifChanged(value)

    def unlessUnchangedDiscard[R, E](zio: => ZIO[R, E, Any]): ZIO[R, E, Unit] =
      zio.unit
  }
}
