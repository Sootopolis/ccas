package ccas.utils.client

import zio.{Task, ZIO}

import ccas.analysis.tables.subtypes.ApiResponseBodyId

/** Result type returned by [[ChessComClient.getCacheable]]. Lets callers distinguish whether a response came from
  * the cache (and why) vs. the network, and defers body loading + JSON decoding until the caller actually asks for
  * the value via [[getValue]].
  *
  * '''Why four variants rather than one `Unchanged`''': observability. A log or metric that pattern-matches on the
  * variant can distinguish "we never asked the server" (`Fresh`) from "the server confirmed unchanged via 304"
  * (`Revalidated`) from "we asked and got a byte-identical reply" (`IdenticalBody`). Each variant also carries the
  * `bodyId` (when one exists), enabling future debug/cross-reference use cases. The three hit variants share a
  * common [[CacheableResult.Unchanged]] supertype so callers can branch on "any cache hit" uniformly while still
  * retaining the option to distinguish by variant.
  *
  * '''Why `getValue` is lazy''': callers that only branch on [[isUnchanged]] to decide whether to re-process pay
  * zero cost beyond the cache-row lookup. The body is loaded from `api_response_body` and decoded only if and when
  * `getValue` is invoked. For [[Changed]] the value is eager so that decode errors on network responses surface at
  * fetch time rather than being deferred into the caller.
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

  /** Symmetric branch on cache-hit-vs-change. The unchanged branch receives the specific
    * [[CacheableResult.Unchanged]] variant (for logging / `bodyId` access); the changed branch receives the
    * already-decoded value. Neither branch forces `getValue` on an unchanged variant, so the laziness is
    * preserved: callers that only care about "did it change" pay no body-load or decode cost on the skip path.
    *
    * Dispatched via `final` overrides on [[CacheableResult.Unchanged]] and [[CacheableResult.Changed]] rather than
    * a pattern match — avoids an erased-type `case c: Changed[T @unchecked]` cast and lets the compiler verify
    * each branch's typing end-to-end.
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

  /** Common supertype of the three cache-hit variants (`Fresh`, `Revalidated`, `IdenticalBody`). All three carry a
    * `bodyId` and a lazy `getValue`; differ only in how the cache layer arrived at the "unchanged" conclusion.
    */
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

  /** Server returned `200` but the new body is byte-identical to what we had — `ApiResponseBody.ensureBody` deduped
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
