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
  * `bodyId` (when one exists), enabling future debug/cross-reference use cases.
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
}

object CacheableResult {

  /** Served from cache without a network call — the cache entry was within `Cache-Control: max-age`. No body was
    * loaded from DB during the initial dispatch; the `getValue` Task performs a `SELECT body FROM api_response_body`
    * on demand and runs the JSON decoder.
    */
  final case class Fresh[T] private[client] (
    bodyId: ApiResponseBodyId,
    getValue: Task[T]
  ) extends CacheableResult[T] {
    val isUnchanged: Boolean = true
  }

  /** Sent a conditional GET (`If-None-Match` / `If-Modified-Since`) and the server returned `304 Not Modified`.
    * The cached row's `fetched_at` is refreshed as a side effect. No body was loaded from DB; `getValue` does it
    * on demand, same as [[Fresh]].
    */
  final case class Revalidated[T] private[client] (
    bodyId: ApiResponseBodyId,
    getValue: Task[T]
  ) extends CacheableResult[T] {
    val isUnchanged: Boolean = true
  }

  /** Server returned `200` but the new body is byte-identical to what we had — `ApiResponseBody.ensureBody` deduped
    * via its SHA-256 hash and we got back the same `body_id`. The body is already in memory (we received it over
    * the wire) so `getValue` just runs the decoder on that in-memory string — no DB read required.
    */
  final case class IdenticalBody[T] private[client] (
    bodyId: ApiResponseBodyId,
    getValue: Task[T]
  ) extends CacheableResult[T] {
    val isUnchanged: Boolean = true
  }

  /** First fetch, or cache was stale and the server returned a new body. Already decoded at construction time so
    * any decode error surfaces from the fetch path rather than from `getValue`.
    */
  final case class Changed[T] private[client] (value: T) extends CacheableResult[T] {
    val isUnchanged: Boolean = false
    val getValue: Task[T]    = ZIO.succeed(value)
  }
}
