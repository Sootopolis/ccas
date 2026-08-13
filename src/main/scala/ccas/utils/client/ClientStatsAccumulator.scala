package ccas.utils.client

import java.time.Instant

import ccas.analysis.tables.ClientStats

private[ccas] case class ClientStatsAccumulator(
  requests: Long = 0,
  successes: Long = 0,
  failures: Long = 0,
  attempts: Long = 0,
  errors429: Long = 0,
  errorsCf403: Long = 0,
  errorsOther: Long = 0,
  connectionErrors: Long = 0,
  throttleDowns: Long = 0,
  peakConcurrent: Int = 0,
  latencyMinMs: Long = Long.MaxValue,
  latencyMaxMs: Long = 0,
  latencySumMs: Long = 0,
  latencyCount: Long = 0,
  latencyBuckets: Vector[Long] = Vector.fill(ClientStatsAccumulator.LatencyBucketCount)(0L),
  gateWaitMs: Long = 0,
  emaDelayMs: Long = 0,
  activeMs: Long = 0,
  throttledMs: Long = 0,
  attemptsByTier: Map[Int, Long] = Map.empty,
  errors429ByTier: Map[Int, Long] = Map.empty,
  errorsCf403ByTier: Map[Int, Long] = Map.empty,
  cacheHits: Long = 0,
  cacheRevalidations: Long = 0,
  cacheMisses: Long = 0,
  cacheUnserved: Long = 0
) {
  def incRequests: ClientStatsAccumulator           = copy(requests = requests + 1)
  def incSuccesses: ClientStatsAccumulator          = copy(successes = successes + 1)
  def incFailures: ClientStatsAccumulator           = copy(failures = failures + 1)
  def incConnectionErrors: ClientStatsAccumulator   = copy(connectionErrors = connectionErrors + 1)
  def incThrottleDowns: ClientStatsAccumulator      = copy(throttleDowns = throttleDowns + 1)
  def incCacheHit: ClientStatsAccumulator           = copy(cacheHits = cacheHits + 1)
  def incCacheRevalidation: ClientStatsAccumulator  = copy(cacheRevalidations = cacheRevalidations + 1)
  def incCacheMiss: ClientStatsAccumulator          = copy(cacheMisses = cacheMisses + 1)

  /** A cache entry we had already counted as served could not actually be served, forcing a network refetch: the
    * body was pruned, absent, or the store errored / outran its deadline (#211, #215).
    *
    * `cacheHits` is incremented on the metadata lookup, before any body is read, and `cacheRevalidations` on a 304 —
    * both optimistic, because `CacheableResult` only carries a lazy `getValue`. This is the reconciling term, so
    * genuinely-served entries are `cacheHits + cacheRevalidations - cacheUnserved`. Additive and monotonic on
    * purpose: retracting `cacheHits` instead would change the meaning of an already-populated column and would rest
    * on `getValue` being forced exactly once, which nothing enforces.
    */
  def incCacheUnserved: ClientStatsAccumulator     = copy(cacheUnserved = cacheUnserved + 1)

  /** True if this session did anything worth persisting. `requests` covers every network request
    * (cacheMisses and cacheRevalidations both ride along with `incRequests`); `cacheHits` catches the one case
    * where a session did only Fresh max-age hits and never touched the network.
    */
  def hasActivity: Boolean = requests > 0 || cacheHits > 0
  /** Record a Cloudflare challenge 403 at the given permit tier, incrementing both the total and per-tier counters. */
  def incCf403AtTier(tier: Int): ClientStatsAccumulator =
    copy(
      errorsCf403 = errorsCf403 + 1,
      errorsCf403ByTier = errorsCf403ByTier.updated(tier, errorsCf403ByTier.getOrElse(tier, 0L) + 1)
    )
  def updatePeak(n: Int): ClientStatsAccumulator  = copy(peakConcurrent = peakConcurrent.max(n))
  def addGateWait(ms: Long): ClientStatsAccumulator = copy(gateWaitMs = gateWaitMs + ms)
  def addEmaDelay(ms: Long): ClientStatsAccumulator = copy(emaDelayMs = emaDelayMs + ms)
  def addActiveMs(ms: Long): ClientStatsAccumulator = copy(activeMs = activeMs + ms)
  def addThrottled(ms: Long): ClientStatsAccumulator = copy(throttledMs = throttledMs + ms)

  /** Increment the attempts counter AND the per-tier attempts counter for the current permit level. */
  def incAttemptAtTier(tier: Int): ClientStatsAccumulator =
    copy(
      attempts = attempts + 1,
      attemptsByTier = attemptsByTier.updated(tier, attemptsByTier.getOrElse(tier, 0L) + 1)
    )

  /** Record a 429 error at the given permit tier, incrementing both the total and per-tier counters. */
  def incError429AtTier(tier: Int): ClientStatsAccumulator =
    copy(
      errors429 = errors429 + 1,
      errors429ByTier = errors429ByTier.updated(tier, errors429ByTier.getOrElse(tier, 0L) + 1)
    )

  /** Record a non-rate-limit error. 429s go through `incError429AtTier` and Cloudflare 403s through `incCf403`;
    * everything else (404, plain 403, 5xx, etc.) is counted here.
    */
  def incErrorOther: ClientStatsAccumulator =
    copy(errorsOther = errorsOther + 1)

  def recordLatency(ms: Long): ClientStatsAccumulator = {
    val idx = ClientStatsAccumulator.LatencyBuckets.indexWhere(ms < _) match {
      case -1 => ClientStatsAccumulator.LatencyBuckets.length
      case i  => i
    }
    copy(
      latencyMinMs = latencyMinMs.min(ms),
      latencyMaxMs = latencyMaxMs.max(ms),
      latencySumMs = latencySumMs + ms,
      latencyCount = latencyCount + 1,
      latencyBuckets = latencyBuckets.updated(idx, latencyBuckets(idx) + 1)
    )
  }

  def toClientStats(
    sessionId: String,
    appLabel: String,
    startedAt: Instant,
    completedAt: Instant,
    configId: Long,
    inProgressThrottleMs: Long
  ): ClientStats = {
    val minDisplay  = if (latencyMinMs == Long.MaxValue) 0L else latencyMinMs
    val meanLatency = if (latencyCount > 0) latencySumMs / latencyCount else 0L
    ClientStats(
      sessionId = sessionId,
      appLabel = appLabel,
      configId = configId,
      startedAt = startedAt,
      completedAt = completedAt,
      requests = requests,
      successes = successes,
      failures = failures,
      attempts = attempts,
      attemptsByTier = ClientStatsAccumulator.serializeTierMap(attemptsByTier),
      errors429 = errors429,
      errors429ByTier = ClientStatsAccumulator.serializeTierMap(errors429ByTier),
      errorsCf403 = errorsCf403,
      errorsCf403ByTier = ClientStatsAccumulator.serializeTierMap(errorsCf403ByTier),
      errorsOther = errorsOther,
      connectionErrors = connectionErrors,
      throttleDowns = throttleDowns,
      throttledMs = throttledMs + inProgressThrottleMs,
      peakConcurrent = peakConcurrent,
      activeMs = activeMs,
      gateWaitMs = gateWaitMs,
      emaDelayMs = emaDelayMs,
      latencyMinMs = minDisplay,
      latencyMaxMs = latencyMaxMs,
      latencyMeanMs = meanLatency,
      latencyBucket0To50 = latencyBuckets(0),
      latencyBucket50To100 = latencyBuckets(1),
      latencyBucket100To200 = latencyBuckets(2),
      latencyBucket200To500 = latencyBuckets(3),
      latencyBucket500To1000 = latencyBuckets(4),
      latencyBucket1000Plus = latencyBuckets(5),
      cacheHits = cacheHits,
      cacheRevalidations = cacheRevalidations,
      cacheMisses = cacheMisses,
      cacheUnserved = cacheUnserved
    )
  }

  def summary: String = {
    val retries        = attempts - requests
    val meanLatency    = if (latencyCount > 0) latencySumMs / latencyCount else 0L
    val minDisplay     = if (latencyMinMs == Long.MaxValue) 0L else latencyMinMs
    val failedSuffix   = if (failures > 0) s" ($failures failed)" else ""
    val retrySuffix    = if (retries > 0) s", $retries retries" else ""
    val throttleSuffix = if (throttleDowns > 0) s", $throttleDowns throttle-downs" else ""
    val latencySuffix =
      if (latencyCount > 0) s", latency min/mean/max = $minDisplay/$meanLatency/${latencyMaxMs}ms" else ""
    val overheadParts = List(
      if (gateWaitMs > 0) Some(s"gate=${gateWaitMs}ms") else None,
      if (emaDelayMs > 0) Some(s"ema=${emaDelayMs}ms") else None,
      if (throttledMs > 0) Some(s"throttled=${throttledMs}ms") else None
    ).flatten
    val overheadSuffix = if (overheadParts.nonEmpty) s", overhead ${overheadParts.mkString(", ")}" else ""
    val unservedSuffix = if (cacheUnserved > 0) s" / $cacheUnserved unserved" else ""
    val cacheSuffix =
      if (cacheHits > 0 || cacheRevalidations > 0 || cacheMisses > 0)
        s", cache: $cacheHits hits / $cacheRevalidations revalidated / $cacheMisses misses$unservedSuffix"
      else ""
    s"API stats: $requests requests$failedSuffix$retrySuffix$throttleSuffix$latencySuffix$overheadSuffix$cacheSuffix"
  }
}

private[ccas] object ClientStatsAccumulator {
  /** Upper-exclusive boundaries for latency histogram buckets, in milliseconds. A latency < 50 lands in bucket 0, a
    * latency in [50, 100) lands in bucket 1, etc. Values >= 1000 land in the final overflow bucket.
    */
  private val LatencyBuckets: Array[Long] = Array(50, 100, 200, 500, 1000)

  /** Bucket count: one per boundary plus an overflow bucket for values >= the largest boundary. */
  val LatencyBucketCount: Int = LatencyBuckets.length + 1

  /** Serialize a tier-keyed counter map as a sorted pipe-delimited string: `"tier:count|tier:count|..."`. */
  def serializeTierMap(m: Map[Int, Long]): String =
    m.toVector.sortBy(_._1).map((k, v) => s"$k:$v").mkString("|")
}
