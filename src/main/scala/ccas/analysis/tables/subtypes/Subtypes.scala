package ccas.analysis.tables.subtypes

import ccas.utils.opaque.LongCompanion

/** Internal DB-surrogate id types. Kept separate from `ccas.api.misc.subtypes`, which holds Chess.com domain types
  * like `PlayerId` / `ClubId` / `ClubMatchId` — these, by contrast, identify rows in our own PostgreSQL tables.
  */

type ApiResponseBodyId = ApiResponseBodyId.Type

object ApiResponseBodyId extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw > 0L, raw, s"$name must be > 0")
}

type ApiResponseCacheId = ApiResponseCacheId.Type

object ApiResponseCacheId extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw > 0L, raw, s"$name must be > 0")
}
