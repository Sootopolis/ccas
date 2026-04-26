package ccas.analysis.tables.subtypes

import ccas.utils.opaque.PositiveLongCompanion

/** Internal DB-surrogate id types. Kept separate from `ccas.api.misc.subtypes`, which holds Chess.com domain types
  * like `PlayerId` / `ClubId` / `ClubMatchId` — these, by contrast, identify rows in our own PostgreSQL tables.
  *
  * All extend `PositiveLongCompanion` (`> 0L` validator): surrogate keys from `BIGSERIAL` start at 1 and only grow,
  * so zero is not a valid id for an existing row. The domain-id validators in `ccas.api.misc.subtypes` are looser
  * (`>= 0L`); the difference is intentional but the looser bound there has not been pinned to a specific Chess.com
  * id observed at zero.
  */

type ApiResponseBodyId = ApiResponseBodyId.Type

object ApiResponseBodyId extends PositiveLongCompanion

type ApiResponseCacheId = ApiResponseCacheId.Type

object ApiResponseCacheId extends PositiveLongCompanion

type RecruitmentRunId = RecruitmentRunId.Type

object RecruitmentRunId extends PositiveLongCompanion

type HistoryRunId = HistoryRunId.Type

object HistoryRunId extends PositiveLongCompanion

type MembershipRunId = MembershipRunId.Type

object MembershipRunId extends PositiveLongCompanion
