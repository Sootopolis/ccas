package ccas.analysis.tables.subtypes

import com.github.f4b6a3.ulid.UlidCreator

import ccas.utils.opaque.{PositiveLongCompanion, StringCompanion}

// Ids we mint for rows in our own tables; Chess.com's ids are in `ccas.api.misc.subtypes`, the client's cache ids in
// `ccas.utils.client`. The run ids are `BIGSERIAL` surrogates, so `PositiveLongCompanion` refuses 0, which Chess.com's
// ids allow: deliberate, though no Chess.com id has been seen at 0.

type RecruitmentRunId = RecruitmentRunId.Type

object RecruitmentRunId extends PositiveLongCompanion

type HistoryRunId = HistoryRunId.Type

object HistoryRunId extends PositiveLongCompanion

type MembershipRunId = MembershipRunId.Type

object MembershipRunId extends PositiveLongCompanion

/** A server job's id: a ULID minted when the job is submitted, which the analysis run tables carry back to it. */
type JobRunId = JobRunId.Type

object JobRunId extends StringCompanion {
  def generate(): JobRunId = wrap(UlidCreator.getMonotonicUlid().toString)
}
