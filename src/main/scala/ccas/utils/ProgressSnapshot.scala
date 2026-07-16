package ccas.utils

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

/** One progress bar's raw, unrendered state — the wire shape streamed by `GET /api/jobs/{id}/progress`.
  *
  * Deliberately semantic (`current` / `total` / `text`), NOT a pre-rendered `████ 57%` string: the server can't render
  * a width-correct bar because it doesn't know the following terminal's column count, so the block-bar + percentage are
  * computed client-side ([[ProgressBar.render]]). `id` is the display-assigned bar id, stable for a bar's lifetime, so a
  * client reconciles successive snapshots (update existing, add new, drop absent) without positional guessing.
  */
final case class BarSnapshot(id: Int, current: Int, total: Int, text: String)

object BarSnapshot {
  given JsonEncoder[BarSnapshot] = DeriveJsonEncoder.gen[BarSnapshot]
  given JsonDecoder[BarSnapshot] = DeriveJsonDecoder.gen[BarSnapshot]
}

/** A full, latest-wins snapshot of every bar currently live for a job (its own app bars plus the shared client's API
  * gauge, merged at the route). Removal is implicit: a bar absent from a newer frame is gone. Wrapped in an object
  * rather than sent as a bare array so the frame can gain fields later without a breaking wire change.
  */
final case class ProgressSnapshot(bars: List[BarSnapshot])

object ProgressSnapshot {
  given JsonEncoder[ProgressSnapshot] = DeriveJsonEncoder.gen[ProgressSnapshot]
  given JsonDecoder[ProgressSnapshot] = DeriveJsonDecoder.gen[ProgressSnapshot]
}
