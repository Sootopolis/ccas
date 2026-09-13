package ccas.api.misc.subtypes

import com.github.f4b6a3.ulid.UlidCreator
import zio.http.URL

import ccas.utils.opaque.{LongCompanion, ShortCompanion, StringCompanion, StringKeyCompanion}

type Elo = Elo.Type

object Elo extends ShortCompanion {
  override protected def validateRaw(raw: Short): Either[String, Short] =
    Either.cond(raw >= 0, raw, s"$name must be >= 0")
}

type PlayerId = PlayerId.Type

object PlayerId extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw >= 0L, raw, s"$name must be >= 0")
}

type Username = Username.Type

object Username extends StringKeyCompanion {
  override protected def normalize(raw: String): String = raw.toLowerCase
  override protected def validateRaw(raw: String): Either[String, String] =
    Either.cond(raw.nonEmpty, raw, s"$name must not be empty")
}

type ClubId = ClubId.Type

object ClubId extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw >= 0L, raw, s"$name must be >= 0")
}

type ClubSlug = ClubSlug.Type

object ClubSlug extends StringKeyCompanion {
  override protected def normalize(raw: String): String = raw.toLowerCase
  override protected def validateRaw(raw: String): Either[String, String] =
    Either.cond(raw.nonEmpty, raw, s"$name must not be empty")

  /** Unlike [[ClubMatchId.fromUrl]], doesn't throw: a team/club `@id` reaches this from parsed API responses whose
    * shape isn't guaranteed the way a URL we constructed ourselves is, so a malformed path is "no slug", not a bug.
    */
  def fromUrlOption(url: URL): Option[ClubSlug] = url.path.segments.lastOption.map(wrap)
}

type ClubMatchId = ClubMatchId.Type

object ClubMatchId extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw >= 0L, raw, s"$name must be >= 0")

  def fromUrl(url: URL): ClubMatchId = validated(url.path.segments.last.toLong).fold(
    msg => throw new IllegalArgumentException(s"$name.fromUrl($url): $msg"),
    identity
  )
}

type ClubAlias = ClubAlias.Type

object ClubAlias extends StringCompanion {
  override protected def validateRaw(raw: String): Either[String, String] =
    Either.cond(raw.nonEmpty, raw, s"$name must not be empty")
}

type TournamentSlug = TournamentSlug.Type

object TournamentSlug extends StringCompanion {
  override protected def normalize(raw: String): String = raw.toLowerCase
  override protected def validateRaw(raw: String): Either[String, String] =
    Either.cond(raw.nonEmpty, raw, s"$name must not be empty")

  def fromUrl(url: URL): TournamentSlug = validated(url.path.segments.last).fold(
    msg => throw new IllegalArgumentException(s"$name.fromUrl($url): $msg"),
    identity
  )
}

type JobRunId = JobRunId.Type

object JobRunId extends StringCompanion {
  def generate(): JobRunId = wrap(UlidCreator.getMonotonicUlid().toString)
}
