package ccas.api.misc.subtypes

import com.github.f4b6a3.ulid.UlidCreator
import zio.http.URL

import ccas.utils.opaque.{IntCompanion, LongCompanion, StringCompanion, StringKeyCompanion}

type Elo = Elo.Type

object Elo extends IntCompanion {
  override protected def validateRaw(raw: Int): Either[String, Int] =
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
}

type ClubMatchId = ClubMatchId.Type

object ClubMatchId extends LongCompanion {
  override protected def validateRaw(raw: Long): Either[String, Long] =
    Either.cond(raw >= 0L, raw, s"$name must be >= 0")

  def fromUrl(url: URL): ClubMatchId = wrap(url.path.segments.last.toLong)
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

  def fromUrl(url: URL): TournamentSlug = wrap(url.path.segments.last)
}

type JobRunId = JobRunId.Type

object JobRunId extends StringCompanion {
  def generate(): JobRunId = wrap(UlidCreator.getMonotonicUlid().toString)
}
