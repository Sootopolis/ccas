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

  /** The club a `@id` names, for a caller that must have one: a URL carrying no slug is a malformed response, the
    * same class of fault as a body that fails to decode, so it throws as [[ClubMatchId.fromUrl]] does.
    */
  def fromUrl(url: URL): ClubSlug = fromUrlOption(url).getOrElse(
    throw new IllegalArgumentException(s"$name.fromUrl($url): no slug in the path")
  )

  /** [[fromUrl]] for a caller comparing against a slug it already has, where a URL naming no club is simply not the
    * one being looked for.
    */
  def fromUrlOption(url: URL): Option[ClubSlug] =
    url.path.segments.lastOption.flatMap(segment => validated(segment).toOption)
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
