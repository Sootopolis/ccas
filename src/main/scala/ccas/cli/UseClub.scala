package ccas.cli

import zio.{Console, ExitCode, UIO, ZIO}

import ccas.cli.config.ConfigWriter

/** `ccas use-club <slug>` — a purely local command that records the current club in the CLI config file (no network). On
  * success it best-effort warns if the slug isn't among the offline completion-cache clubs (a likely typo), but never
  * blocks the write: a brand-new user — or any club CCAS hasn't fetched yet — has nothing to validate against, so a
  * miss is a hint, not an error.
  */
object UseClub {

  def run(slug: String): UIO[ExitCode] = {
    // Trim before storing: a slug never has surrounding whitespace, and `ClubSlug.normalize` only lowercases (no trim),
    // so a padded value would otherwise persist and later reach the API path verbatim.
    val s = slug.trim
    if (s.isEmpty) {
      Console.printLineError("error: club slug must not be blank").orDie.as(ExitCode(2))
    } else {
      ConfigWriter
        .setCurrentClub(XdgPaths.configFile, s)
        .foldZIO(
          e => Console.printLineError(s"error: failed to save current club: ${rootMessage(e)}").orDie.as(ExitCode(1)),
          _ => warnIfUnknown(s) *> Console.printLine(s"current club set to $s").orDie.as(ExitCode.success)
        )
    }
  }

  private def warnIfUnknown(slug: String): UIO[Unit] =
    CompletionCache.readClubs.flatMap { known =>
      ZIO.whenDiscard(known.nonEmpty && !known.contains(slug)) {
        Console.printLineError(s"warning: '$slug' not in known clubs (cache may be stale)").orDie
      }
    }

  private def rootMessage(e: Throwable): String = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
}
