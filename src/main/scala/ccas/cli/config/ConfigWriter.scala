package ccas.cli.config

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

import zio.{Task, ZIO}

/** Writes (and removes) the single `current_club` key in the CLI config file. zio-config (the read side, [[CliConfig]])
  * is a parser only — it has no HOCON writer — so this does a *surgical* line edit rather than serialising the whole
  * config: it drops any existing top-level `current_club` assignment and, when setting, appends a fresh one, leaving
  * every other key, blank line, and comment untouched. `current_club` is always a simple quoted string, so a flat-key
  * edit is sufficient (the descriptor only reads flat keys anyway).
  */
object ConfigWriter {

  // A top-level `current_club = …` / `current_club: …` assignment. A comment line (`# current_club …`) does not match,
  // so user comments survive. The trailing `.*` also drops any inline comment on the old assignment we're replacing.
  private val CurrentClubAssignment: Regex = """^\s*current_club\s*[=:].*$""".r

  def setCurrentClub(file: Path, slug: String): Task[Unit] =
    rewrite(file, Some(s"""current_club = "${escape(slug)}""""))

  /** Drop the `current_club` assignment, leaving the rest of the config intact. Used by `ccas use-club --clear` and by
    * `ccas club remove` when it unmanages the club the pointer names.
    */
  def clearCurrentClub(file: Path): Task[Unit] = rewrite(file, None)

  // Shared surgical edit: strip any existing assignment, then append `assignment` if there is one.
  private def rewrite(file: Path, assignment: Option[String]): Task[Unit] =
    ZIO.attemptBlocking {
      val dir    = Option(file.getParent).getOrElse(Paths.get("."))
      val exists = Files.exists(file)
      // Clearing a config that was never written is a no-op: don't create an empty file just to remove nothing.
      if (!exists && assignment.isEmpty) { () }
      else {
        Files.createDirectories(dir)
        val existing = if (exists) Files.readAllLines(file).asScala.toList else Nil
        val kept     = existing.filterNot(l => CurrentClubAssignment.matches(l))
        // Drop trailing blank lines so re-runs don't accumulate them before the re-appended key.
        val trimmed = kept.reverse.dropWhile(_.trim.isEmpty).reverse
        val lines   = trimmed ++ assignment
        // An emptied config writes as a genuinely empty file, not a lone newline.
        val out = if (lines.isEmpty) { "" }
        else { lines.mkString("", "\n", "\n") }
        writeAtomically(dir, file, out)
      }
    }

  // Write to a sibling temp file, then rename over the target: same-dir rename is atomic on POSIX, so a crash or a
  // concurrent writer can never leave a half-written or interleaved config — readers see the old or new file, whole.
  // `createTempFile` yields owner-only (0600) perms, which the rename carries onto config.conf — fine, and tighter
  // than a default-umask write for a file that may hold connection settings.
  private def writeAtomically(dir: Path, file: Path, contents: String): Unit = {
    val tmp = Files.createTempFile(dir, "config", ".tmp")
    try {
      Files.writeString(tmp, contents)
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING)
      ()
    } catch {
      case e: Throwable =>
        Files.deleteIfExists(tmp) // don't leak the temp on a write/rename failure
        throw e
    }
  }

  private def escape(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")
}
