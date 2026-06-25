package ccas.server.config

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import scala.jdk.CollectionConverters.*

import zio.{Task, ZIO}

/** Reads and writes the server-bootstrap env file (`KEY=VALUE`, `#` comments) that `ccas config` manages and
  * [[ServerEnvOverlay]] applies at boot. Distinct from `ccas.cli.config.CliConfig` (the HOCON CLI *client* config): this
  * file holds the settings the *server* needs to boot (DB connection, contact email, port), in plain env-var form so the
  * same file doubles as a systemd `EnvironmentFile` and a shell-`source`-able file. Lives in `ccas.server.config` (not
  * `ccas.cli`) so `CcasServer` can apply it at boot without a server→cli package cycle.
  *
  * The file is modelled as an ordered list of [[Line]]s so a `set`/`unset` of one key leaves every other key, blank
  * line, comment, and even line ordering untouched — only the touched key's line is rewritten. A line with no `=`, or a
  * comment/blank, is preserved verbatim as [[Line.Other]] (a malformed line never crashes parsing). Values are split on
  * the FIRST `=` so JDBC URLs keep their `&`, `?`, `=`, `:` intact, and an optional surrounding `"`/`'` pair is stripped
  * on read (and re-added on write only when the value needs it).
  *
  * Writes go through [[writeAtomic]], which copies `ConfigWriter`'s temp-file-then-rename strategy: the temp file is
  * created owner-only (0600) and that mode rides the atomic rename onto the target, so secrets (DATABASE_URL,
  * DB_PASSWORD) are never world-readable.
  */
object ServerEnvFile {

  /** One parsed line. `Pair.raw` is the exact text to emit on render (original text for untouched lines; a freshly
    * rendered `KEY=VALUE` for ones `set` produced), while `Pair.value` is the parsed (unquoted) value used by reads.
    */
  sealed trait Line
  object Line {
    final case class Pair(key: String, value: String, raw: String) extends Line
    final case class Other(raw: String) extends Line
  }

  /** Parse `content` into ordered lines. Never fails: a line without `=` becomes [[Line.Other]]. */
  def parseLines(content: String): List[Line] =
    // `split("\n", -1)` keeps trailing empties; we drop a single trailing "" from the final newline so render's own
    // trailing "\n" doesn't accumulate blank lines across read/write cycles.
    dropFinalEmpty(content.split("\n", -1).toList).map(parseLine)

  private def dropFinalEmpty(lines: List[String]): List[String] =
    lines match {
      case init :+ "" => init
      case other      => other
    }

  private def parseLine(line: String): Line = {
    val trimmed = line.trim
    if (trimmed.isEmpty || trimmed.startsWith("#")) { Line.Other(line) }
    else {
      val idx = line.indexOf('=')
      if (idx < 0) { Line.Other(line) }
      else {
        // Strip a leading `export ` so a hand-`source`-able file (`export DB_HOST=…`) keys on `DB_HOST`, not
        // `export DB_HOST`. Env var names never contain a space, so the prefix is unambiguous. `raw` keeps the
        // original text, so an untouched `export …` line is rendered verbatim.
        val rawKey = line.substring(0, idx).trim
        val key    = if (rawKey.startsWith("export ")) { rawKey.drop("export ".length).trim } else { rawKey }
        if (key.isEmpty) { Line.Other(line) }
        else { Line.Pair(key, unquote(line.substring(idx + 1).trim), line) }
      }
    }
  }

  /** Effective key -> value map, last assignment winning on a duplicated key. */
  def toMap(lines: List[Line]): Map[String, String] =
    lines.foldLeft(Map.empty[String, String]) {
      case (acc, Line.Pair(k, v, _)) => acc.updated(k, v)
      case (acc, _: Line.Other)      => acc
    }

  /** Replace the first `Pair` for `key` in place (dropping any later duplicates), or append a new line. */
  def setValue(lines: List[Line], key: String, value: String): List[Line] = {
    val rendered          = Line.Pair(key, value, renderPair(key, value))
    val isKey: Line => Boolean = { case Line.Pair(k, _, _) => k == key; case _ => false }
    if (lines.exists(isKey)) {
      // Replace the first occurrence in place; drop any later duplicates so exactly one assignment of `key` remains.
      val (before, rest) = lines.span(l => !isKey(l))
      before ::: rendered :: rest.tail.filterNot(isKey)
    } else {
      // Append after trimming trailing blank lines, mirroring ConfigWriter so re-runs don't accrue blanks.
      val kept = lines.reverse.dropWhile(isBlankOther).reverse
      kept :+ rendered
    }
  }

  /** Drop every `Pair` for `key`; returns the new lines and whether anything was removed. */
  def unsetValue(lines: List[Line], key: String): (List[Line], Boolean) = {
    val kept = lines.filterNot { case Line.Pair(k, _, _) => k == key; case _ => false }
    (kept, kept.length != lines.length)
  }

  def render(lines: List[Line]): String =
    if (lines.isEmpty) { "" }
    else {
      lines.map {
        case Line.Pair(_, _, raw) => raw
        case Line.Other(raw)      => raw
      }.mkString("", "\n", "\n")
    }

  // --- effectful file ops ---

  /** Read and parse the file; an absent file yields `Nil` (never fails for want of a file). */
  def read(file: Path): Task[List[Line]] =
    ZIO.attemptBlocking {
      if (Files.exists(file)) { parseLines(Files.readAllLines(file).asScala.toList.mkString("\n")) }
      else { Nil }
    }

  def readMap(file: Path): Task[Map[String, String]] = read(file).map(toMap)

  def get(file: Path, key: String): Task[Option[String]] = readMap(file).map(_.get(key))

  def set(file: Path, key: String, value: String): Task[Unit] =
    read(file).flatMap(lines => writeAtomic(file, render(setValue(lines, key, value))))

  /** Returns true if the key was present (and is now removed), false if it was already absent. */
  def unset(file: Path, key: String): Task[Boolean] =
    read(file).flatMap { lines =>
      val (kept, removed) = unsetValue(lines, key)
      ZIO.when(removed)(writeAtomic(file, render(kept))).as(removed)
    }

  /** Merge `pairs` into the file in one atomic write (used by the `init` wizard so it doesn't write N times). */
  def setAll(file: Path, pairs: List[(String, String)]): Task[Unit] =
    read(file).flatMap { lines =>
      val merged = pairs.foldLeft(lines) { case (acc, (k, v)) => setValue(acc, k, v) }
      writeAtomic(file, render(merged))
    }

  // Same atomic 0600 write as ConfigWriter: write a sibling temp (owner-only perms), then rename over the target —
  // a same-dir rename is atomic on POSIX, so a crash or concurrent writer never leaves a half-written/interleaved file,
  // and the 0600 mode carries onto the env file (which may hold connection secrets).
  private def writeAtomic(file: Path, content: String): Task[Unit] =
    ZIO.attemptBlocking {
      val dir = Option(file.getParent).getOrElse(Paths.get("."))
      Files.createDirectories(dir)
      val tmp = Files.createTempFile(dir, "ccas", ".env.tmp")
      try {
        Files.writeString(tmp, content)
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING)
      } catch {
        case e: Throwable =>
          Files.deleteIfExists(tmp)
          throw e
      }
      ()
    }

  private def isBlankOther(line: Line): Boolean =
    line match {
      case Line.Other(raw) => raw.trim.isEmpty
      case _               => false
    }

  private def renderPair(key: String, value: String): String = s"$key=${quoteIfNeeded(value)}"

  // Our loader (Files.readAllLines) needs no shell escaping, so a bare JDBC URL is written unquoted (&/? are literal in
  // both our parser and a systemd EnvironmentFile). Quote only when the value has whitespace / a `#` / is empty, which
  // would otherwise be reparsed wrongly; inside double quotes escape `\` and `"`.
  private def quoteIfNeeded(v: String): String =
    if (v.isEmpty || v.exists(c => c.isWhitespace || c == '#')) {
      "\"" + v.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    } else { v }

  // Strip a matching surrounding quote pair. Double quotes unescape `\"`/`\\`; single quotes are literal (shell-like).
  private def unquote(raw: String): String =
    if (raw.length >= 2 && raw.startsWith("\"") && raw.endsWith("\"")) {
      raw.substring(1, raw.length - 1).replace("\\\"", "\"").replace("\\\\", "\\")
    } else if (raw.length >= 2 && raw.startsWith("'") && raw.endsWith("'")) {
      raw.substring(1, raw.length - 1)
    } else { raw }
}
