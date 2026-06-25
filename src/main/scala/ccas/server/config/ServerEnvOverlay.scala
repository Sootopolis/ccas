package ccas.server.config

import java.nio.file.Path

import com.typesafe.config.ConfigFactory
import zio.{UIO, ZIO}

/** Applies the [[ServerEnvFile]] to the JVM at boot so the server can read its settings from `ccas.env` instead of
  * hand-exported env vars. For each `KEY=VALUE`, sets a JVM system property `KEY` — but ONLY when the process env var
  * `KEY` is unset/blank and no system property `KEY` already exists. Because Typesafe Config resolves `${?KEY}`
  * substitutions against system properties first and the process environment as a fallback, this yields the precedence
  * **process env > ccas.env file > HOCON compiled default** without touching `application.conf` or any read-site: a real
  * env var, when present, is left untouched and wins; otherwise the file value fills in over the compiled default.
  *
  * `ConfigFactory.invalidateCaches()` is called (only if at least one property was set) so the system-properties layer
  * is re-read by the next `ConfigFactory.load()`. MUST run before the first `ConfigFactory.load()` in the JVM — wired
  * into both `Main` (the `serve`/`serve --detach` paths) and `CcasServer.run` (the standalone `ccas-server` binary). It
  * never fails the boot: a missing or unreadable file logs a warning and applies nothing. Idempotent (the only-if-absent
  * guard makes a second call a no-op).
  */
object ServerEnvOverlay {

  /** Apply `file` and return the keys whose values were promoted to system properties (for an optional debug log). */
  def apply(file: Path): UIO[List[String]] =
    ServerEnvFile
      .readMap(file)
      .foldZIO(
        err => ZIO.logWarning(s"could not read server config $file: ${rootMessage(err)}").as(Nil),
        applyMap
      )

  // `ZIO.attempt(...).orElseSucceed(Nil)`: a `setProperty` SecurityException must not become an uncaught defect that
  // crashes the boot — the overlay is best-effort, so swallow it and apply nothing.
  private def applyMap(map: Map[String, String]): UIO[List[String]] =
    ZIO.attempt {
      val applied = map.toList.flatMap { case (key, value) =>
        val envUnset  = Option(System.getenv(key)).forall(_.trim.isEmpty)
        val propUnset = Option(System.getProperty(key)).isEmpty
        if (envUnset && propUnset && value.trim.nonEmpty) {
          System.setProperty(key, value)
          Some(key)
        } else { None }
      }
      if (applied.nonEmpty) { ConfigFactory.invalidateCaches() }
      applied
    }.orElseSucceed(Nil)

  private def rootMessage(e: Throwable): String = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
}
