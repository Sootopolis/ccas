package ccas.server.config

import java.nio.file.Path

import com.typesafe.config.ConfigFactory
import zio.{IO, UIO, ZIO}

import ccas.utils.errors.safeMessageNaming

/** Applies the [[ServerEnvFile]] to the JVM at boot so the server can read its settings from `ccas.env` instead of
  * hand-exported env vars. For each `KEY=VALUE`, sets a JVM system property `KEY` — but ONLY when the process env var
  * `KEY` is unset/blank and no system property `KEY` already exists. Because Typesafe Config resolves `${?KEY}`
  * substitutions against system properties first and the process environment as a fallback, this yields the precedence
  * **process env > ccas.env file > HOCON compiled default** without touching `application.conf` or any read-site: a real
  * env var, when present, is left untouched and wins; otherwise the file value fills in over the compiled default.
  *
  * `ConfigFactory.invalidateCaches()` is called (only if at least one property was set) so the system-properties layer
  * is re-read by the next `ConfigFactory.load()`. MUST run before the first `ConfigFactory.load()` in the JVM — applied
  * once per process, by `Main` (every `server` subcommand) or by `CcasServer.run` (the standalone `ccas-server`).
  * Idempotent (the only-if-absent guard makes a second call a no-op).
  */
object ServerEnvOverlay {

  /** Apply `file` and return the keys whose values were promoted to system properties. Fails with a ready-to-print
    * reason when the file exists but can't be read, having applied nothing; a missing file applies nothing and succeeds.
    * The caller reports the reason, since the server logs it and a CLI command prints it.
    */
  def apply(file: Path): IO[String, List[String]] =
    ServerEnvFile
      .readMap(file)
      .mapError(err => s"could not read server config ${err.safeMessageNaming(file)}")
      .flatMap(applyMap)

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
}
