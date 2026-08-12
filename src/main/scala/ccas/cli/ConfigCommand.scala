package ccas.cli

import java.io.EOFException
import java.nio.file.{Files, Path}

import zio.{Console, ExitCode, Task, UIO, ZIO}

import ccas.server.config.{ServerEnvFile, ServerEnvKeys}
import ccas.utils.sql.PostgresClient

/** Handlers for `ccas config` — a purely local command (no server, no DB) that manages the server-bootstrap env file
  * (`~/.config/ccas/ccas.env`, [[XdgPaths.serverEnvFile]]) which [[ccas.server.config.ServerEnvOverlay]] applies at boot.
  * Modeled on [[UseClub]]: each entry point returns a `UIO[ExitCode]` (0 ok / 2 usage error / 1 I/O failure), prints
  * results to stdout and errors to stderr. Secrets (DATABASE_URL, DB_PASSWORD) are redacted by `list`/`show` and in the
  * `set` echo unless `--show-secrets`; `get` of a single explicitly-named key prints the raw value.
  */
object ConfigCommand {

  private def file: Path = XdgPaths.serverEnvFile

  def get(key: String): UIO[ExitCode] = {
    val k = key.trim
    if (k.isEmpty) { blankKeyError }
    else { ServerEnvFile.get(file, k).foldZIO(ioError, printGet(k, _)) }
  }

  def set(key: String, value: String): UIO[ExitCode] = {
    val k = key.trim
    if (k.isEmpty) { blankKeyError }
    // A blank value is useless (the boot overlay skips blanks) and would render as "set" in `list`, so reject it
    // rather than write a value-vs-effect mismatch. `value` is otherwise stored verbatim (no trim) to keep exact bytes.
    else if (value.trim.isEmpty) {
      Console
        .printLineError(s"error: value for $k must not be blank (use 'ccas config unset $k' to remove it)")
        .orDie
        .as(ExitCode(2))
    } else {
      warnIfUnknown(k) *> warnIfUnusableDatabaseUrl(k, value) *> ServerEnvFile
        .set(file, k, value)
        .foldZIO(ioError, _ => Console.printLine(s"set $k = ${ServerEnvKeys.redact(k, value)}").orDie.as(ExitCode.success))
    }
  }

  def unset(key: String): UIO[ExitCode] = {
    val k = key.trim
    if (k.isEmpty) { blankKeyError }
    else { ServerEnvFile.unset(file, k).foldZIO(ioError, printUnset(k, _)) }
  }

  def list(showSecrets: Boolean): UIO[ExitCode] =
    ServerEnvFile.readMap(file).foldZIO(ioError, printList(_, showSecrets))

  def path: UIO[ExitCode] = Console.printLine(file.toString).orDie.as(ExitCode.success)

  def init: UIO[ExitCode] = initProgram.foldZIO(ioError, ZIO.succeed(_))

  // --- single-key handlers ---

  private def printGet(key: String, value: Option[String]): UIO[ExitCode] =
    value match {
      case Some(v) => Console.printLine(v).orDie.as(ExitCode.success)
      case None    => Console.printLineError(s"error: $key is not set").orDie.as(ExitCode(2))
    }

  private def printUnset(key: String, removed: Boolean): UIO[ExitCode] =
    if (removed) { Console.printLine(s"unset $key").orDie.as(ExitCode.success) }
    else { Console.printLine(s"$key was not set").orDie.as(ExitCode.success) }

  private def warnIfUnknown(key: String): UIO[Unit] =
    ZIO.whenDiscard(ServerEnvKeys.byName(key).isEmpty) {
      Console.printLineError(s"warning: '$key' is not a known ccas setting (writing anyway)").orDie
    }

  /** Surface an unusable `DATABASE_URL` here rather than at the next `ccas server up`. Both the JDBC and libpq forms are
    * accepted at boot ([[ccas.utils.sql.PostgresClient.normalizeJdbcUrl]]); anything else only fails once the pool is
    * built, so warn at write time. Writes anyway — same reasoning as `warnIfUnknown`: this is a bootstrap file, and
    * refusing a value the operator meant to stage would be worse than a warning. The message comes from the normaliser
    * and never echoes the URL, so a mistyped password cannot land in the terminal scrollback.
    */
  private def warnIfUnusableDatabaseUrl(key: String, value: String): UIO[Unit] =
    if (key != "DATABASE_URL") { ZIO.unit }
    else {
      PostgresClient
        .normalizeJdbcUrl(value)
        .fold(msg => Console.printLineError(s"warning: $msg (writing anyway)").orDie, _ => ZIO.unit)
    }

  private def printList(map: Map[String, String], showSecrets: Boolean): UIO[ExitCode] = {
    def display(name: String, v: String): String = if (showSecrets) { v } else { ServerEnvKeys.redact(name, v) }
    def line(name: String): String = s"$name=${map.get(name).fold("(unset)")(display(name, _))}"
    // Known keys grouped under a `# <domain>` header (registry order within each domain); domains separated by a blank.
    val knownSections = ServerEnvKeys.grouped.map { case (domain, keys) =>
      (s"# ${domain.label}" :: keys.map(k => line(k.name))).mkString("\n")
    }
    val knownNames = ServerEnvKeys.all.map(_.name).toSet
    val otherLines = map.view.filterKeys(!knownNames(_)).toList.sortBy(_._1).map { case (name, v) => s"$name=${display(name, v)}" }
    val otherSection =
      if (otherLines.nonEmpty) { List(("# Other keys set in the file:" :: otherLines).mkString("\n")) } else { Nil }
    val output = (knownSections ::: otherSection).mkString("\n\n")
    Console.printLine(output).orDie.as(ExitCode.success)
  }

  // --- init wizard ---

  private def initProgram: Task[ExitCode] =
    for {
      exists  <- ZIO.attemptBlocking(Files.exists(file))
      proceed <- if (exists) { confirmOverwrite } else { ZIO.succeed(true) }
      code    <- if (proceed) { runWizard } else { Console.printLine("aborted").as(ExitCode.success) }
    } yield code

  private def runWizard: Task[ExitCode] =
    for {
      email   <- promptRequired("Contact email (CCAS_CONTACT_EMAIL): ")
      dbPairs <- promptDatabase
      portOpt <- promptOptional("Server port [8080] (Enter to keep the default): ")
      pairs = ("CCAS_CONTACT_EMAIL" -> email) :: dbPairs ::: portOpt.map("SERVER_PORT" -> _).toList
      _       <- preview(pairs)
      confirm <- promptConfirm(s"Save to $file? [Y/n]: ")
      code    <- if (confirm) { save(pairs) } else { Console.printLine("aborted").as(ExitCode.success) }
    } yield code

  private def save(pairs: List[(String, String)]): Task[ExitCode] =
    ServerEnvFile.setAll(file, pairs) *> Console.printLine(s"wrote $file").as(ExitCode.success)

  private def promptDatabase: Task[List[(String, String)]] =
    prompt("Database connection: [1] full JDBC URL  [2] separate host/port/name/user/password — choose [1/2]: ").flatMap {
      case "1" | "" => promptDbUrl
      case "2"      => promptDbFields
      case _        => Console.printLine("  enter 1 or 2") *> promptDatabase
    }

  private def promptDbUrl: Task[List[(String, String)]] =
    promptRequired("DATABASE_URL (jdbc:postgresql://host/db?user=…&password=…&sslmode=require): ")
      .map(url => List("DATABASE_URL" -> url))

  private def promptDbFields: Task[List[(String, String)]] =
    for {
      host   <- promptDefault("DB_HOST [localhost]: ", "localhost")
      port   <- promptDefault("DB_PORT [5432]: ", "5432")
      name   <- promptRequired("DB_NAME: ")
      user   <- promptRequired("DB_USER: ")
      _      <- Console.printLine("  note: the password is visible as you type (no echo masking)")
      pass   <- promptRequired("DB_PASSWORD: ")
      schema <- promptOptional("DB_SCHEMA (Enter to skip): ")
    } yield List("DB_HOST" -> host, "DB_PORT" -> port, "DB_NAME" -> name, "DB_USER" -> user, "DB_PASSWORD" -> pass) :::
      schema.map("DB_SCHEMA" -> _).toList

  private def preview(pairs: List[(String, String)]): Task[Unit] =
    Console.printLine("\nWill write:") *>
      ZIO.foreachDiscard(pairs) { case (k, v) => Console.printLine(s"  $k=${ServerEnvKeys.redact(k, v)}") }

  // --- prompt primitives (mirror RecruitmentCriteriaApp) ---

  // On EOF (e.g. `ccas config init </dev/null`) `Console.readLine` fails with EOFException; surface a clear reason
  // instead of a bare "error: null" from the generic handler. `init` is the only caller, and it is interactive-only.
  private def prompt(label: String): Task[String] =
    (Console.print(label) *> Console.readLine)
      .map(_.trim)
      .catchSome { case _: EOFException => ZIO.fail(new RuntimeException("'ccas config init' requires an interactive terminal")) }

  private def promptRequired(label: String): Task[String] =
    prompt(label).flatMap(s => if (s.nonEmpty) { ZIO.succeed(s) } else { Console.printLine("  value is required") *> promptRequired(label) })

  private def promptOptional(label: String): Task[Option[String]] =
    prompt(label).map(s => Option(s).filter(_.nonEmpty))

  private def promptDefault(label: String, default: String): Task[String] =
    prompt(label).map(s => if (s.nonEmpty) { s } else { default })

  private def promptConfirm(label: String): Task[Boolean] =
    prompt(label).map(_.toLowerCase).map(s => s.isEmpty || s == "y" || s == "yes")

  private def confirmOverwrite: Task[Boolean] =
    prompt(s"$file already exists — update it with these values? [y/N]: ").map(_.toLowerCase).map(s => s == "y" || s == "yes")

  // --- shared ---

  private def blankKeyError: UIO[ExitCode] =
    Console.printLineError("error: key must not be blank").orDie.as(ExitCode(2))

  private def ioError(e: Throwable): UIO[ExitCode] =
    Console.printLineError(s"error: ${rootMessage(e)}").orDie.as(ExitCode(1))

  private def rootMessage(e: Throwable): String = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
}
