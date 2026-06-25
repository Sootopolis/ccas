package ccas.cli

import java.io.EOFException
import java.nio.file.{Files, Path}

import zio.{Console, ExitCode, Task, UIO, ZIO}

import ccas.server.config.{ServerEnvFile, ServerEnvKeys}

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
      warnIfUnknown(k) *> ServerEnvFile
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

  private def printList(map: Map[String, String], showSecrets: Boolean): UIO[ExitCode] = {
    val knownLines = ServerEnvKeys.all.map { k =>
      val display = map.get(k.name).fold("(unset)")(v => if (showSecrets) { v } else { ServerEnvKeys.redact(k.name, v) })
      s"${k.name}=$display"
    }
    val knownNames = ServerEnvKeys.all.map(_.name).toSet
    val otherLines = map.view.filterKeys(!knownNames(_)).toList.sortBy(_._1).map { case (name, v) =>
      s"$name=${if (showSecrets) { v } else { ServerEnvKeys.redact(name, v) }}"
    }
    val output = (knownLines ::: (if (otherLines.nonEmpty) { "" :: "# Other keys set in the file:" :: otherLines } else { Nil }))
      .mkString("\n")
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
