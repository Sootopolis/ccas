package ccas.cli

import zio.cli.*

/** Parsed `ccas` subcommand. Each variant carries the resolved server URL plus its already-validated arguments;
  * `zio-cli` does all parsing/validation, so the dispatcher only translates these into HTTP calls.
  *
  * The global `--server` option is composed into every subcommand. zio-cli takes bare option names and renders the
  * leading dashes itself (`server` -> `--server`).
  *
  * IMPORTANT — option/argument ordering: zio-cli expects all options BEFORE positional arguments, e.g.
  * `ccas membership --server http://host:port --no-trust-usernames team-alpha team-beta`. Options placed AFTER a
  * positional are NOT errors — they are silently dropped (single positional) or swallowed as positional values
  * (variadic `<slug>...` / `<username>...`). The per-subcommand `--help` shows the required `[options] <args>` order.
  */
sealed trait CliCommand

object CliCommand {

  /** Built-in fallback server URL, used when neither `--server` nor config `api_url` is set. `Main` reads it to seed
    * the resolved default before parsing.
    */
  val DefaultServer = "http://127.0.0.1:8080"

  /** Subcommands that reach a running server as an HTTP client; each carries the resolved base URL. Local commands
    * (`Serve`, `Completion`) are handled in-process by `Main`, don't extend this, and never reach `Dispatcher`.
    */
  sealed trait ServerCommand extends CliCommand {
    def server: String
  }

  // Local commands — handled in `Main`, no client HTTP, no `--server`.
  final case class Serve(detach: Boolean) extends CliCommand
  case object Stop extends CliCommand
  final case class Completion(shell: String) extends CliCommand

  // Server commands — dispatched as HTTP calls by `Dispatcher`.
  final case class Membership(server: String, slugs: List[String], trustUsernames: Option[Boolean])
      extends ServerCommand
  final case class History(
    server: String,
    slugs: List[String],
    full: Boolean,
    includeFinished: Boolean,
    refresh: Boolean,
    refreshMinHours: Option[Int]
  ) extends ServerCommand
  final case class Recruit(
    server: String,
    slug: String,
    alias: Option[String],
    target: Option[Int],
    cumulative: Boolean,
    sourceClubs: List[String],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean]
  ) extends ServerCommand
  final case class Stats(server: String, slug: String, since: Option[String], until: Option[String])
      extends ServerCommand
  final case class Jobs(server: String, limit: Option[Int]) extends ServerCommand
  final case class Logs(server: String, jobId: String) extends ServerCommand
  final case class BlacklistAdd(
    server: String,
    slug: String,
    usernames: List[String],
    reason: Option[String],
    months: Option[Int]
  ) extends ServerCommand
  final case class BlacklistList(server: String, slug: String) extends ServerCommand
  final case class BlacklistRemove(server: String, slug: String, username: String) extends ServerCommand
  final case class ScheduleList(server: String) extends ServerCommand
  final case class ScheduleAdd(
    server: String,
    kind: String,
    intervalHours: Int,
    club: Option[String],
    params: Option[String]
  ) extends ServerCommand
  final case class ScheduleRemove(server: String, id: Long) extends ServerCommand

  // --- Shared options ---

  // Parameterized by the resolved default (config `api_url`, else `DefaultServer`) so an absent `--server` falls back to
  // it — zio-cli then expresses the issue's resolution order natively: explicit `--server` > config > built-in default.
  private def serverOpt(default: String): Options[String] =
    Options.text("server").withDefault(default) ?? s"Base URL of the ccas server (default $default)"

  // Two switches reduced to Option[Boolean]: --no-x wins, --x => Some(true), neither => None (server default).
  private val trustOpt: Options[Option[Boolean]] =
    ((Options.boolean("trust-usernames") ?? "Assume stored usernames are current; skip rename re-verification") ++
      (Options.boolean("no-trust-usernames") ?? "Re-verify every username against the API to catch renames")).map {
      case (_, true) => Some(false)
      case (true, _) => Some(true)
      case _         => None
    }

  private val exploreOpt: Options[Option[Boolean]] =
    ((Options.boolean("explore") ?? "Scout players from clubs beyond the listed source clubs") ++
      (Options.boolean("no-explore") ?? "Restrict scouting to the given source clubs only")).map {
      case (_, true) => Some(false)
      case (true, _) => Some(true)
      case _         => None
    }

  private def intOpt(name: String): Options[Option[Int]] = Options.integer(name).map(_.toInt).optional

  private val slugsArg: Args[List[String]] =
    Args.text("slug").repeat1 ?? "Club slug (URL name); repeatable"

  // --- Leaf commands (each typed Command[CliCommand] so subcommands share a uniform type) ---

  private val serve: Command[CliCommand] =
    Command(
      "serve",
      Options.boolean("detach") ?? "Run the server as a detached background process (writes a pid file; stop with 'ccas stop')"
    ).withHelp("Run the ccas backend HTTP server (foreground by default; --detach to background it)")
      .map(detach => Serve(detach))

  private val stop: Command[CliCommand] =
    Command("stop")
      .withHelp("Stop a detached ccas server (reads the pid file and sends SIGTERM)")
      .map(_ => Stop)

  private def membership(default: String): Command[CliCommand] =
    Command("membership", serverOpt(default) ++ trustOpt, slugsArg)
      .withHelp("Submit a membership-sync job for one or more clubs")
      .map { case ((server, trust), slugs) =>
        Membership(server, slugs, trust)
      }

  private def history(default: String): Command[CliCommand] =
    Command(
      "history",
      serverOpt(default) ++
        (Options.boolean("full") ?? "Clear member-query history and re-crawl all members") ++
        (Options.boolean("include-finished") ?? "Re-queue recently finished matches for refresh") ++
        (Options.boolean("refresh") ?? "Refresh already-stored matches, not just newly seen ones") ++
        (intOpt("refresh-min-hours") ?? "Skip refreshing a match seen within the last N hours"),
      slugsArg
    ).withHelp("Submit a match-history crawl job for one or more clubs")
      .map { case ((server, full, includeFinished, refresh, refreshMinHours), slugs) =>
        History(server, slugs, full, includeFinished, refresh, refreshMinHours)
      }

  private val sourceClubsOpt: Options[List[String]] =
    (Options.text("source-clubs").optional ?? "Comma-separated club slugs to scout for candidates")
      .map(_.fold(List.empty[String])(_.split(",").toList.filter(_.nonEmpty)))

  private def recruit(default: String): Command[CliCommand] =
    Command(
      "recruit",
      serverOpt(default) ++
        (Options.text("alias").optional ?? "Recruitment-criteria alias to use (default: the club's default)") ++
        (intOpt("target") ?? "Stop once N candidates have been found") ++
        (Options.boolean("cumulative") ?? "Count candidates already found earlier today toward the target") ++
        sourceClubsOpt ++
        (intOpt("time-limit-minutes") ?? "Stop scouting after roughly N minutes") ++
        exploreOpt,
      Args.text("slug") ?? "Club slug (URL name) to recruit for"
    ).withHelp("Submit a recruitment scouting job for a club")
      .map { case ((server, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore), slug) =>
        Recruit(server, slug, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore)
      }

  private def stats(default: String): Command[CliCommand] =
    Command(
      "stats",
      serverOpt(default) ++
        (Options.text("since").optional ?? "Start of the date window (ISO-8601 date or instant)") ++
        (Options.text("until").optional ?? "End of the date window (requires --since)"),
      Args.text("slug") ?? "Club slug (URL name)"
    ).withHelp("Submit a club performance-stats job")
      .map { case ((server, since, until), slug) => Stats(server, slug, since, until) }

  private def jobs(default: String): Command[CliCommand] =
    Command("jobs", serverOpt(default) ++ (intOpt("limit") ?? "Maximum number of recent jobs to list"))
      .withHelp("List recent jobs and their status")
      .map { case (server, limit) => Jobs(server, limit) }

  private def logs(default: String): Command[CliCommand] =
    Command("logs", serverOpt(default), Args.text("jobId") ?? "Job run id to poll")
      .withHelp("Poll a job's status and logs until it finishes")
      .map { case (server, jobId) => Logs(server, jobId) }

  private def blacklistAdd(default: String): Command[CliCommand] =
    Command(
      "add",
      serverOpt(default) ++
        (Options.text("reason").optional ?? "Reason recorded with the blacklist entry") ++
        (intOpt("months") ?? "Auto-expire the entry after N months"),
      (Args.text("slug") ?? "Club slug (URL name)") ++ (Args.text("username").repeat1 ?? "Username(s) to blacklist")
    ).withHelp("Blacklist one or more usernames for a club")
      .map { case ((server, reason, months), (slug, usernames)) =>
        BlacklistAdd(server, slug, usernames, reason, months)
      }

  private def blacklistList(default: String): Command[CliCommand] =
    Command("list", serverOpt(default), Args.text("slug") ?? "Club slug (URL name)")
      .withHelp("List a club's blacklist entries")
      .map { case (server, slug) => BlacklistList(server, slug) }

  private def blacklistRemove(default: String): Command[CliCommand] =
    Command(
      "remove",
      serverOpt(default),
      (Args.text("slug") ?? "Club slug (URL name)") ++ (Args.text("username") ?? "Username to remove")
    ).withHelp("Remove a username from a club's blacklist")
      .map { case (server, (slug, username)) =>
        BlacklistRemove(server, slug, username)
      }

  private def blacklist(default: String): Command[CliCommand] =
    Command("blacklist")
      .withHelp("Manage a club's recruitment blacklist")
      .subcommands(blacklistAdd(default), blacklistList(default), blacklistRemove(default))

  private def scheduleList(default: String): Command[CliCommand] =
    Command("list", serverOpt(default))
      .withHelp("List scheduled jobs")
      .map(ScheduleList.apply)

  private def scheduleAdd(default: String): Command[CliCommand] =
    Command(
      "add",
      serverOpt(default) ++
        (Options.text("kind") ?? "Job kind: Recruitment, Membership, MatchRef, History, Stats, or ClubData") ++
        (Options.integer("interval-hours").map(_.toInt) ?? "Run the job every N hours") ++
        (Options.text("club").optional ?? "Club slug the job targets (when the kind needs one)") ++
        (Options.text("params").optional ?? "Extra job parameters passed to the run")
    ).withHelp("Create a scheduled job")
      .map { case (server, kind, intervalHours, club, params) =>
        ScheduleAdd(server, kind, intervalHours, club, params)
      }

  private def scheduleRemove(default: String): Command[CliCommand] =
    Command("remove", serverOpt(default), Args.integer("id").map(_.toLong) ?? "Schedule id to delete")
      .withHelp("Delete a scheduled job by id")
      .map { case (server, id) =>
        ScheduleRemove(server, id)
      }

  private def schedule(default: String): Command[CliCommand] =
    Command("schedule")
      .withHelp("Manage scheduled jobs")
      .subcommands(scheduleList(default), scheduleAdd(default), scheduleRemove(default))

  private val completion: Command[CliCommand] =
    Command("completion", Args.text("shell") ?? "Shell to emit completions for: bash, zsh, or fish")
      .withHelp("Emit a shell completion script (bash, zsh, or fish)")
      .map(Completion.apply)

  /** zio-cli config for the `ccas` command tree. `finalCheckBuiltIn = false` works around a zio-cli 0.8.1 bug: with
    * the default `true`, `ccas <sub> --help` renders the FIRST subcommand's help (`serve`) instead of the named one.
    * `Command.Subcommands.parse` feeds the child `OrElse` a `finalCheckBuiltIn = true` config, so
    * `Command.Single.parse`'s `exhaustiveSearch` claims the `--help` token for the first alternative regardless of the
    * command name, short-circuiting the `OrElse`. Disabling it routes `--help` to the correct subcommand (and to the
    * parent for `ccas <parent> --help`). `Main` and the parser tests both use this so behaviour matches.
    */
  val config: CliConfig = CliConfig.default.copy(finalCheckBuiltIn = false)

  /** Build the `ccas` command tree with `defaultServer` as the `--server` default — `Main` resolves it from config
    * `api_url` (else [[DefaultServer]]) before parsing, so an absent `--server` inherits it.
    */
  def command(defaultServer: String): Command[CliCommand] =
    Command("ccas").subcommands(
      serve,
      stop,
      membership(defaultServer),
      history(defaultServer),
      recruit(defaultServer),
      stats(defaultServer),
      jobs(defaultServer),
      logs(defaultServer),
      blacklist(defaultServer),
      schedule(defaultServer),
      completion
    )
}
