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
sealed trait CliCommand {
  def server: String
}

object CliCommand {

  private val DefaultServer = "http://127.0.0.1:8080"

  final case class Serve(server: String) extends CliCommand
  final case class Membership(server: String, slugs: List[String], trustUsernames: Option[Boolean]) extends CliCommand
  final case class History(
    server: String,
    slugs: List[String],
    full: Boolean,
    includeFinished: Boolean,
    refresh: Boolean,
    refreshMinHours: Option[Int]
  ) extends CliCommand
  final case class Recruit(
    server: String,
    slug: String,
    alias: Option[String],
    target: Option[Int],
    cumulative: Boolean,
    sourceClubs: List[String],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean]
  ) extends CliCommand
  final case class Stats(server: String, slug: String, since: Option[String], until: Option[String]) extends CliCommand
  final case class Jobs(server: String, limit: Option[Int]) extends CliCommand
  final case class Logs(server: String, jobId: String) extends CliCommand
  final case class BlacklistAdd(
    server: String,
    slug: String,
    usernames: List[String],
    reason: Option[String],
    months: Option[Int]
  ) extends CliCommand
  final case class BlacklistList(server: String, slug: String) extends CliCommand
  final case class BlacklistRemove(server: String, slug: String, username: String) extends CliCommand
  final case class ScheduleList(server: String) extends CliCommand
  final case class ScheduleAdd(
    server: String,
    kind: String,
    intervalHours: Int,
    club: Option[String],
    params: Option[String]
  ) extends CliCommand
  final case class ScheduleRemove(server: String, id: Long) extends CliCommand
  // No server interaction: `completion` emits a static script. `server` is unused but required by the trait.
  final case class Completion(shell: String) extends CliCommand { def server: String = "" }

  // --- Shared options ---

  private val serverOpt: Options[String] =
    Options.text("server").withDefault(DefaultServer) ?? s"Base URL of the ccas server (default $DefaultServer)"

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
    Command("serve", serverOpt)
      .withHelp("Run the ccas backend HTTP server in this process")
      .map(Serve.apply)

  private val membership: Command[CliCommand] =
    Command("membership", serverOpt ++ trustOpt, slugsArg)
      .withHelp("Submit a membership-sync job for one or more clubs")
      .map { case ((server, trust), slugs) =>
        Membership(server, slugs, trust)
      }

  private val history: Command[CliCommand] =
    Command(
      "history",
      serverOpt ++
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

  private val recruit: Command[CliCommand] =
    Command(
      "recruit",
      serverOpt ++
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

  private val stats: Command[CliCommand] =
    Command(
      "stats",
      serverOpt ++
        (Options.text("since").optional ?? "Start of the date window (ISO-8601 date or instant)") ++
        (Options.text("until").optional ?? "End of the date window (requires --since)"),
      Args.text("slug") ?? "Club slug (URL name)"
    ).withHelp("Submit a club performance-stats job")
      .map { case ((server, since, until), slug) => Stats(server, slug, since, until) }

  private val jobs: Command[CliCommand] =
    Command("jobs", serverOpt ++ (intOpt("limit") ?? "Maximum number of recent jobs to list"))
      .withHelp("List recent jobs and their status")
      .map { case (server, limit) => Jobs(server, limit) }

  private val logs: Command[CliCommand] =
    Command("logs", serverOpt, Args.text("jobId") ?? "Job run id to poll")
      .withHelp("Poll a job's status and logs until it finishes")
      .map { case (server, jobId) => Logs(server, jobId) }

  private val blacklistAdd: Command[CliCommand] =
    Command(
      "add",
      serverOpt ++
        (Options.text("reason").optional ?? "Reason recorded with the blacklist entry") ++
        (intOpt("months") ?? "Auto-expire the entry after N months"),
      (Args.text("slug") ?? "Club slug (URL name)") ++ (Args.text("username").repeat1 ?? "Username(s) to blacklist")
    ).withHelp("Blacklist one or more usernames for a club")
      .map { case ((server, reason, months), (slug, usernames)) =>
        BlacklistAdd(server, slug, usernames, reason, months)
      }

  private val blacklistList: Command[CliCommand] =
    Command("list", serverOpt, Args.text("slug") ?? "Club slug (URL name)")
      .withHelp("List a club's blacklist entries")
      .map { case (server, slug) => BlacklistList(server, slug) }

  private val blacklistRemove: Command[CliCommand] =
    Command(
      "remove",
      serverOpt,
      (Args.text("slug") ?? "Club slug (URL name)") ++ (Args.text("username") ?? "Username to remove")
    ).withHelp("Remove a username from a club's blacklist")
      .map { case (server, (slug, username)) =>
        BlacklistRemove(server, slug, username)
      }

  private val blacklist: Command[CliCommand] =
    Command("blacklist")
      .withHelp("Manage a club's recruitment blacklist")
      .subcommands(blacklistAdd, blacklistList, blacklistRemove)

  private val scheduleList: Command[CliCommand] =
    Command("list", serverOpt)
      .withHelp("List scheduled jobs")
      .map(ScheduleList.apply)

  private val scheduleAdd: Command[CliCommand] =
    Command(
      "add",
      serverOpt ++
        (Options.text("kind") ?? "Job kind: Recruitment, Membership, MatchRef, History, Stats, or ClubData") ++
        (Options.integer("interval-hours").map(_.toInt) ?? "Run the job every N hours") ++
        (Options.text("club").optional ?? "Club slug the job targets (when the kind needs one)") ++
        (Options.text("params").optional ?? "Extra job parameters passed to the run")
    ).withHelp("Create a scheduled job")
      .map { case (server, kind, intervalHours, club, params) =>
        ScheduleAdd(server, kind, intervalHours, club, params)
      }

  private val scheduleRemove: Command[CliCommand] =
    Command("remove", serverOpt, Args.integer("id").map(_.toLong) ?? "Schedule id to delete")
      .withHelp("Delete a scheduled job by id")
      .map { case (server, id) =>
        ScheduleRemove(server, id)
      }

  private val schedule: Command[CliCommand] =
    Command("schedule")
      .withHelp("Manage scheduled jobs")
      .subcommands(scheduleList, scheduleAdd, scheduleRemove)

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

  val command: Command[CliCommand] =
    Command("ccas").subcommands(serve, membership, history, recruit, stats, jobs, logs, blacklist, schedule, completion)
}
