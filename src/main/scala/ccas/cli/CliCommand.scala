package ccas.cli

import zio.cli.*

/** Parsed `ccas` subcommand. Each variant carries the resolved server URL plus its already-validated arguments, so
  * the dispatcher only translates these into HTTP calls; zio-cli does all parsing and validation.
  *
  * The global `--server` option is composed into every subcommand. zio-cli takes bare option names and renders the
  * leading dashes itself (`server` -> `--server`). Club targeting is by option, never positional — see
  * `docs/adr/0011-cli-locality-and-the-current-club-pointer.md`.
  *
  * IMPORTANT — option/argument ordering: zio-cli expects all options BEFORE positionals, e.g.
  * `ccas blacklist add --club team-alpha alice bob`. An option placed AFTER a positional is NOT an error — it is
  * silently swallowed as a positional value. Each subcommand's `--help` shows the required `[options] <args>` order.
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
  case object ServerStatus extends CliCommand
  final case class Completion(shell: String) extends CliCommand
  final case class Use(slugs: List[String], clear: Boolean) extends CliCommand
  final case class ConfigGet(key: String) extends CliCommand
  final case class ConfigSet(key: String, value: String) extends CliCommand
  final case class ConfigUnset(key: String) extends CliCommand
  final case class ConfigList(showSecrets: Boolean) extends CliCommand
  case object ConfigPath extends CliCommand
  case object ConfigInit extends CliCommand

  // Server commands — dispatched as HTTP calls by `Dispatcher`. Club-targeting fields hold the *parsed* request
  // (`clubs`/`club` may be empty/None); `Dispatcher` resolves them against `--all` and the config's `current_club`.
  final case class Membership(
    server: String,
    clubs: List[String],
    all: Boolean,
    trustUsernames: Option[Boolean],
    noProgress: Boolean,
    detach: Boolean
  ) extends ServerCommand
  final case class History(
    server: String,
    clubs: List[String],
    all: Boolean,
    full: Boolean,
    includeFinished: Boolean,
    refresh: Boolean,
    refreshMinHours: Option[Int],
    noProgress: Boolean,
    detach: Boolean
  ) extends ServerCommand
  final case class Recruit(
    server: String,
    club: Option[String],
    alias: Option[String],
    target: Option[Int],
    cumulative: Boolean,
    sourceClubs: List[String],
    timeLimitMinutes: Option[Int],
    explore: Option[Boolean],
    stdout: Boolean,
    report: Boolean,
    runId: Option[Int],
    noProgress: Boolean
  ) extends ServerCommand
  final case class Stats(
    server: String,
    club: Option[String],
    since: Option[String],
    until: Option[String],
    noProgress: Boolean,
    detach: Boolean
  ) extends ServerCommand
  final case class Jobs(server: String, limit: Option[Int]) extends ServerCommand
  final case class Logs(server: String, jobId: String, noProgress: Boolean) extends ServerCommand
  final case class Cancel(server: String, jobId: String) extends ServerCommand
  final case class BlacklistAdd(
    server: String,
    club: Option[String],
    usernames: List[String],
    reason: Option[String],
    months: Option[Int]
  ) extends ServerCommand
  final case class BlacklistList(server: String, club: Option[String]) extends ServerCommand
  final case class BlacklistRemove(server: String, club: Option[String], username: String) extends ServerCommand
  final case class ScheduleList(server: String) extends ServerCommand
  final case class ScheduleAdd(
    server: String,
    kind: String,
    intervalHours: Option[Int],
    cron: Option[String],
    tz: Option[String],
    misfire: Option[String],
    club: Option[String],
    params: Option[String]
  ) extends ServerCommand
  final case class ScheduleRemove(server: String, id: Long) extends ServerCommand
  final case class ClubsAdd(server: String, slug: String) extends ServerCommand
  final case class ClubsRemove(server: String, slug: String) extends ServerCommand
  final case class ClubsList(server: String) extends ServerCommand

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

  // Opt out of live progress bars while following a job (plain log lines only). Bars otherwise render on an interactive
  // terminal; a non-TTY (piped/redirected) already suppresses them regardless of this flag.
  private val noProgressOpt: Options[Boolean] =
    Options.boolean("no-progress") ?? "Don't render progress bars while following the job (plain log lines only)"

  // Fire-and-forget: submit the job and return immediately without following it. Reattach later with `ccas logs <id>`.
  // Not offered on `recruit` (its result delivery / invite confirmation needs the follow) or `logs` (which IS the follow).
  private val detachOpt: Options[Boolean] =
    Options.boolean("detach") ?? "Submit the job and return immediately without following it (reattach with 'ccas logs <id>')"

  private def intOpt(name: String): Options[Option[Int]] = Options.integer(name).map(_.toInt).optional

  // Single-club target. Absent → Dispatcher falls back to the config's `current_club`.
  private val clubOpt: Options[Option[String]] =
    Options.text("club").optional ?? "Club slug (URL name); falls back to the current club (set with 'ccas use-club')"

  // Multi-club target: comma-separated slugs. Absent (and without --all) → Dispatcher falls back to `current_club`.
  private val clubsOpt: Options[List[String]] =
    (Options.text("club").optional ?? "Comma-separated club slugs; falls back to the current club (set with 'ccas use-club')")
      .map(_.fold(List.empty[String])(_.split(",").toList.map(_.trim).filter(_.nonEmpty)))

  private val allOpt: Options[Boolean] =
    Options.boolean("all") ?? "Run for every managed club (overrides --club and the current club)"

  // --- Leaf commands (each typed Command[CliCommand] so subcommands share a uniform type) ---

  // NOTE: `Detach.reconstruct`/`fallbackCommand` rebuild the detached child as `... ccas.cli.Main server up`, so
  // renaming this subcommand requires updating them in lockstep or detached launch breaks.
  private val serverUp: Command[CliCommand] =
    Command(
      "up",
      Options.boolean("detach").alias("d") ?? "Run the server as a detached background process (writes a pid file; stop with 'ccas server down')"
    ).withHelp("Run the ccas backend HTTP server (foreground by default; --detach/-d to background it)")
      .map(detach => Serve(detach))

  private val serverDown: Command[CliCommand] =
    Command("down")
      .withHelp("Stop a detached ccas server (reads the pid file and sends SIGTERM)")
      .map(_ => Stop)

  private val serverStatus: Command[CliCommand] =
    Command("status")
      .withHelp("Report whether the local ccas server is running and ready")
      .map(_ => ServerStatus)

  private val serverGroup: Command[CliCommand] =
    Command("server")
      .withHelp("Run and manage the ccas backend HTTP server")
      .subcommands(serverUp, serverDown, serverStatus)

  // The slug is optional so a bare `ccas use-club` PRINTS the current club rather than erroring — the only read path
  // for a value that is otherwise write-only (`git branch` / `kubectl config current-context` shape). `--clear` drops
  // it; passing both is rejected as conflicting intent, mirroring `--all` vs `--club`.
  //
  // `repeat` rather than `atMost(1)` because `atMost(1)` silently TRUNCATES extra positionals instead of rejecting
  // them. Combined with the zio-cli ordering bug documented at the top of this file — an option written after a
  // positional is swallowed as another positional — `ccas use-club team-alpha --clear` would parse as a plain set and
  // silently set the very club the user asked to clear. Capturing every positional lets `UseClub` reject the arity and
  // point at the right ordering.
  private val useClub: Command[CliCommand] =
    Command(
      "use-club",
      Options.boolean("clear") ?? "Clear the current club instead of setting one",
      (Args.text("slug") ?? "Club slug (URL name) to set as the current club; omit to print the current one").repeat
    ).withHelp("Show, set, or clear the current club used by commands that omit --club")
      .map { case (clear, slugs) => Use(slugs, clear) }

  private def membership(default: String): Command[CliCommand] =
    Command("membership", serverOpt(default) ++ trustOpt ++ clubsOpt ++ allOpt ++ noProgressOpt ++ detachOpt)
      .withHelp("Submit a membership-sync job (current club, --club a,b, or --all managed clubs)")
      .map { case (server, trust, clubs, all, noProgress, detach) =>
        Membership(server, clubs, all, trust, noProgress, detach)
      }

  private def history(default: String): Command[CliCommand] =
    Command(
      "history",
      serverOpt(default) ++
        (Options.boolean("full") ?? "Clear member-query history and re-crawl all members") ++
        (Options.boolean("include-finished") ?? "Re-queue recently finished matches for refresh") ++
        (Options.boolean("refresh") ?? "Refresh already-stored matches, not just newly seen ones") ++
        (intOpt("refresh-min-hours") ?? "Skip refreshing a match seen within the last N hours") ++
        clubsOpt ++ allOpt ++ noProgressOpt ++ detachOpt
    ).withHelp("Submit a match-history crawl job (current club, --club a,b, or --all managed clubs)")
      .map { case (server, full, includeFinished, refresh, refreshMinHours, clubs, all, noProgress, detach) =>
        History(server, clubs, all, full, includeFinished, refresh, refreshMinHours, noProgress, detach)
      }

  private val sourceClubsOpt: Options[List[String]] =
    (Options.text("source-clubs").optional ?? "Comma-separated club slugs to scout for candidates")
      .map(_.fold(List.empty[String])(_.split(",").toList.map(_.trim).filter(_.nonEmpty)))

  private def recruit(default: String): Command[CliCommand] =
    Command(
      "recruit",
      serverOpt(default) ++
        (Options.text("alias").optional ?? "Recruitment-criteria alias to use (default: the club's default)") ++
        (intOpt("target") ?? "Stop once N candidates have been found") ++
        (Options.boolean("cumulative") ?? "Count candidates already found earlier today toward the target") ++
        sourceClubsOpt ++
        (intOpt("time-limit-minutes") ?? "Stop scouting after roughly N minutes") ++
        exploreOpt ++ clubOpt ++
        (Options.boolean("stdout") ?? "Print invited usernames (bare, newline-separated) to stdout for piping to a clipboard tool (e.g. wl-copy); logs go to stderr. Auto-confirms invites") ++
        (Options.boolean("report") ?? "Show a past run's invited usernames instead of scouting (the club's latest run, or the run id given as an argument)") ++
        noProgressOpt,
      (Args.integer("run-id") ?? "With --report, the run id to show (default: the club's latest run)")
        .atMost(1)
        .map(_.headOption.map(_.toInt))
    ).withHelp("Submit a recruitment scouting job for a club (interactive runs confirm invites before marking them)")
      // Options and the optional `[run-id]` arg combine as (optionsTuple, argValue), so destructure the two levels.
      .map {
        case ((server, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore, club, stdout, report, noProgress), runId) =>
          Recruit(server, club, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore, stdout, report, runId, noProgress)
      }

  private def stats(default: String): Command[CliCommand] =
    Command(
      "stats",
      serverOpt(default) ++
        (Options.text("since").optional ?? "Start of the date window (ISO-8601 date or instant)") ++
        (Options.text("until").optional ?? "End of the date window (requires --since)") ++
        clubOpt ++ noProgressOpt ++ detachOpt
    ).withHelp("Submit a club performance-stats job")
      .map { case (server, since, until, club, noProgress, detach) =>
        Stats(server, club, since, until, noProgress, detach)
      }

  private def jobs(default: String): Command[CliCommand] =
    Command("jobs", serverOpt(default) ++ (intOpt("limit") ?? "Maximum number of recent jobs to list"))
      .withHelp("List recent jobs and their status")
      .map { case (server, limit) => Jobs(server, limit) }

  private def logs(default: String): Command[CliCommand] =
    Command("logs", serverOpt(default) ++ noProgressOpt, Args.text("jobId") ?? "Job run id to poll")
      .withHelp("Poll a job's status and logs until it finishes")
      .map { case ((server, noProgress), jobId) => Logs(server, jobId, noProgress) }

  private def cancel(default: String): Command[CliCommand] =
    Command("cancel", serverOpt(default), Args.text("jobId") ?? "Job run id to cancel")
      .withHelp("Request cancellation of a running job (the job stops as soon as it reaches an interruptible point)")
      .map { case (server, jobId) => Cancel(server, jobId) }

  private def blacklistAdd(default: String): Command[CliCommand] =
    Command(
      "add",
      serverOpt(default) ++
        (Options.text("reason").optional ?? "Reason recorded with the blacklist entry") ++
        (intOpt("months") ?? "Auto-expire the entry after N months") ++ clubOpt,
      Args.text("username").repeat1 ?? "Username(s) to blacklist"
    ).withHelp("Blacklist one or more usernames for a club")
      .map { case ((server, reason, months, club), usernames) =>
        BlacklistAdd(server, club, usernames, reason, months)
      }

  private def blacklistList(default: String): Command[CliCommand] =
    Command("list", serverOpt(default) ++ clubOpt)
      .withHelp("List a club's blacklist entries")
      .map { case (server, club) => BlacklistList(server, club) }

  private def blacklistRemove(default: String): Command[CliCommand] =
    Command(
      "remove",
      serverOpt(default) ++ clubOpt,
      Args.text("username") ?? "Username to remove"
    ).withHelp("Remove a username from a club's blacklist")
      .map { case ((server, club), username) =>
        BlacklistRemove(server, club, username)
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
        (intOpt("interval-hours") ?? "Interval trigger: run the job every N hours") ++
        (Options.text("cron").optional ?? "Cron trigger: 5-field expression, e.g. '0 9 * * MON'") ++
        (Options.text("tz").optional ?? "Cron timezone (IANA, e.g. Europe/London; default UTC)") ++
        (Options.text("misfire").optional ?? "Cron misfire policy: skip (default) or catch_up") ++
        (Options.text("club").optional ?? "Club slug the job targets (when the kind needs one)") ++
        (Options.text("params").optional ?? "Extra job parameters passed to the run")
    ).withHelp("Create a scheduled job (interval via --interval-hours, or wall-clock via --cron)")
      .map { case (server, kind, intervalHours, cron, tz, misfire, club, params) =>
        ScheduleAdd(server, kind, intervalHours, cron, tz, misfire, club, params)
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

  // `club add`/`remove` manage the membership set itself, so the club is the direct operand — a required positional
  // slug (like `git remote add <name>`), not the `--club` context option the operation commands use.
  private def clubsAdd(default: String): Command[CliCommand] =
    Command("add", serverOpt(default), Args.text("slug") ?? "Club slug (URL name) to start managing")
      .withHelp("Mark a club as one you manage with CCAS")
      .map { case (server, slug) => ClubsAdd(server, slug) }

  private def clubsRemove(default: String): Command[CliCommand] =
    Command("remove", serverOpt(default), Args.text("slug") ?? "Club slug (URL name) to stop managing")
      .withHelp("Remove a club from the ones you manage")
      .map { case (server, slug) => ClubsRemove(server, slug) }

  private def clubsList(default: String): Command[CliCommand] =
    Command("list", serverOpt(default))
      .withHelp("List the clubs you manage with CCAS")
      .map(ClubsList.apply)

  // The group help names `use-club` because the two are easy to confuse and live on opposite sides of the tree: this
  // group edits the server-side managed set, while `use-club` is a local pointer at which of them commands target.
  private def clubs(default: String): Command[CliCommand] =
    Command("club")
      .withHelp("Manage the set of clubs you run CCAS for (pick the one commands target with 'ccas use-club')")
      .subcommands(clubsAdd(default), clubsRemove(default), clubsList(default))

  private val completion: Command[CliCommand] =
    Command("completion", Args.text("shell") ?? "Shell to emit completions for: bash, zsh, or fish")
      .withHelp("Emit a shell completion script (bash, zsh, or fish)")
      .map(Completion.apply)

  // `ccas config` — local management of the server-bootstrap env file (ccas.env). No `--server`: it edits a local file,
  // not a running server. Named `configCmd` because `config` is already the zio-cli `CliConfig` val below.
  private val showSecretsOpt: Options[Boolean] =
    Options.boolean("show-secrets") ?? "Reveal secret values (DATABASE_URL, DB_PASSWORD) instead of ****"

  private val configGet: Command[CliCommand] =
    Command("get", Args.text("key") ?? "Env var name to read")
      .withHelp("Print one value from the server config file (ccas.env)")
      .map(ConfigGet.apply)

  private val configSet: Command[CliCommand] =
    Command("set", Args.text("key") ++ Args.text("value"))
      .withHelp("Set a value in the server config file (written 0600); warns on an unknown key")
      .map { case (key, value) => ConfigSet(key, value) }

  private val configUnset: Command[CliCommand] =
    Command("unset", Args.text("key") ?? "Env var name to remove")
      .withHelp("Remove a value from the server config file")
      .map(ConfigUnset.apply)

  private val configList: Command[CliCommand] =
    Command("list", showSecretsOpt)
      .withHelp("List known settings and current values (secrets redacted unless --show-secrets)")
      .map(ConfigList.apply)

  private val configShow: Command[CliCommand] =
    Command("show", showSecretsOpt)
      .withHelp("Alias for 'config list'")
      .map(ConfigList.apply)

  private val configPath: Command[CliCommand] =
    Command("path")
      .withHelp("Print the server config file path")
      .map(_ => ConfigPath)

  private val configInit: Command[CliCommand] =
    Command("init")
      .withHelp("Interactive wizard to create the server config file (ccas.env)")
      .map(_ => ConfigInit)

  private val configCmd: Command[CliCommand] =
    Command("config")
      .withHelp("Manage the local server-bootstrap config file (ccas.env)")
      .subcommands(configGet, configSet, configUnset, configList, configShow, configPath, configInit)

  /** zio-cli config for the `ccas` command tree. `finalCheckBuiltIn = false` works around a zio-cli 0.8.1 bug: with
    * the default `true`, `ccas <sub> --help` renders the FIRST subcommand's help (`server`) instead of the named one.
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
      serverGroup,
      useClub,
      membership(defaultServer),
      history(defaultServer),
      recruit(defaultServer),
      stats(defaultServer),
      jobs(defaultServer),
      logs(defaultServer),
      cancel(defaultServer),
      blacklist(defaultServer),
      schedule(defaultServer),
      clubs(defaultServer),
      configCmd,
      completion
    )
}
