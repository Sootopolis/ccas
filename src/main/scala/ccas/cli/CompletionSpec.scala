package ccas.cli

import zio.cli.Command

/** Single source of truth for shell-completion generation. Mirrors the `zio-cli` [[CliCommand]] tree so one structure
  * drives the bash/zsh/fish emitters ([[CompletionEmitter]]) and the drift guard (`TestCcasCompletion`). Keep it in
  * sync with [[CliCommand]]: the drift test fails if the tree gains a command or flag that is missing here.
  *
  * The emitters need three things the rendered `--help` text can't reliably give them: which flags take a value (so
  * completion offers nothing right after one), the *kind* of a command's first free positional (so it offers the right
  * cache), and whether that positional is variadic. All three are captured explicitly below.
  */
object CompletionSpec {

  /** What a command's positional arguments accept, so completion can offer the right cache (or nothing). Note: club
    * slugs are now targeted by the `--club` option (see [[clubFlag]]), not a positional — the only commands left with a
    * positional club slug are `use-club` and `club add` / `remove` / `show`.
    */
  enum PositionalKind {
    case Slug       // a single club slug: use-club, club add/remove/show
    case JobId      // a single job-run id: logs
    case Shell      // bash | zsh | fish: completion
    case EnvKey     // a server-env var name: config get/unset (candidates from ServerEnvKeys.all)
    case EnvSetting // a server-env var name, then the values that key takes: config set
    case Other      // an opaque value we don't complete: schedule add/remove, blacklist usernames
    case NoArgs     // no positional (club, if any, comes via --club): serve, membership, jobs, …
  }

  import PositionalKind.*

  /** A runnable leaf command at `path` (e.g. `List("blacklist", "add")`). `flags` are its long options including the
    * inherited `--server`; `valueFlags` is the subset that takes an argument; `positional` is its first positional kind.
    */
  final case class Leaf(path: List[String], flags: List[String], valueFlags: List[String], positional: PositionalKind) {
    def name: String = path.last
  }

  /** A grouping command (`blacklist`, `schedule`) whose own completion is just its child subcommand names. */
  final case class Group(name: String, children: List[String])

  private val server = "--server"

  /** The club-targeting option. Completion offers cached club slugs as its value wherever it appears, so it is a
    * value flag on every club command (and on `schedule add`).
    */
  val clubFlag = "--club"

  /** Names a club outright, in place of [[clubFlag]], which is how a contested name gets settled. A value flag with
    * nothing to offer — the ids come from the error that asked for one, not from any cache.
    */
  private val clubIdFlag = "--club-id"

  /** Takes a file's path, so completion offers the filesystem as its value. */
  val fileFlag = "--file"

  /** Leaves in tree order. `valueFlags` always lists `--server` first to match its position in `flags`. */
  val leaves: List[Leaf] = List(
    Leaf(
      path = List("server", "up"),
      flags = List("--detach", "-d", "--ready-timeout-seconds"),
      valueFlags = List("--ready-timeout-seconds"),
      positional = NoArgs
    ),
    Leaf(List("server", "down"), List("--port"), List("--port"), NoArgs),
    Leaf(List("server", "status"), List("--port"), List("--port"), NoArgs),
    Leaf(List("server", "list"), Nil, Nil, NoArgs),
    Leaf(List("use-club"), List("--clear"), Nil, Slug),
    Leaf(
      path = List("membership"),
      flags = List(
        server,
        "--trust-usernames",
        "--no-trust-usernames",
        clubFlag,
        clubIdFlag,
        "--all",
        "--no-progress",
        "--detach"
      ),
      valueFlags = List(server, clubFlag, clubIdFlag),
      positional = NoArgs
    ),
    Leaf(
      path = List("history"),
      flags = List(
        server,
        "--full",
        "--include-finished",
        "--refresh",
        "--refresh-min-hours",
        clubFlag,
        clubIdFlag,
        "--all",
        "--no-progress",
        "--detach"
      ),
      valueFlags = List(server, "--refresh-min-hours", clubFlag, clubIdFlag),
      positional = NoArgs
    ),
    Leaf(
      path = List("recruit"),
      flags = List(
        server,
        "--criteria",
        "--target",
        "--cumulative",
        "--source-clubs",
        "--time-limit-minutes",
        "--explore",
        "--no-explore",
        clubFlag,
        clubIdFlag,
        "--stdout",
        "--report",
        "--no-progress"
      ),
      valueFlags =
        List(server, "--criteria", "--target", "--source-clubs", "--time-limit-minutes", clubFlag, clubIdFlag),
      // `--report`'s optional `[run-id]` is a bare numeric positional with nothing to complete, so NoArgs (suggest
      // nothing) is the right shell behavior.
      positional = NoArgs
    ),
    Leaf(
      path = List("recruitment-criteria", "show"),
      flags = List(server, clubFlag, clubIdFlag, "--json"),
      valueFlags = List(server, clubFlag, clubIdFlag),
      positional = Other
    ),
    Leaf(
      path = List("recruitment-criteria", "set"),
      flags = List(server, clubFlag, clubIdFlag, fileFlag),
      valueFlags = List(server, clubFlag, clubIdFlag, fileFlag),
      positional = Other
    ),
    Leaf(
      path = List("recruitment-criteria", "list"),
      flags = List(server, clubFlag, clubIdFlag),
      valueFlags = List(server, clubFlag, clubIdFlag),
      positional = NoArgs
    ),
    Leaf(
      path = List("stats"),
      flags = List(server, "--since", "--until", clubFlag, clubIdFlag, "--no-progress", "--detach"),
      valueFlags = List(server, "--since", "--until", clubFlag, clubIdFlag),
      positional = NoArgs
    ),
    Leaf(List("jobs"), List(server, "--limit"), List(server, "--limit"), NoArgs),
    Leaf(List("logs"), List(server, "--no-progress"), List(server), JobId),
    Leaf(List("cancel"), List(server), List(server), JobId),
    Leaf(
      path = List("blacklist", "add"),
      flags = List(server, "--reason", "--months", clubFlag, clubIdFlag),
      valueFlags = List(server, "--reason", "--months", clubFlag, clubIdFlag),
      positional = Other
    ),
    Leaf(List("blacklist", "list"), List(server, clubFlag, clubIdFlag), List(server, clubFlag, clubIdFlag), NoArgs),
    Leaf(List("blacklist", "remove"), List(server, clubFlag, clubIdFlag), List(server, clubFlag, clubIdFlag), Other),
    Leaf(List("schedule", "list"), List(server), List(server), NoArgs),
    Leaf(
      path = List("schedule", "add"),
      flags =
        List(server, "--kind", "--interval-hours", "--cron", "--tz", "--misfire", clubFlag, clubIdFlag, "--params"),
      valueFlags =
        List(server, "--kind", "--interval-hours", "--cron", "--tz", "--misfire", clubFlag, clubIdFlag, "--params"),
      positional = Other
    ),
    Leaf(List("schedule", "remove"), List(server), List(server), Other),
    Leaf(List("club", "add"), List(server, clubIdFlag), List(server, clubIdFlag), Slug),
    Leaf(List("club", "remove"), List(server, clubIdFlag), List(server, clubIdFlag), Slug),
    Leaf(List("club", "list"), List(server), List(server), NoArgs),
    Leaf(List("club", "show"), List(server, clubIdFlag), List(server, clubIdFlag), Slug),
    // `config` is local (no --server): it edits the local ccas.env file. get/set/unset take an env-var-name positional
    // we complete from ServerEnvKeys.all, and set a value after it; --show-secrets is a boolean (not a value flag), so
    // list/show keep empty valueFlags.
    Leaf(List("config", "get"), Nil, Nil, EnvKey),
    Leaf(List("config", "set"), Nil, Nil, EnvSetting),
    Leaf(List("config", "unset"), Nil, Nil, EnvKey),
    Leaf(List("config", "list"), List("--show-secrets"), Nil, NoArgs),
    Leaf(List("config", "show"), List("--show-secrets"), Nil, NoArgs),
    Leaf(List("config", "path"), Nil, Nil, NoArgs),
    Leaf(List("config", "init"), Nil, Nil, NoArgs),
    Leaf(List("completion"), List("--help"), Nil, Shell)
  )

  val groups: List[Group] = List(
    Group("server", List("up", "down", "status", "list")),
    Group("recruitment-criteria", List("show", "set", "list")),
    Group("blacklist", List("add", "list", "remove")),
    Group("schedule", List("list", "add", "remove")),
    Group("club", List("add", "remove", "list", "show")),
    Group("config", List("get", "set", "unset", "list", "show", "path", "init"))
  )

  /** Top-level subcommand names, in tree order (matches `CliCommand.command(...).subcommands`). */
  val topLevel: List[String] =
    List(
      "server",
      "use-club",
      "membership",
      "history",
      "recruit",
      "recruitment-criteria",
      "stats",
      "jobs",
      "logs",
      "cancel",
      "blacklist",
      "schedule",
      "club",
      "config",
      "completion"
    )

  /** Flags available on every command. */
  val globalFlags: List[String] = List("--help", "--version")

  /** Every value-taking flag used anywhere, de-duplicated in first-seen (tree) order. */
  val valueFlags: List[String] = leaves.flatMap(_.valueFlags).distinct

  /** Top-level leaves (single-word path), in tree order. */
  val topLevelLeaves: List[Leaf] = leaves.filter(_.path.sizeIs == 1)

  /** Leaves under the given group name, in tree order. */
  def leavesOf(group: String): List[Leaf] = leaves.filter(l => l.path.sizeIs == 2 && l.path.head == group)

  /** Every command word in the tree — for the drift guard (`treeWords ⊆ this`). */
  val allCommandWords: Set[String] =
    (topLevel ++ groups.flatMap(g => g.name :: g.children) ++ leaves.flatMap(_.path)).toSet

  /** Every long flag used anywhere — for the drift guard (`treeFlags ⊆ this`). */
  val allFlags: Set[String] = (globalFlags ++ leaves.flatMap(_.flags)).toSet

  /** A command's one-line help, by its path below `ccas`, as [[CliCommand]] gives it; empty for a path it lacks. */
  def summary(path: List[String]): String = helpByPath.getOrElse(path, "")

  /** Every command's help, read from the [[CliCommand]] tree rather than copied, so the two can't drift (#492). */
  private[cli] val helpByPath: Map[List[String], String] = {
    def walk(command: Command[?], prefix: List[String]): List[(List[String], String)] = command match {
      case Command.Map(inner, _)       => walk(inner, prefix)
      case Command.OrElse(left, right) => walk(left, prefix) ++ walk(right, prefix)
      case Command.Subcommands(parent, child) =>
        val own = walk(parent, prefix)
        own ++ own.flatMap { case (path, _) => walk(child, path) }
      case single: Command.Single[?, ?] => List((prefix :+ single.name) -> single.shortDesc)
    }
    // The root is `ccas` itself, whose own help no completion shows.
    walk(CliCommand.command(CliCommand.DefaultServer), Nil).collect {
      case (_ :: path, help) if path.nonEmpty => path -> help
    }.toMap
  }
}
