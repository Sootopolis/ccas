package ccas.cli

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
    * slugs are now targeted by the `--club` option (see [[clubFlag]]), not a positional — the only command left with a
    * positional club slug is `use-club`.
    */
  enum PositionalKind {
    case Slug    // a single club slug: use-club
    case JobId   // a single job-run id: logs
    case Shell   // bash | zsh | fish: completion
    case Other   // an opaque value we don't complete: schedule add/remove, blacklist usernames
    case NoArgs  // no positional (club, if any, comes via --club): serve, membership, jobs, …
  }

  import PositionalKind.*

  /** A runnable leaf command at `path` (e.g. `List("blacklist", "add")`). `flags` are its long options including the
    * inherited `--server`; `valueFlags` is the subset that takes an argument; `positional` is its first positional kind.
    */
  final case class Leaf(path: List[String], flags: List[String], valueFlags: List[String], positional: PositionalKind) {
    def name: String = path.last
    def summary: String = CompletionSpec.summaries.getOrElse(path, "")
  }

  /** A grouping command (`blacklist`, `schedule`) whose own completion is just its child subcommand names. */
  final case class Group(name: String, children: List[String], summary: String)

  private val server = "--server"

  /** The club-targeting option. Completion offers cached club slugs as its value wherever it appears, so it is a
    * value flag on every club command (and on `schedule add`). */
  val clubFlag = "--club"

  /** Leaves in tree order. `valueFlags` always lists `--server` first to match its position in `flags`. */
  val leaves: List[Leaf] = List(
    Leaf(List("serve"), List("--detach"), Nil, NoArgs),
    Leaf(List("stop"), Nil, Nil, NoArgs),
    Leaf(List("use-club"), Nil, Nil, Slug),
    Leaf(
      List("membership"),
      List(server, "--trust-usernames", "--no-trust-usernames", clubFlag, "--all"),
      List(server, clubFlag),
      NoArgs
    ),
    Leaf(
      List("history"),
      List(server, "--full", "--include-finished", "--refresh", "--refresh-min-hours", clubFlag, "--all"),
      List(server, "--refresh-min-hours", clubFlag),
      NoArgs
    ),
    Leaf(
      List("recruit"),
      List(server, "--alias", "--target", "--cumulative", "--source-clubs", "--time-limit-minutes", "--explore",
        "--no-explore", clubFlag),
      List(server, "--alias", "--target", "--source-clubs", "--time-limit-minutes", clubFlag),
      NoArgs
    ),
    Leaf(List("stats"), List(server, "--since", "--until", clubFlag), List(server, "--since", "--until", clubFlag), NoArgs),
    Leaf(List("jobs"), List(server, "--limit"), List(server, "--limit"), NoArgs),
    Leaf(List("logs"), List(server), List(server), JobId),
    Leaf(
      List("blacklist", "add"),
      List(server, "--reason", "--months", clubFlag),
      List(server, "--reason", "--months", clubFlag),
      Other
    ),
    Leaf(List("blacklist", "list"), List(server, clubFlag), List(server, clubFlag), NoArgs),
    Leaf(List("blacklist", "remove"), List(server, clubFlag), List(server, clubFlag), Other),
    Leaf(List("schedule", "list"), List(server), List(server), NoArgs),
    Leaf(
      List("schedule", "add"),
      List(server, "--kind", "--interval-hours", clubFlag, "--params"),
      List(server, "--kind", "--interval-hours", clubFlag, "--params"),
      Other
    ),
    Leaf(List("schedule", "remove"), List(server), List(server), Other),
    Leaf(List("club", "add"), List(server), List(server), Slug),
    Leaf(List("club", "remove"), List(server), List(server), Slug),
    Leaf(List("club", "list"), List(server), List(server), NoArgs),
    Leaf(List("completion"), List("--help"), Nil, Shell)
  )

  val groups: List[Group] = List(
    Group("blacklist", List("add", "list", "remove"), "Manage a club's recruitment blacklist"),
    Group("schedule", List("list", "add", "remove"), "Manage scheduled jobs"),
    Group("club", List("add", "remove", "list"), "Manage the set of clubs you run CCAS for")
  )

  /** Top-level subcommand names, in tree order (matches `CliCommand.command(...).subcommands`). */
  val topLevel: List[String] =
    List("serve", "stop", "use-club", "membership", "history", "recruit", "stats", "jobs", "logs", "blacklist",
      "schedule", "club", "completion")

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

  /** One-line help per command path, mirroring the `withHelp(...)` strings in [[CliCommand]]; used as fish `-d` text. */
  private[cli] val summaries: Map[List[String], String] = Map(
    List("serve")               -> "Run the ccas backend HTTP server (foreground by default; --detach to background it)",
    List("stop")                -> "Stop a detached ccas server (reads the pid file and sends SIGTERM)",
    List("use-club")            -> "Set the current club used by commands that omit --club",
    List("membership")          -> "Submit a membership-sync job (current club, --club a,b, or --all managed clubs)",
    List("history")             -> "Submit a match-history crawl job (current club, --club a,b, or --all managed clubs)",
    List("recruit")             -> "Submit a recruitment scouting job for a club",
    List("stats")               -> "Submit a club performance-stats job",
    List("jobs")                -> "List recent jobs and their status",
    List("logs")                -> "Poll a job's status and logs until it finishes",
    List("completion")          -> "Emit a shell completion script (bash, zsh, or fish)",
    List("club", "add")         -> "Mark a club as one you manage with CCAS",
    List("club", "remove")      -> "Remove a club from the ones you manage",
    List("club", "list")        -> "List the clubs you manage with CCAS",
    List("blacklist", "add")    -> "Blacklist one or more usernames for a club",
    List("blacklist", "list")   -> "List a club's blacklist entries",
    List("blacklist", "remove") -> "Remove a username from a club's blacklist",
    List("schedule", "list")    -> "List scheduled jobs",
    List("schedule", "add")     -> "Create a scheduled job",
    List("schedule", "remove")  -> "Delete a scheduled job by id"
  )
}
