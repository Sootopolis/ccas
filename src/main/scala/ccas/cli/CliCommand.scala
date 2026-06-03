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

  // --- Shared options ---

  private val serverOpt: Options[String] =
    Options.text("server").withDefault(DefaultServer) ?? s"Base URL of the ccas server (default $DefaultServer)"

  // Two switches reduced to Option[Boolean]: --no-x wins, --x => Some(true), neither => None (server default).
  private val trustOpt: Options[Option[Boolean]] =
    (Options.boolean("trust-usernames") ++ Options.boolean("no-trust-usernames")).map {
      case (_, true) => Some(false)
      case (true, _) => Some(true)
      case _         => None
    }

  private val exploreOpt: Options[Option[Boolean]] =
    (Options.boolean("explore") ++ Options.boolean("no-explore")).map {
      case (_, true) => Some(false)
      case (true, _) => Some(true)
      case _         => None
    }

  private def intOpt(name: String): Options[Option[Int]] = Options.integer(name).map(_.toInt).optional

  private val slugsArg: Args[List[String]] = Args.text("slug").repeat1

  // --- Leaf commands (each typed Command[CliCommand] so subcommands share a uniform type) ---

  private val serve: Command[CliCommand] =
    Command("serve", serverOpt).map(Serve.apply)

  private val membership: Command[CliCommand] =
    Command("membership", serverOpt ++ trustOpt, slugsArg).map { case ((server, trust), slugs) =>
      Membership(server, slugs, trust)
    }

  private val history: Command[CliCommand] =
    Command(
      "history",
      serverOpt ++ Options.boolean("full") ++ Options.boolean("include-finished") ++
        Options.boolean("refresh") ++ intOpt("refresh-min-hours"),
      slugsArg
    ).map { case ((server, full, includeFinished, refresh, refreshMinHours), slugs) =>
      History(server, slugs, full, includeFinished, refresh, refreshMinHours)
    }

  private val sourceClubsOpt: Options[List[String]] =
    Options.text("source-clubs").optional.map(_.fold(List.empty[String])(_.split(",").toList.filter(_.nonEmpty)))

  private val recruit: Command[CliCommand] =
    Command(
      "recruit",
      serverOpt ++ Options.text("alias").optional ++ intOpt("target") ++ Options.boolean("cumulative") ++
        sourceClubsOpt ++ intOpt("time-limit-minutes") ++ exploreOpt,
      Args.text("slug")
    ).map { case ((server, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore), slug) =>
      Recruit(server, slug, alias, target, cumulative, sourceClubs, timeLimitMinutes, explore)
    }

  private val stats: Command[CliCommand] =
    Command(
      "stats",
      serverOpt ++ Options.text("since").optional ++ Options.text("until").optional,
      Args.text("slug")
    ).map { case ((server, since, until), slug) => Stats(server, slug, since, until) }

  private val jobs: Command[CliCommand] =
    Command("jobs", serverOpt ++ intOpt("limit")).map { case (server, limit) => Jobs(server, limit) }

  private val logs: Command[CliCommand] =
    Command("logs", serverOpt, Args.text("jobId")).map { case (server, jobId) => Logs(server, jobId) }

  private val blacklistAdd: Command[CliCommand] =
    Command(
      "add",
      serverOpt ++ Options.text("reason").optional ++ intOpt("months"),
      Args.text("slug") ++ Args.text("username").repeat1
    ).map { case ((server, reason, months), (slug, usernames)) =>
      BlacklistAdd(server, slug, usernames, reason, months)
    }

  private val blacklistList: Command[CliCommand] =
    Command("list", serverOpt, Args.text("slug")).map { case (server, slug) => BlacklistList(server, slug) }

  private val blacklistRemove: Command[CliCommand] =
    Command("remove", serverOpt, Args.text("slug") ++ Args.text("username")).map { case (server, (slug, username)) =>
      BlacklistRemove(server, slug, username)
    }

  private val blacklist: Command[CliCommand] =
    Command("blacklist").subcommands(blacklistAdd, blacklistList, blacklistRemove)

  private val scheduleList: Command[CliCommand] =
    Command("list", serverOpt).map(ScheduleList.apply)

  private val scheduleAdd: Command[CliCommand] =
    Command(
      "add",
      serverOpt ++ Options.text("kind") ++ Options.integer("interval-hours").map(_.toInt) ++
        Options.text("club").optional ++ Options.text("params").optional
    ).map { case (server, kind, intervalHours, club, params) =>
      ScheduleAdd(server, kind, intervalHours, club, params)
    }

  private val scheduleRemove: Command[CliCommand] =
    Command("remove", serverOpt, Args.integer("id").map(_.toLong)).map { case (server, id) =>
      ScheduleRemove(server, id)
    }

  private val schedule: Command[CliCommand] =
    Command("schedule").subcommands(scheduleList, scheduleAdd, scheduleRemove)

  val command: Command[CliCommand] =
    Command("ccas").subcommands(serve, membership, history, recruit, stats, jobs, logs, blacklist, schedule)
}
