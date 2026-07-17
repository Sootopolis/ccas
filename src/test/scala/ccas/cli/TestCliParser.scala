package ccas.cli

import zio.cli.{BuiltInOption, CommandDirective}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests for the `zio-cli` command tree via `Command.parse` — no server, no DB. Asserts argv parses to the expected
  * [[CliCommand]] (a `UserDefined` directive), errors fail validation, and `--help` routes to the correct command
  * (a `BuiltIn` `ShowHelp` directive). Parsing uses [[CliCommand.config]] so behaviour matches the real binary.
  */
object TestCliParser extends ZIOSpecDefault {

  private val DefaultServer = "http://127.0.0.1:8080"

  // Command.parse expects the top command's own name as the first token (CliApp.run injects it at runtime). The real
  // binary passes the config-resolved default into `command`; tests use DefaultServer unless exercising resolution.
  private def parseWith(default: String)(args: String*) =
    CliCommand.command(default).parse(("ccas" +: args).toList, CliCommand.config)

  private def parse(args: String*) = parseWith(DefaultServer)(args*)

  private def parsed(args: String*) =
    parse(args*).map {
      case CommandDirective.UserDefined(_, cmd) => Some(cmd)
      case _                                    => None
    }

  private def serverOf(default: String)(args: String*) =
    parseWith(default)(args*).map {
      case CommandDirective.UserDefined(_, cmd: CliCommand.ServerCommand) => Some(cmd.server)
      case _                                                              => None
    }

  // Rendered help text when `--help` resolves to a ShowHelp directive (None for any other directive). Plain (no ANSI).
  private def helpText(args: String*) =
    parse(args*).map {
      case CommandDirective.BuiltIn(BuiltInOption.ShowHelp(_, doc)) => Some(doc.toPlaintext(200, color = false))
      case _                                                        => None
    }

  override def spec: Spec[Any, Any] = suite("TestCliParser")(
    test("membership parses comma-separated --club and defaults the server") {
      parsed("membership", "--club", "team-alpha,team-beta").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha", "team-beta"), false, None, false)))
      )
    },
    test("membership --all parses with no explicit clubs") {
      parsed("membership", "--all").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, Nil, true, None, false)))
      )
    },
    test("use-club parses the slug (local, no server)") {
      parsed("use-club", "team-alpha").map(c => assertTrue(c.contains(CliCommand.Use("team-alpha"))))
    },
    test("config set parses key and value (local, no server)") {
      parsed("config", "set", "DB_HOST", "localhost").map(c =>
        assertTrue(c.contains(CliCommand.ConfigSet("DB_HOST", "localhost")))
      )
    },
    test("config get parses the key") {
      parsed("config", "get", "DATABASE_URL").map(c => assertTrue(c.contains(CliCommand.ConfigGet("DATABASE_URL"))))
    },
    test("config list defaults --show-secrets to false") {
      parsed("config", "list").map(c => assertTrue(c.contains(CliCommand.ConfigList(false))))
    },
    test("config list --show-secrets parses the flag") {
      parsed("config", "list", "--show-secrets").map(c => assertTrue(c.contains(CliCommand.ConfigList(true))))
    },
    test("config init parses with no args") {
      parsed("config", "init").map(c => assertTrue(c.contains(CliCommand.ConfigInit)))
    },
    test("--server overrides the default") {
      parsed("jobs", "--server", "http://example:9000").map(c =>
        assertTrue(c.contains(CliCommand.Jobs("http://example:9000", None)))
      )
    },
    test("cancel parses the jobId (options before the positional)") {
      parsed("cancel", "--server", "http://example:9000", "job-123").map(c =>
        assertTrue(c.contains(CliCommand.Cancel("http://example:9000", "job-123")))
      )
    },
    test("config api_url becomes the default when --server is absent") {
      serverOf("http://config:1234")("jobs").map(s => assertTrue(s.contains("http://config:1234")))
    },
    test("--server overrides the config-resolved default") {
      serverOf("http://config:1234")("jobs", "--server", "http://flag:9999").map(s =>
        assertTrue(s.contains("http://flag:9999"))
      )
    },
    // No slug and no --club/--all now PARSES (empty clubs); the "no club" error is raised later by the Dispatcher
    // against the config's current_club, not at parse time.
    test("membership with no club parses to empty clubs") {
      parsed("membership").map(c => assertTrue(c.contains(CliCommand.Membership(DefaultServer, Nil, false, None, false))))
    },
    test("--no-trust-usernames maps to Some(false)") {
      parsed("membership", "--no-trust-usernames", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha"), false, Some(false), false)))
      )
    },
    test("--no-progress sets the flag on a follow command") {
      parsed("membership", "--no-progress", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha"), false, None, true)))
      )
    },
    test("recruit parses options, comma-separated source-clubs, and --club") {
      parsed("recruit", "--target", "5", "--no-explore", "--source-clubs", "x,y", "--club", "team-alpha").map(c =>
        assertTrue(
          c.contains(
            CliCommand
              .Recruit(DefaultServer, Some("team-alpha"), None, Some(5), false, List("x", "y"), None, Some(false), false, false, None, false)
          )
        )
      )
    },
    test("recruit with no --club parses to None") {
      parsed("recruit").map(c =>
        assertTrue(c.contains(CliCommand.Recruit(DefaultServer, None, None, None, false, Nil, None, None, false, false, None, false)))
      )
    },
    test("recruit --stdout sets stdout") {
      parsed("recruit", "--stdout").map(c =>
        assertTrue(c.contains(CliCommand.Recruit(DefaultServer, None, None, None, false, Nil, None, None, true, false, None, false)))
      )
    },
    test("recruit --report parses, with an optional run-id argument") {
      for {
        r <- parsed("recruit", "--report")
        n <- parsed("recruit", "--report", "42")
      } yield assertTrue(
        r.contains(CliCommand.Recruit(DefaultServer, None, None, None, false, Nil, None, None, false, true, None, false)),
        n.contains(CliCommand.Recruit(DefaultServer, None, None, None, false, Nil, None, None, false, true, Some(42), false))
      )
    },
    test("history flags parse") {
      parsed("history", "--full", "--include-finished", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.History(DefaultServer, List("team-alpha"), false, true, true, false, None, false)))
      )
    },
    test("blacklist add parses usernames and options") {
      parsed("blacklist", "add", "--reason", "spam", "--months", "3", "--club", "team-alpha", "u1", "u2").map(c =>
        assertTrue(c.contains(CliCommand.BlacklistAdd(DefaultServer, Some("team-alpha"), List("u1", "u2"), Some("spam"), Some(3))))
      )
    },
    test("schedule add parses kind and interval") {
      parsed("schedule", "add", "--kind", "Recruitment", "--interval-hours", "24", "--club", "team-alpha").map(c =>
        assertTrue(
          c.contains(
            CliCommand.ScheduleAdd(DefaultServer, "Recruitment", Some(24), None, None, None, Some("team-alpha"), None)
          )
        )
      )
    },
    test("schedule add parses cron, tz and misfire") {
      parsed(
        "schedule", "add", "--kind", "ClubData", "--cron", "0 9 * * MON", "--tz", "Europe/London", "--misfire", "catch_up"
      ).map(c =>
        assertTrue(
          c.contains(
            CliCommand.ScheduleAdd(
              DefaultServer,
              "ClubData",
              None,
              Some("0 9 * * MON"),
              Some("Europe/London"),
              Some("catch_up"),
              None,
              None
            )
          )
        )
      )
    },
    test("schedule remove parses the id") {
      parsed("schedule", "remove", "7").map(c => assertTrue(c.contains(CliCommand.ScheduleRemove(DefaultServer, 7L))))
    },
    test("club add parses the slug positional") {
      parsed("club", "add", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.ClubsAdd(DefaultServer, "team-alpha")))
      )
    },
    test("club remove parses the slug positional") {
      parsed("club", "remove", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.ClubsRemove(DefaultServer, "team-alpha")))
      )
    },
    test("club list parses with no slug and defaults the server") {
      parsed("club", "list").map(c => assertTrue(c.contains(CliCommand.ClubsList(DefaultServer))))
    },
    // `club add`/`remove` manage the set, so the slug is a required operand — no slug fails validation (not a
    // current_club fallback like the operation commands).
    test("club add with no slug fails validation") {
      parse("club", "add").exit.map(e => assertTrue(e.isFailure))
    },
    test("completion parses the shell argument (no server)") {
      parsed("completion", "bash").map(c => assertTrue(c.contains(CliCommand.Completion("bash"))))
    },
    test("server up defaults to foreground (no --detach)") {
      parsed("server", "up").map(c => assertTrue(c.contains(CliCommand.Serve(false))))
    },
    test("server up --detach parses the flag") {
      parsed("server", "up", "--detach").map(c => assertTrue(c.contains(CliCommand.Serve(true))))
    },
    test("server up -d parses the detach alias") {
      parsed("server", "up", "-d").map(c => assertTrue(c.contains(CliCommand.Serve(true))))
    },
    test("server down parses to Stop (no server)") {
      parsed("server", "down").map(c => assertTrue(c.contains(CliCommand.Stop)))
    },
    test("server status parses (no server)") {
      parsed("server", "status").map(c => assertTrue(c.contains(CliCommand.ServerStatus)))
    },
    test("top-level serve and stop no longer parse (grouped under server)") {
      for {
        serveFailed <- parse("serve").exit.map(_.isFailure)
        stopFailed  <- parse("stop").exit.map(_.isFailure)
      } yield assertTrue(serveFailed, stopFailed)
    },
    test("unknown subcommand fails validation") {
      parse("nonsense").exit.map(e => assertTrue(e.isFailure))
    },
    test("--help yields a BuiltIn directive") {
      parse("--help").map(d => assertTrue(d.isInstanceOf[CommandDirective.BuiltIn]))
    },
    // Regression: zio-cli 0.8.1 routed every `<sub> --help` to the FIRST subcommand (now `server`). With
    // CliCommand.config (finalCheckBuiltIn = false), each --help must render its own command's help.
    test("membership --help renders membership help, not the first subcommand") {
      helpText("membership", "--help").map(t =>
        assertTrue(t.exists(_.contains("trust-usernames"))) // membership-only option; absent from serve
      )
    },
    test("recruit --help renders recruit help") {
      helpText("recruit", "--help").map(t => assertTrue(t.exists(_.contains("source-clubs"))))
    },
    // Match child help sentences, not bare names: "blacklist" contains the substring "list", which would
    // false-positive a `contains("list")` check.
    test("blacklist --help lists the child subcommands") {
      helpText("blacklist", "--help").map(t =>
        assertTrue(t.exists(s => s.contains("List a club's blacklist entries") && s.contains("Remove a username")))
      )
    },
    // `remove` is NOT the first child of `blacklist`, so this catches the routing bug — the bug rendered the
    // first child (add), whose `--months` help "Auto-expire" must be absent here.
    test("blacklist remove --help renders remove, not the first child") {
      helpText("blacklist", "remove", "--help").map(t =>
        assertTrue(t.exists(s => s.contains("Username to remove") && !s.contains("Auto-expire")))
      )
    }
  )
}
