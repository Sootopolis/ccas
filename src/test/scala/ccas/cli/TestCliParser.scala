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
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha", "team-beta"), false, None)))
      )
    },
    test("membership --all parses with no explicit clubs") {
      parsed("membership", "--all").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, Nil, true, None)))
      )
    },
    test("use-club parses the slug (local, no server)") {
      parsed("use-club", "team-alpha").map(c => assertTrue(c.contains(CliCommand.Use("team-alpha"))))
    },
    test("--server overrides the default") {
      parsed("jobs", "--server", "http://example:9000").map(c =>
        assertTrue(c.contains(CliCommand.Jobs("http://example:9000", None)))
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
      parsed("membership").map(c => assertTrue(c.contains(CliCommand.Membership(DefaultServer, Nil, false, None))))
    },
    test("--no-trust-usernames maps to Some(false)") {
      parsed("membership", "--no-trust-usernames", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha"), false, Some(false))))
      )
    },
    test("recruit parses options, comma-separated source-clubs, and --club") {
      parsed("recruit", "--target", "5", "--no-explore", "--source-clubs", "x,y", "--club", "team-alpha").map(c =>
        assertTrue(
          c.contains(
            CliCommand.Recruit(DefaultServer, Some("team-alpha"), None, Some(5), false, List("x", "y"), None, Some(false))
          )
        )
      )
    },
    test("recruit with no --club parses to None") {
      parsed("recruit").map(c =>
        assertTrue(c.contains(CliCommand.Recruit(DefaultServer, None, None, None, false, Nil, None, None)))
      )
    },
    test("history flags parse") {
      parsed("history", "--full", "--include-finished", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.History(DefaultServer, List("team-alpha"), false, true, true, false, None)))
      )
    },
    test("blacklist add parses usernames and options") {
      parsed("blacklist", "add", "--reason", "spam", "--months", "3", "--club", "team-alpha", "u1", "u2").map(c =>
        assertTrue(c.contains(CliCommand.BlacklistAdd(DefaultServer, Some("team-alpha"), List("u1", "u2"), Some("spam"), Some(3))))
      )
    },
    test("schedule add parses kind and interval") {
      parsed("schedule", "add", "--kind", "Recruitment", "--interval-hours", "24", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.ScheduleAdd(DefaultServer, "Recruitment", 24, Some("team-alpha"), None)))
      )
    },
    test("schedule remove parses the id") {
      parsed("schedule", "remove", "7").map(c => assertTrue(c.contains(CliCommand.ScheduleRemove(DefaultServer, 7L))))
    },
    test("club add parses --club") {
      parsed("club", "add", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.ClubsAdd(DefaultServer, Some("team-alpha"))))
      )
    },
    test("club remove parses --club") {
      parsed("club", "remove", "--club", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.ClubsRemove(DefaultServer, Some("team-alpha"))))
      )
    },
    test("club list parses with no slug and defaults the server") {
      parsed("club", "list").map(c => assertTrue(c.contains(CliCommand.ClubsList(DefaultServer))))
    },
    // No --club now parses (None); the "no club" error is the Dispatcher's job against current_club.
    test("club add with no --club parses to None") {
      parsed("club", "add").map(c => assertTrue(c.contains(CliCommand.ClubsAdd(DefaultServer, None))))
    },
    test("completion parses the shell argument (no server)") {
      parsed("completion", "bash").map(c => assertTrue(c.contains(CliCommand.Completion("bash"))))
    },
    test("serve defaults to foreground (no --detach)") {
      parsed("serve").map(c => assertTrue(c.contains(CliCommand.Serve(false))))
    },
    test("serve --detach parses the flag") {
      parsed("serve", "--detach").map(c => assertTrue(c.contains(CliCommand.Serve(true))))
    },
    test("stop parses (no server)") {
      parsed("stop").map(c => assertTrue(c.contains(CliCommand.Stop)))
    },
    test("unknown subcommand fails validation") {
      parse("nonsense").exit.map(e => assertTrue(e.isFailure))
    },
    test("--help yields a BuiltIn directive") {
      parse("--help").map(d => assertTrue(d.isInstanceOf[CommandDirective.BuiltIn]))
    },
    // Regression: zio-cli 0.8.1 routed every `<sub> --help` to the FIRST subcommand (serve). With
    // CliCommand.config (finalCheckBuiltIn = false), each --help must render its own command's help.
    test("membership --help renders membership help, not serve") {
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
