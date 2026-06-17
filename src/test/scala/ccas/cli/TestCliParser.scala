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
    test("membership parses slugs and defaults the server") {
      parsed("membership", "team-alpha", "team-beta").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha", "team-beta"), None)))
      )
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
    test("membership with no slug fails validation") {
      parse("membership").exit.map(e => assertTrue(e.isFailure))
    },
    // zio-cli requires options before positional arguments (see CliCommand scaladoc).
    test("--no-trust-usernames maps to Some(false)") {
      parsed("membership", "--no-trust-usernames", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.Membership(DefaultServer, List("team-alpha"), Some(false))))
      )
    },
    test("recruit parses options, comma-separated source-clubs, and slug") {
      parsed("recruit", "--target", "5", "--no-explore", "--source-clubs", "x,y", "team-alpha").map(c =>
        assertTrue(
          c.contains(
            CliCommand.Recruit(DefaultServer, "team-alpha", None, Some(5), false, List("x", "y"), None, Some(false))
          )
        )
      )
    },
    test("history flags parse") {
      parsed("history", "--full", "--include-finished", "team-alpha").map(c =>
        assertTrue(c.contains(CliCommand.History(DefaultServer, List("team-alpha"), true, true, false, None)))
      )
    },
    test("blacklist add parses usernames and options") {
      parsed("blacklist", "add", "--reason", "spam", "--months", "3", "team-alpha", "u1", "u2").map(c =>
        assertTrue(c.contains(CliCommand.BlacklistAdd(DefaultServer, "team-alpha", List("u1", "u2"), Some("spam"), Some(3))))
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
    test("completion parses the shell argument (no server)") {
      parsed("completion", "bash").map(c => assertTrue(c.contains(CliCommand.Completion("bash"))))
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
