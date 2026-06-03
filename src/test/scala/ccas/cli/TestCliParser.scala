package ccas.cli

import zio.cli.{CliConfig, CommandDirective}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests for the `zio-cli` command tree via `Command.parse` — no server, no DB. Asserts argv parses to the expected
  * [[CliCommand]] (a `UserDefined` directive), errors fail validation, and `--help` yields a `BuiltIn` directive.
  */
object TestCliParser extends ZIOSpecDefault {

  private val DefaultServer = "http://127.0.0.1:8080"

  // Command.parse expects the top command's own name as the first token (CliApp.run injects it at runtime).
  private def parse(args: String*) =
    CliCommand.command.parse(("ccas" +: args).toList, CliConfig.default)

  private def parsed(args: String*) =
    parse(args*).map {
      case CommandDirective.UserDefined(_, cmd) => Some(cmd)
      case _                                    => None
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
    test("unknown subcommand fails validation") {
      parse("nonsense").exit.map(e => assertTrue(e.isFailure))
    },
    test("--help yields a BuiltIn directive") {
      parse("--help").map(d => assertTrue(d.isInstanceOf[CommandDirective.BuiltIn]))
    }
  )
}
