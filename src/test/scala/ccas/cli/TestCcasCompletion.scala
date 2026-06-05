package ccas.cli

import java.io.IOException
import java.nio.file.{Files, Paths}

import zio.ZIO
import zio.test.{assertCompletes, assertTrue, Spec, ZIOSpecDefault}

/** Drift guard for the static bash completion (`completions/ccas.bash`). That script is hand-maintained for instant,
  * JVM-free completion (Sootopolis/ccas#49); this test asserts it stays valid bash and still covers every command
  * and option in the [[CliCommand]] tree, so a newly added subcommand or flag can't silently go uncompleted. The
  * command tree is read by rendering the root help in-process (no JVM spawn). Paths are relative to the repo root
  * (sbt's test cwd).
  */
object TestCcasCompletion extends ZIOSpecDefault {

  private val ScriptPath = "completions/ccas.bash"

  private val script = Files.readString(Paths.get(ScriptPath))

  // Full tree help: the COMMANDS enumeration lists every command path with its complete option synopsis.
  private val help = CliCommand.command.helpDoc.toPlaintext(1000, color = false)

  // Long-option tokens used anywhere in the tree, e.g. --server, --no-trust-usernames, --refresh-min-hours.
  private val flags: Set[String] = "--[a-z][a-z0-9-]*".r.findAllIn(help).toSet

  // Command words from enumeration lines like "- blacklist add [--server …] <slug> …" (path = words before [ or <).
  // Assumes every command carries at least one option or positional arg (true today — all have --server); a command
  // with neither would render a bracketless line and be skipped here.
  private val commandWords: Set[String] =
    """(?m)^\s*-\s+([a-z][a-z ]*?) +[\[<]""".r
      .findAllMatchIn(help)
      .flatMap(_.group(1).trim.split(" "))
      .toSet

  override def spec: Spec[Any, Any] = suite("TestCcasCompletion")(
    // Token-presence guards catch drift but not a broken `case`/syntax error; `bash -n` catches the latter.
    test(s"$ScriptPath is syntactically valid bash") {
      val check =
        for {
          proc    <- ZIO.attempt(new ProcessBuilder("bash", "-n", ScriptPath).redirectErrorStream(true).start())
          output  <- ZIO.attempt(new String(proc.getInputStream.readAllBytes()))
          code    <- ZIO.attemptBlocking(proc.waitFor())
        } yield assertTrue(code == 0, output.isEmpty)
      // No bash on PATH (unlikely in CI/dev) -> skip rather than fail.
      check.catchSome { case _: IOException => ZIO.succeed(assertCompletes) }
    },
    test(s"every command word in the tree appears in $ScriptPath") {
      val missing = commandWords.filterNot(script.contains)
      assertTrue(commandWords.nonEmpty, missing.isEmpty)
    },
    test(s"every option in the tree appears in $ScriptPath") {
      val missing = flags.filterNot(script.contains)
      assertTrue(flags.nonEmpty, missing.isEmpty)
    }
  )
}
