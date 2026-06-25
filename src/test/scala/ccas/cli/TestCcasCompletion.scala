package ccas.cli

import java.io.IOException
import java.nio.file.{Files, Paths}

import zio.ZIO
import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, TestResult, ZIOSpecDefault}

import ccas.cli.CompletionSpec.PositionalKind

/** Guards the generated shell completions ([[CompletionEmitter]]). Three concerns:
  *   1. the committed `completions/ccas.bash` equals the bash emitter output (regenerate with
  *      `ccas completion bash > completions/ccas.bash`);
  *   2. every emitted script is valid for its shell (`<shell> -n`, skipped when that shell isn't installed);
  *   3. [[CompletionSpec]] still covers every command and flag in the [[CliCommand]] tree, so a newly added
  *      subcommand or flag can't silently go uncompleted.
  *
  * The command tree is read by rendering the root help in-process (no JVM spawn). Paths are relative to the repo root
  * (sbt's test cwd).
  */
object TestCcasCompletion extends ZIOSpecDefault {

  private val BashScriptPath = "completions/ccas.bash"

  // Full-tree help: the COMMANDS enumeration lists every command path with its complete option synopsis.
  private val help = CliCommand.command(CliCommand.DefaultServer).helpDoc.toPlaintext(1000, color = false)

  // Long-option tokens used anywhere in the tree, e.g. --server, --no-trust-usernames, --refresh-min-hours.
  private val treeFlags: Set[String] = "--[a-z][a-z0-9-]*".r.findAllIn(help).toSet

  // Value-taking flags render in the synopsis with a metavar ("--server <text>", "--target <integer>"); booleans
  // render bare ("--cumulative"). The completion scripts use this split to offer nothing right after a value flag.
  private val treeValueFlags: Set[String] =
    """(--[a-z][a-z0-9-]*) <""".r.findAllMatchIn(help).map(_.group(1)).toSet

  // Command words from enumeration lines like "- blacklist add [--server …] <slug> …" (path = words before [ or <).
  // The char class allows `-` so a hyphenated top-level command (e.g. `use-club`) is still seen by the drift guard.
  private val treeCommandWords: Set[String] =
    """(?m)^\s*-\s+([a-z][a-z -]*?) +[\[<]""".r
      .findAllMatchIn(help)
      .flatMap(_.group(1).trim.split(" "))
      .toSet

  // Run `<shell> -n <file>` on the emitted script (parse-only, no execution). Skip (pass) if the shell isn't on PATH.
  private def syntaxCheck(shell: String, script: String): ZIO[Any, Throwable, TestResult] = {
    val run =
      for {
        tmp    <- ZIO.attempt(Files.createTempFile("ccas-completion-", s".$shell"))
        _      <- ZIO.attempt(Files.writeString(tmp, script))
        proc   <- ZIO.attempt(new ProcessBuilder(shell, "-n", tmp.toString).redirectErrorStream(true).start())
        output <- ZIO.attempt(new String(proc.getInputStream.readAllBytes()))
        code   <- ZIO.attemptBlocking(proc.waitFor())
        _      <- ZIO.attempt(Files.deleteIfExists(tmp))
      } yield assertTrue(code == 0, output.isEmpty)
    run.catchSome { case _: IOException => ZIO.succeed(assertCompletes) }
  }

  private def positionalOf(path: String*): PositionalKind =
    CompletionSpec.leaves.find(_.path == path.toList).map(_.positional).getOrElse(PositionalKind.Other)

  override def spec: Spec[Any, Any] = suite("TestCcasCompletion")(
    test(s"$BashScriptPath equals the bash emitter output") {
      val committed = Files.readString(Paths.get(BashScriptPath))
      // On failure: regenerate with `ccas completion bash > completions/ccas.bash`.
      assertTrue(committed == CompletionEmitter.bash)
    },
    // These three checks spawn `<shell> -n` subprocesses. They previously carried `@@ TestAspect.flaky` to absorb
    // transient fork failures under parallel-suite memory pressure; that band-aid is gone now that suites run
    // one-at-a-time (`Test / parallelExecution := false`, see build.sbt) — on an unloaded machine the fork is
    // reliable. The suite is `@@ sequential` (below) so the spawns don't overlap each other either. A genuine syntax
    // error still surfaces as a non-zero exit (no retry masks it); a missing shell is skipped inside `syntaxCheck`.
    test("emitted bash is syntactically valid") {
      syntaxCheck("bash", CompletionEmitter.bash)
    },
    test("emitted zsh is syntactically valid (skipped if zsh absent)") {
      syntaxCheck("zsh", CompletionEmitter.zsh)
    },
    test("emitted fish is syntactically valid (skipped if fish absent)") {
      syntaxCheck("fish", CompletionEmitter.fish)
    },
    test("CompletionSpec covers every flag in the command tree") {
      val missing = treeFlags -- CompletionSpec.allFlags
      assertTrue(treeFlags.nonEmpty, missing.isEmpty)
    },
    test("CompletionSpec covers every command word in the tree") {
      val missing = treeCommandWords -- CompletionSpec.allCommandWords
      assertTrue(treeCommandWords.nonEmpty, missing.isEmpty)
    },
    // Guards value-vs-boolean drift: a new value flag (or a flag whose arity changed) that isn't reclassified in
    // CompletionSpec.valueFlags would otherwise mis-complete (suggest right after a value flag, or suppress after a
    // boolean) while the presence checks above still pass.
    test("CompletionSpec.valueFlags matches the tree's value-taking flags") {
      assertTrue(treeValueFlags.nonEmpty, CompletionSpec.valueFlags.toSet == treeValueFlags)
    },
    test("positional kinds are pinned (not recoverable from help text)") {
      assertTrue(
        positionalOf("use-club") == PositionalKind.Slug,
        positionalOf("membership") == PositionalKind.NoArgs,
        positionalOf("history") == PositionalKind.NoArgs,
        positionalOf("recruit") == PositionalKind.NoArgs,
        positionalOf("logs") == PositionalKind.JobId,
        positionalOf("completion") == PositionalKind.Shell,
        positionalOf("blacklist", "add") == PositionalKind.Other
      )
    }
  ) @@ TestAspect.sequential
}
