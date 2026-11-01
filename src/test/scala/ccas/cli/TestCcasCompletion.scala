package ccas.cli

import java.io.IOException
import java.nio.file.{Files, Paths}

import zio.ZIO
import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, TestResult, ZIOSpecDefault}

import ccas.cli.CompletionSpec.PositionalKind
import ccas.server.config.ServerEnvKeys

/** Guards the generated shell completions ([[CompletionEmitter]]). Five concerns:
  *   1. the committed `completions/ccas.bash` equals the bash emitter output (regenerate it as
  *      `.claude/rules/cli.md` says);
  *   2. every emitted script is valid for its shell (`<shell> -n`, skipped when that shell isn't installed);
  *   3. what the bash script offers for `config set`, whose second positional depends on its first;
  *   4. [[CompletionSpec]] still covers every command and flag in the [[CliCommand]] tree, so a newly added
  *      subcommand or flag can't silently go uncompleted;
  *   5. README's `Commands:` line lists what [[CompletionSpec]] does.
  *
  * The command tree is read by rendering the root help in-process (no JVM spawn). Paths are relative to the repo root
  * (sbt's test cwd).
  */
object TestCcasCompletion extends ZIOSpecDefault {

  private val BashScriptPath = "completions/ccas.bash"

  private val ReadmePath = "README.md"

  /** README's `Commands:` line as [[CompletionSpec]] gives it: each top-level command in tree order, a group followed
    * by its subcommands.
    */
  private val commandsLine: String = {
    val children = CompletionSpec.groups.map(group => group.name -> group.children).toMap
    val commands = CompletionSpec.topLevel.map { name =>
      children.get(name).fold(name)(subcommands => s"$name {${subcommands.mkString("|")}}")
    }
    commands.map(command => s"`$command`").mkString("Commands: ", ", ", ".")
  }

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

  // A check that spawns a shell passes when that shell isn't on PATH.
  private def unlessNoShell(check: ZIO[Any, Throwable, TestResult]): ZIO[Any, Throwable, TestResult] =
    check.catchSome { case _: IOException => ZIO.succeed(assertCompletes) }

  // Run `<shell> -n <file>` on the emitted script (parse-only, no execution).
  private def syntaxCheck(shell: String, script: String): ZIO[Any, Throwable, TestResult] =
    unlessNoShell {
      for {
        tmp    <- ZIO.attempt(Files.createTempFile("ccas-completion-", s".$shell"))
        _      <- ZIO.attempt(Files.writeString(tmp, script))
        proc   <- ZIO.attempt(new ProcessBuilder(shell, "-n", tmp.toString).redirectErrorStream(true).start())
        output <- ZIO.attempt(new String(proc.getInputStream.readAllBytes()))
        code   <- ZIO.attemptBlocking(proc.waitFor())
        _      <- ZIO.attempt(Files.deleteIfExists(tmp))
      } yield assertTrue(code == 0, output.isEmpty)
    }

  /** What the emitted bash completion offers for `words`, the last being the one under the cursor. bash runs without
    * rc files, so bash-completion isn't loaded and the script reads `COMP_WORDS` itself. stderr joins the offers, so a
    * script that fails to load can't pass for one that offers nothing.
    */
  private def bashOffers(words: String*): ZIO[Any, Throwable, List[String]] = {
    val line = words.map(word => "'" + word + "'").mkString(" ")
    val script =
      "source \"$1\"; COMP_WORDS=(" + line + "); COMP_CWORD=" + (words.size - 1) + "; _ccas; " +
        "printf '%s\\n' \"${COMPREPLY[@]}\""
    for {
      tmp <- ZIO.attempt(Files.createTempFile("ccas-completion-", ".bash"))
      _   <- ZIO.attempt(Files.writeString(tmp, CompletionEmitter.bash))
      proc <- ZIO.attempt {
        new ProcessBuilder("bash", "--noprofile", "--norc", "-c", script, "bash", tmp.toString)
          .redirectErrorStream(true)
          .start()
      }
      output <- ZIO.attempt(new String(proc.getInputStream.readAllBytes()))
      _      <- ZIO.attemptBlocking(proc.waitFor())
      _      <- ZIO.attempt(Files.deleteIfExists(tmp))
    } yield output.linesIterator.filter(_.nonEmpty).toList
  }

  private def positionalOf(path: String*): PositionalKind =
    CompletionSpec.leaves.find(_.path == path.toList).map(_.positional).getOrElse(PositionalKind.Other)

  override def spec: Spec[Any, Any] = suite("TestCcasCompletion")(
    test(s"$BashScriptPath equals the bash emitter output") {
      val committed = Files.readString(Paths.get(BashScriptPath))
      // On failure: regenerate it as `.claude/rules/cli.md` says.
      assertTrue(committed == CompletionEmitter.bash)
    },
    test(s"$ReadmePath's command list is CompletionSpec's top-level commands and groups, in order") {
      val readmeLine = Files.readString(Paths.get(ReadmePath)).linesIterator.find(_.startsWith("Commands: "))
      // On failure: put the expected line in README.
      assertTrue(readmeLine == Some(commandsLine))
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
    test("bash offers a fixed-value key's values after 'config set <key>'") {
      unlessNoShell {
        for {
          backend <- bashOffers("ccas", "config", "set", "CCAS_BODY_STORE_BACKEND", "")
          toggle  <- bashOffers("ccas", "config", "set", "SCHEDULER_HISTORY_ENABLED", "")
          typed   <- bashOffers("ccas", "config", "set", "SCHEDULER_HISTORY_ENABLED", "t")
        } yield assertTrue(backend == List("fs", "s3"), toggle == List("true", "false"), typed == List("true"))
      }
    },
    test("bash offers nothing after 'config set' on a free-form key") {
      unlessNoShell(bashOffers("ccas", "config", "set", "SERVER_PORT", "").map(offered => assertTrue(offered.isEmpty)))
    },
    test("bash offers key names, not values, at 'config set <TAB>'") {
      unlessNoShell {
        bashOffers("ccas", "config", "set", "").map(offered => assertTrue(offered == ServerEnvKeys.all.map(_.name)))
      }
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
        positionalOf("cancel") == PositionalKind.JobId,
        positionalOf("completion") == PositionalKind.Shell,
        positionalOf("blacklist", "add") == PositionalKind.Other,
        positionalOf("config", "get") == PositionalKind.EnvKey,
        positionalOf("config", "set") == PositionalKind.EnvSetting,
        positionalOf("config", "unset") == PositionalKind.EnvKey
      )
    },
    // Each group's declared children must have a matching leaf under that group name. Guards the class of bug where a
    // group is renamed (`clubs` -> `club`) but its leaf `path`s aren't, leaving `leavesOf(group)` empty so the group's
    // subcommands silently lose argument completion and summaries.
    test("every group's children have a matching leaf under the group name") {
      val mismatches = CompletionSpec.groups.filter { g =>
        CompletionSpec.leavesOf(g.name).map(_.name).toSet != g.children.toSet
      }
      assertTrue(mismatches.isEmpty)
    },
    // `topLevel` drives top-level completion and README's command list, so a command left out of it would be neither
    // offered nor listed, with every check above still passing.
    test("CompletionSpec.topLevel is every leaf's top-level command, once each, in tree order") {
      val leafCommands = CompletionSpec.leaves.map(_.path.head).distinct
      assertTrue(leafCommands == CompletionSpec.topLevel)
    }
  ) @@ TestAspect.sequential
}
