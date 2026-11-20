package ccas.scripts

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters.*

import zio.{Task, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

/** Commits in a throwaway repository whose `core.hooksPath` is this checkout's `.githooks`. The environment is cleared
  * but for `PATH`, so a `GIT_DIR` exported by the `pre-push` hook running this suite cannot point git at the real
  * repository, and `HOME` is a temp dir, so no global config or hook reaches it.
  */
object TestGitHooks extends ZIOSpecDefault {

  private final case class Result(exitCode: Int, output: String)

  private val Hooks = Paths.get(".githooks").toAbsolutePath.toString

  private def git(repo: Path, args: String*): Task[Result] =
    ZIO.attemptBlocking {
      val builder = new ProcessBuilder(("git" +: args).asJava).directory(repo.toFile).redirectErrorStream(true)
      val vars    = builder.environment()
      val path    = vars.get("PATH")
      vars.clear()
      vars.putAll(Map("PATH" -> path, "HOME" -> repo.toString, "GIT_CONFIG_NOSYSTEM" -> "1").asJava)
      val proc = builder.start()
      val out  = new String(proc.getInputStream.readAllBytes())
      Result(proc.waitFor(), out)
    }

  // A repository on `branch` with one commit made before the hooks were enabled, so `main` and `wip` can exist.
  private def repoOn(branch: String): Task[Path] =
    for {
      repo <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-git-hooks"))
      _    <- git(repo, "init", "--quiet", "--initial-branch=main")
      _    <- git(repo, "config", "user.name", "Test")
      _    <- git(repo, "config", "user.email", "test@example.invalid")
      _    <- git(repo, "commit", "--quiet", "--allow-empty", "-m", "root")
      _    <- git(repo, "config", "core.hooksPath", Hooks)
      _    <- ZIO.whenDiscard(branch != "main")(git(repo, "switch", "--quiet", "-c", branch))
    } yield repo

  private def commit(repo: Path, message: String): Task[Result] =
    git(repo, "commit", "--allow-empty", "-m", message)

  private def commitCount(repo: Path): Task[String] =
    git(repo, "rev-list", "--count", "HEAD").map(_.output.trim)

  override def spec: Spec[Any, Any] = suite("TestGitHooks")(
    test("a commit on main or wip is refused, and leaves the branch where it was") {
      for {
        results <- ZIO.foreach(List("main", "wip")) { branch =>
          for {
            repo   <- repoOn(branch)
            result <- commit(repo, "chore: something")
            count  <- commitCount(repo)
          } yield (result, count)
        }
      } yield assertTrue(results.forall { case (result, count) =>
        result.exitCode != 0 && result.output.contains("git switch -c") && count == "1"
      })
    },
    test("a commit on a feature branch, or a detached HEAD, goes through") {
      for {
        feature  <- repoOn("chore/hooks")
        onBranch <- commit(feature, "chore: something")
        detached <- repoOn("chore/detached")
        _        <- git(detached, "switch", "--quiet", "--detach")
        offAny   <- commit(detached, "chore: something")
      } yield assertTrue(onBranch.exitCode == 0, offAny.exitCode == 0)
    },
    test("a message holding a Claude Code session URL is refused, wherever in the message it sits") {
      val messages = List(
        "chore: something\n\nClaude-Session: https://claude.ai/code/session_01AbCdEf",
        "chore: see claude.ai/code/session_01AbCdEf for why"
      )
      for {
        repo    <- repoOn("chore/hooks")
        results <- ZIO.foreach(messages)(commit(repo, _))
        count   <- commitCount(repo)
      } yield assertTrue(results.forall(_.exitCode != 0), count == "1")
    },
    test("a session URL in a comment line is refused, since -m's default cleanup keeps the comment") {
      for {
        repo   <- repoOn("chore/hooks")
        result <- commit(repo, "chore: something\n# claude.ai/code/session_01AbCdEf")
      } yield assertTrue(result.exitCode != 0)
    },
    test("a session URL only in git commit -v's diff, below the scissors line, goes through") {
      for {
        repo   <- repoOn("chore/hooks")
        _      <- ZIO.attemptBlocking(Files.writeString(repo.resolve("notes.txt"), "claude.ai/code/session_01AbCdEf\n"))
        _      <- git(repo, "add", "notes.txt")
        result <- git(repo, "-c", "core.editor=true", "commit", "--verbose", "--edit", "-m", "chore: add notes")
      } yield assertTrue(result.exitCode == 0)
    },
    test("a message naming Claude and linking Claude Code, but no session, goes through") {
      val message =
        "chore: something\n\nWritten with [Claude Code](https://claude.com/claude-code).\n\n" +
          "Co-authored-by: Claude <noreply@anthropic.com>"
      for {
        repo   <- repoOn("chore/hooks")
        result <- commit(repo, message)
      } yield assertTrue(result.exitCode == 0)
    }
  ) @@ TestAspect.sequential
}
