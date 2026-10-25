package ccas.utils

import java.nio.file.Paths

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.server.jobs.JobRunner

/** `AppDir#resolve` with the two real directories as subjects, its inputs passed in so no environment rebinding is
  * needed; the last test reads this process's config through `fromConfig`.
  */
object TestAppDir extends ZIOSpecDefault {

  private val JobLogs = JobRunner.LogDir
  private val Reports = OutputFile.Root

  override def spec: Spec[Any, Throwable] = suite("AppDir")(
    test("a configured directory wins over the XDG default, relative or not") {
      val absolute = JobLogs.resolve(
        configured = Some("/srv/ccas-logs"),
        xdgHome = Some("/xdg-state"),
        userHome = Some("/home/u")
      )
      val relative = Reports.resolve(
        configured = Some("reports"),
        xdgHome = Some("/xdg-data"),
        userHome = Some("/home/u")
      )
      assertTrue(
        absolute == Right(Paths.get("/srv/ccas-logs")),
        relative == Right(Paths.get("reports"))
      )
    },
    test("job logs default to $XDG_STATE_HOME/ccas/job-logs and reports to $XDG_DATA_HOME/ccas/output") {
      val jobLogs = JobLogs.resolve(
        configured = None,
        xdgHome = Some("/xdg-state"),
        userHome = Some("/home/u")
      )
      val reports = Reports.resolve(
        configured = None,
        xdgHome = Some("/xdg-data"),
        userHome = Some("/home/u")
      )
      assertTrue(
        jobLogs == Right(Paths.get("/xdg-state/ccas/job-logs")),
        reports == Right(Paths.get("/xdg-data/ccas/output"))
      )
    },
    test("with the XDG variable unset, each falls back under $HOME: .local/state for job logs, .local/share for reports") {
      val jobLogs = JobLogs.resolve(configured = None, xdgHome = None, userHome = Some("/home/u"))
      val reports = Reports.resolve(configured = None, xdgHome = None, userHome = Some("/home/u"))
      assertTrue(
        jobLogs == Right(Paths.get("/home/u/.local/state/ccas/job-logs")),
        reports == Right(Paths.get("/home/u/.local/share/ccas/output"))
      )
    },
    test("a relative XDG variable is ignored, as the XDG spec requires, rather than resolved against the launch directory") {
      val resolved = Reports.resolve(
        configured = None,
        xdgHome = Some("share"),
        userHome = Some("/home/u")
      )
      assertTrue(resolved == Right(Paths.get("/home/u/.local/share/ccas/output")))
    },
    test("blank values are treated as absent, never as the launch directory") {
      val blankSetting = JobLogs.resolve(
        configured = Some("   "),
        xdgHome = Some("/xdg-state"),
        userHome = Some("/home/u")
      )
      val blankXdg = Reports.resolve(configured = None, xdgHome = Some(""), userHome = Some("/home/u"))
      assertTrue(
        blankSetting == Right(Paths.get("/xdg-state/ccas/job-logs")),
        blankXdg == Right(Paths.get("/home/u/.local/share/ccas/output"))
      )
    },
    test("no configured directory and no resolvable home is a Left naming the variable to set") {
      val missing = JobLogs.resolve(configured = None, xdgHome = None, userHome = None)
      val blank   = Reports.resolve(configured = Some(" "), xdgHome = Some(""), userHome = Some(" "))
      // The JDK's `user.home` for a user with no home directory.
      val relativeHome = JobLogs.resolve(configured = None, xdgHome = None, userHome = Some("?"))
      assertTrue(
        missing.left.exists(_.contains("JOB_LOGS_DIR")),
        blank.left.exists(_.contains("CCAS_OUTPUT_DIR")),
        relativeHome.left.exists(_.contains("JOB_LOGS_DIR"))
      )
    },
    test("an unset key resolves to the XDG default instead of failing, as on every default install") {
      // With its variable unset, the shipped config has no such key at all. The test config sets both real ones, so a
      // key nothing defines stands in for them.
      val unset = AppDir(
        configKey = "no-such.directory",
        overrideVar = "NO_SUCH_DIR",
        base = AppDir.XdgBase.State,
        leaf = "probe"
      )
      for {
        resolved <- unset.fromConfig
      } yield assertTrue(resolved.isAbsolute, resolved.endsWith(Paths.get("ccas", "probe")))
    }
  )
}
