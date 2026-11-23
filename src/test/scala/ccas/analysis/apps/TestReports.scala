package ccas.analysis.apps

import java.nio.file.Paths

import zio.{ULayer, ZLayer}

/** Where a test's reports go: under `target/`, never the real XDG data directory. */
object TestReports {

  val root: ULayer[ReportRoot] = ZLayer.succeed(ReportRoot.at(Paths.get("target", "test-output")))
}
