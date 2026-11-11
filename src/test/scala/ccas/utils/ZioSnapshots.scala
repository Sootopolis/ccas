package ccas.utils

import snapshot4s.{Result, ResultLike, SnapshotAssertions, SnapshotEq}
import zio.test.{assertTrue, TestResult}

/** snapshot4s's file snapshots as ZIO Test results; snapshot4s ships no ZIO Test integration (#455). A test that fails
  * leaves what it found for `sbt snapshot4sPromote` to copy over the checked-in file.
  */
trait ZioSnapshots extends SnapshotAssertions[TestResult] {

  // Compared as lines, so ZIO Test's diff names the line that changed rather than printing the text whole. `-1` keeps
  // trailing empty lines, so a change to the final newline still fails.
  given ResultLike[String, TestResult] = result =>
    result() match {
      case Result.Success(_, _) => assertTrue(true)
      case Result.Failure(found, snapshot) =>
        val foundLines    = found.split("\n", -1).toList
        val snapshotLines = snapshot.split("\n", -1).toList
        assertTrue(foundLines == snapshotLines)
      case Result.NonExistent(_) =>
        assertTrue(false).label("no snapshot yet: run `sbt snapshot4sPromote`, then read it")
    }

  given SnapshotEq[String] = SnapshotEq.fromUniversalEquals[String]
}
