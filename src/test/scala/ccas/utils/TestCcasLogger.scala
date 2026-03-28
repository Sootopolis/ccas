package ccas.utils

import zio.{Scope, UIO, URIO, ZIO}

object TestCcasLogger {
  val noop: CcasLogger = new CcasLogger {
    override def info(msg: String): UIO[Unit]  = ZIO.unit
    override def warn(msg: String): UIO[Unit]  = ZIO.unit
    override def error(msg: String): UIO[Unit] = ZIO.unit
    override def debug(msg: String): UIO[Unit] = ZIO.unit
    override def progressBar: URIO[Scope, ProgressBar] =
      ProgressDisplay.make(enabled = false).flatMap(_.addBarScoped)
  }

  /** Create a no-op progress bar (no Scope required). */
  val noopBar: UIO[ProgressBar] = ProgressDisplay.make(enabled = false).flatMap(_.addBar)
}
