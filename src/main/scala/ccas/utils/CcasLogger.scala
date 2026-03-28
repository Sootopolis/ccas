package ccas.utils

import zio.{LogLevel, Scope, UIO, URIO, URLayer, ZIO, ZLayer}

trait CcasLogger {
  def info(msg: String): UIO[Unit]
  def warn(msg: String): UIO[Unit]
  def error(msg: String): UIO[Unit]
  def debug(msg: String): UIO[Unit]
  def progressBar: URIO[Scope, ProgressBar]
}

object CcasLogger {

  def info(msg: String): URIO[CcasLogger, Unit]  = ZIO.serviceWithZIO(_.info(msg))
  def warn(msg: String): URIO[CcasLogger, Unit]  = ZIO.serviceWithZIO(_.warn(msg))
  def error(msg: String): URIO[CcasLogger, Unit] = ZIO.serviceWithZIO(_.error(msg))
  def debug(msg: String): URIO[CcasLogger, Unit] = ZIO.serviceWithZIO(_.debug(msg))

  def progressBar: URIO[CcasLogger & Scope, ProgressBar] = ZIO.serviceWithZIO[CcasLogger](_.progressBar)

  def live(showProgress: Boolean = true, minLevel: LogLevel = LogLevel.Info): URLayer[Scope, CcasLogger] =
    ZLayer.scoped {
      for {
        display <- ProgressDisplay.make(enabled = showProgress)
        _       <- ZIO.addFinalizer(display.finishAll)
      } yield CcasLoggerLive(display, minLevel)
    }

  // ---------------------------------------------------------------------------
  // Private implementation
  // ---------------------------------------------------------------------------

  private val Reset  = "\u001b[0m"
  private val Cyan   = "\u001b[36m"
  private val Green  = "\u001b[32m"
  private val Yellow = "\u001b[33m"
  private val Red    = "\u001b[31m"

  private def colorFor(level: LogLevel): String = level match {
    case LogLevel.Debug   => Cyan
    case LogLevel.Info    => Green
    case LogLevel.Warning => Yellow
    case LogLevel.Error   => Red
    case LogLevel.Fatal   => Red
    case _                => Reset
  }

  private def format(level: LogLevel, msg: String): String = s"${colorFor(level)}[${level.label}]$Reset $msg"

  private case class CcasLoggerLive(display: ProgressDisplay, minLevel: LogLevel) extends CcasLogger {

    private def log(level: LogLevel, msg: String): UIO[Unit] =
      if (level.ordinal >= minLevel.ordinal) display.logAboveBars(format(level, msg))
      else ZIO.unit

    override def info(msg: String): UIO[Unit]  = log(LogLevel.Info, msg)
    override def warn(msg: String): UIO[Unit]  = log(LogLevel.Warning, msg)
    override def error(msg: String): UIO[Unit] = log(LogLevel.Error, msg)
    override def debug(msg: String): UIO[Unit] = log(LogLevel.Debug, msg)

    override def progressBar: URIO[Scope, ProgressBar] = display.addBarScoped
  }
}
