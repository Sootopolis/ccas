package ccas.server

import zio.{duration2DurationOps, durationInt, Clock, Duration, UIO, ZIO, ZLayer}

import ccas.utils.ProgressDisplay

/** The server's start, logged as it goes, as [[ShutdownLog]] logs its stop: a step that hangs is the one whose `Starting`
  * line has no `Started` after it. Layers that don't depend on each other are built side by side, so their lines
  * interleave, and one that fails leaves those it interrupts open too: the error says which failed. A boot can wait for
  * minutes where a stop has a time limit, so [[boot]] also says it is still alive (#437).
  */
private[server] object BootLog {

  private val StillStartingInterval = 30.seconds

  /** `acquire`, logged as a step. */
  def step[R, E, A](name: String)(acquire: ZIO[R, E, A]): ZIO[R, E, A] =
    for {
      _            <- log(s"Starting $name")
      (elapsed, a) <- acquire.timed
      _            <- log(s"Started $name in ${renderTime(elapsed)}")
    } yield a

  /** `underlying`, with its build logged as a step, which starts only once the layers it needs are built. */
  def layer[RIn, E, A](name: String)(underlying: ZLayer[RIn, E, A]): ZLayer[RIn, E, A] =
    ZLayer.scopedEnvironment[RIn](step(name)(underlying.build))

  /** [[bootEvery]] at the server's own interval. */
  def boot[R, E, A](run: UIO[Unit] => ZIO[R, E, A]): ZIO[R, E, A] = bootEvery(StillStartingInterval)(run)

  /** The whole boot. `run` calls the effect it is given once the server has started; until then, or until `run` ends,
    * a line at each `interval` says the boot is still alive. The line doesn't blame the database, which not every step
    * waits on.
    */
  private[server] def bootEvery[R, E, A](interval: Duration)(run: UIO[Unit] => ZIO[R, E, A]): ZIO[R, E, A] =
    for {
      startedAt <- Clock.nanoTime
      _         <- log("Starting up")
      notices   <- stillStarting(interval).fork
      started = notices.interrupt *> Clock.nanoTime.flatMap { now =>
        log(s"Started in ${renderTime(Duration.fromNanos(now - startedAt))}")
      }
      a <- run(started).ensuring(notices.interrupt)
    } yield a

  /** Milliseconds under a second, where `Duration#render` would give a fast step in nanoseconds; whole seconds above. */
  private[server] def renderTime(elapsed: Duration): String =
    if (elapsed.toMillis < 1000) s"${elapsed.toMillis} ms"
    else Duration.fromSeconds(elapsed.toSeconds).render

  private def stillStarting(interval: Duration): UIO[Unit] =
    ZIO
      .iterate(interval)(_ => true) { waited =>
        ZIO.sleep(interval) *> log(s"Still starting after ${waited.render}").as(waited + interval)
      }
      .unit

  private def log(line: String): UIO[Unit] = ProgressDisplay.sourced("boot")(ZIO.logInfo(line))
}
