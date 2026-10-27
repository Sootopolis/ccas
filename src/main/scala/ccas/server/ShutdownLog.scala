package ccas.server

import zio.{Clock, Duration, Ref, Scope, UIO, ZIO, ZLayer}

import ccas.utils.ProgressDisplay

/** The server's stop, logged as it goes: each step as it starts and again as it ends, with how long it took. A step
  * that hangs is then the one whose `Stopping` line has no `Stopped` after it, even once the time limit has cut the JVM
  * off (#316). Most steps run one after another; layers built side by side are released side by side, so their lines
  * interleave. Only built steps are logged: a stop during boot, before zio-http's `Server started`, names none still
  * building.
  */
private[server] object ShutdownLog {

  /** `acquire`, with its release logged as a step. Finalizers run last-registered first, so the one registered after
    * `acquire` opens the step and the one registered before it closes it.
    */
  def step[R, E, A](name: String)(acquire: ZIO[R & Scope, E, A]): ZIO[R & Scope, E, A] =
    for {
      stopwatch <- Stopwatch.make
      _         <- ZIO.addFinalizer(stopwatch.stop(s"Stopped $name"))
      a         <- acquire
      _         <- ZIO.addFinalizer(stopwatch.start(s"Stopping $name"))
    } yield a

  /** `underlying`, with its release logged as a step. */
  def layer[RIn, E, A](name: String)(underlying: ZLayer[RIn, E, A]): ZLayer[RIn, E, A] =
    ZLayer.scopedEnvironment[RIn](step(name)(underlying.build))

  /** Times a stretch of the stop, from the line that opens it to the line that closes it, which ends with how long it
    * took. One never started closes with no line: a step whose acquisition failed had nothing to stop, and the whole
    * stop is timed only from a running server, so a boot that fails or is stopped part-way logs just the steps it built.
    */
  final class Stopwatch private (startedRef: Ref[Option[Long]]) {
    def start(line: String): UIO[Unit] =
      for {
        now <- Clock.nanoTime
        _   <- startedRef.set(Some(now))
        _   <- log(line)
      } yield ()

    def stop(line: String): UIO[Unit] =
      for {
        startedOption <- startedRef.get
        now           <- Clock.nanoTime
        _ <- ZIO.foreachDiscard(startedOption) { started =>
          // Whole milliseconds, where `Duration#render` would give most steps, which take under one, in nanoseconds.
          log(s"$line in ${Duration.fromNanos(now - started).toMillis} ms")
        }
      } yield ()
  }

  object Stopwatch {
    def make: UIO[Stopwatch] = Ref.make(Option.empty[Long]).map(new Stopwatch(_))
  }

  private def log(line: String): UIO[Unit] = ProgressDisplay.sourced("shutdown")(ZIO.logInfo(line))
}
