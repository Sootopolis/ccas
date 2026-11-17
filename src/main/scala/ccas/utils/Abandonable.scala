package ccas.utils

import zio.{Task, ZIO}

/** A blocking read that an interrupt abandons instead of waiting for, as Ctrl-C at a question needs. */
object Abandonable {

  /** Runs `blocking` on a daemon thread of its own. A blocked stdin or terminal read ignores interruption, and at exit
    * ZIOApp waits for every interrupted fiber, so a fiber parked in the read itself would turn Ctrl-C into a wait for
    * Enter or for `gracefulShutdownTimeout` (#513). Here the fiber waits only on the callback, and an interrupt ends
    * it; the abandoned thread still takes the next line, which nothing reads once the command is exiting. Not
    * `.disconnect`: its read goes on in a root fiber, which ZIOApp's shutdown waits for just the same.
    */
  def run[A](blocking: => A): Task[A] =
    ZIO.async[Any, Throwable, A] { callback =>
      val read: Runnable = () => {
        // Read here, not inside `fromEither`, whose by-name argument would run the read back on the fiber. Every
        // throwable, fatal ones too, so the thread always calls back and the fiber can't wait forever.
        val result =
          try { Right(blocking) }
          catch { case thrown: Throwable => Left(thrown) }
        callback(ZIO.fromEither(result))
      }
      val reader = new Thread(read, "ccas-abandonable-read")
      reader.setDaemon(true)
      reader.start()
    }
}
