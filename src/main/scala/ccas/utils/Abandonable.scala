package ccas.utils

import zio.{Promise, Ref, Runtime, Task, UIO, Unsafe, ZIO}

/** A blocking read that an interrupt abandons instead of waiting for, as Ctrl-C at a question needs. */
object Abandonable {

  /** Runs `blocking` on a daemon thread of its own. A blocked stdin or terminal read ignores interruption, and at exit
    * ZIOApp waits for every interrupted fiber, so a fiber parked in the read itself would turn Ctrl-C into a wait for
    * Enter or for `gracefulShutdownTimeout` (#513). Here the fiber waits only on the callback, and an interrupt ends
    * it; the abandoned thread still takes the next line, and gives it to nobody, so a caller that must not lose one
    * reads through [[keeping]]. Not `.disconnect`: its read goes on in a root fiber, which ZIOApp's shutdown waits for
    * just the same.
    */
  def run[A](blocking: => A): Task[A] =
    ZIO.async[Any, Throwable, A](callback => onThread(blocking)(result => callback(ZIO.fromEither(result))))

  /** Reads `blocking` as [[run]] does, except that a read whose asker is interrupted is kept, and its line goes to the
    * next asker instead of to nobody (#514). So one read at most is ever under way, and each line goes to one asker.
    */
  def keeping[A](blocking: => A): UIO[Task[A]] =
    for {
      runtime  <- ZIO.runtime[Any]
      underWay <- Ref.Synchronized.make(Option.empty[Promise[Throwable, A]])
    } yield {
      def next: Task[A] =
        for {
          // The thread completes the read's promise itself, so no fiber stands for the read: nothing can interrupt
          // it, and nothing is left for shutdown to wait on. Uninterruptible, so no read is started without being kept.
          read <- underWay.modifyZIO {
            case kept @ Some(read) => ZIO.succeed((read, kept))
            case None =>
              for {
                read <- Promise.make[Throwable, A]
                _    <- ZIO.succeed(onThread(blocking)(result => complete(runtime, read, result)))
              } yield (read, Some(read))
          }.uninterruptible
          // Only an asker that has the read's outcome takes it, and none can be interrupted between the two. An
          // asker whose read another took, as two at once would, reads again.
          line <- ZIO.uninterruptibleMask { restore =>
            for {
              outcome <- restore(read.await.exit)
              taken <- underWay.modify {
                case Some(kept) if kept eq read => (true, None)
                case other                      => (false, other)
              }
              line <-
                if (taken) { outcome }
                else { restore(next) }
            } yield line
          }
        } yield line
      next
    }

  // From the reader thread, which has no fiber to complete it from.
  private def complete[A](runtime: Runtime[Any], read: Promise[Throwable, A], result: Either[Throwable, A]): Unit =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(read.complete(ZIO.fromEither(result)))
      ()
    }

  // Reads before delivering, so the read runs on this thread and not wherever `deliver` would evaluate it. Catches
  // every throwable, fatal ones too, so the thread always delivers and nothing waits on it forever.
  private def onThread[A](blocking: => A)(deliver: Either[Throwable, A] => Unit): Unit = {
    val read: Runnable = () => {
      val result =
        try { Right(blocking) }
        catch { case thrown: Throwable => Left(thrown) }
      deliver(result)
    }
    val reader = new Thread(read, "ccas-abandonable-read")
    reader.setDaemon(true)
    reader.start()
  }
}
