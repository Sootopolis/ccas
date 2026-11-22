package ccas.cli

import zio.{durationInt, Promise, Ref}
import zio.test.{assertTrue, Spec, TestClock, ZIOSpecDefault}

/** [[Attention]]'s reminders, on the `TestClock`, and the notification command it picks for each desktop. */
object TestAttention extends ZIOSpecDefault {

  private val title = "ccas recruit"
  private val text  = "2 found for team-alpha: confirm the invites"

  def spec: Spec[Any, Any] = suite("Attention")(
    test("the reminder repeats every interval while the wait lasts, and stops once it ends") {
      for {
        alerts   <- Ref.make(0)
        answered <- Promise.make[Nothing, Boolean]
        waiting  <- Attention.whileWaiting(alerts.update(_ + 1), 10.minutes, 6)(answered.await).fork
        _        <- TestClock.adjust(25.minutes)
        before   <- alerts.get
        _        <- answered.succeed(true)
        answer   <- waiting.join
        _        <- TestClock.adjust(30.minutes)
        after    <- alerts.get
      } yield assertTrue(before == 3, answer, after == 3)
    },
    test("the reminder stops after its last repeat, though the wait goes on") {
      for {
        alerts  <- Ref.make(0)
        never   <- Promise.make[Nothing, Boolean]
        waiting <- Attention.whileWaiting(alerts.update(_ + 1), 10.minutes, 2)(never.await).fork
        _       <- TestClock.adjust(60.minutes)
        sent    <- alerts.get
        done    <- waiting.poll
        _       <- waiting.interrupt
      } yield assertTrue(sent == 3, done.isEmpty)
    },
    test("macOS notifies through osascript, passing the title and text as arguments rather than as script") {
      val command = Attention.notifier(Desktop.Mac, title, text)
      assertTrue(
        command.flatMap(_.headOption).contains("osascript"),
        command.exists(_.takeRight(2) == List(title, text)),
        command.exists(_.dropRight(2).forall(part => !part.contains(text) && !part.contains(title)))
      )
    },
    test("a Linux desktop session notifies through notify-send") {
      val expected = Some(List("notify-send", "--", title, text))
      assertTrue(
        Attention.notifier(Desktop.Wayland, title, text) == expected,
        Attention.notifier(Desktop.X11, title, text) == expected
      )
    },
    test("a session with no desktop has no notifier") {
      assertTrue(Attention.notifier(Desktop.Headless, title, text).isEmpty)
    }
  )
}
