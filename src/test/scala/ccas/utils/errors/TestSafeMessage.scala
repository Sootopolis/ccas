package ccas.utils.errors

import java.nio.file.NoSuchFileException

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestSafeMessage extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("safeMessage")(
    test("an exception's message, or its simple class name when the message is null") {
      assertTrue(
        new NoSuchFileException("/tmp/report.txt").safeMessage == "/tmp/report.txt",
        new NullPointerException().safeMessage == "NullPointerException"
      )
    }
  )
}
