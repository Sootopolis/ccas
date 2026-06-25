package ccas.cli

import zio.ExitCode
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests the [[UseClub]] guard that doesn't touch the filesystem: a blank slug is rejected (exit 2) without writing
  * config. The happy path (which writes the real config file) is covered indirectly via `TestConfigWriter`. */
object TestUseClub extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestUseClub")(
    test("blank slug is rejected with exit 2 (no write)") {
      UseClub.run("   ").map(code => assertTrue(code == ExitCode(2)))
    }
  )
}
