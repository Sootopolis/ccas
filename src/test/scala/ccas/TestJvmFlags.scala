package ccas

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

// Pins two of the three homes in docs/adr/0018-every-jvm-carries-the-same-two-flags.md (#231): the flags
// this JVM was started with, and the ones `.jvmopts` declares. The launcher's copy is not observable here.
object TestJvmFlags extends ZIOSpecDefault {

  private val Flags = List("--sun-misc-unsafe-memory-access=allow", "--enable-native-access=ALL-UNNAMED")

  private val args: List[String] = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toList

  private lazy val jvmopts: List[String] =
    Files.readAllLines(Path.of(".jvmopts")).asScala.toList.map(_.trim)

  override def spec: Spec[Any, Throwable] = suite("JvmFlags")(
    test("the test JVM was started with both flags") {
      assertTrue(Flags.forall(args.contains))
    },
    test(".jvmopts carries both flags for the JVMs the sbt script starts") {
      assertTrue(Flags.forall(jvmopts.contains))
    }
  )
}
