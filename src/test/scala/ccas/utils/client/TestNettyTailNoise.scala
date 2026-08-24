package ccas.utils.client

import java.util.logging.{Level, LogRecord, Logger as JLogger}

import io.netty.channel.DefaultChannelPipeline
import io.netty.handler.codec.PrematureChannelClosureException
import io.netty.util.internal.logging.{InternalLoggerFactory, JdkLoggerFactory}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

object TestNettyTailNoise extends ZIOSpecDefault {

  private val TailMessage =
    "An exceptionCaught() event was fired, and it reached at the tail of the pipeline. It usually means the last " +
      "handler in the pipeline did not handle the exception."

  private def record(level: Level, thrown: Option[Throwable]): LogRecord = {
    val r = new LogRecord(level, TailMessage)
    thrown.foreach(r.setThrown)
    r
  }

  private def nettyMissingResponse: LogRecord =
    record(
      Level.WARNING,
      Some(new PrematureChannelClosureException("channel gone inactive with 1 missing response(s)"))
    )

  override def spec: Spec[Any, Throwable] = suite("NettyTailNoise")(
    test("drops Netty's 'channel gone inactive with 1 missing response(s)' WARN") {
      assertTrue(NettyTailNoise.isMissingResponseNoise(nettyMissingResponse))
    },
    test("keeps zio-http's own PrematureChannelClosureException — that one names a request that really failed") {
      val r = record(
        Level.WARNING,
        Some(
          new PrematureChannelClosureException(
            "Channel closed while executing the request. This is likely caused due to a client connection misconfiguration"
          )
        )
      )
      assertTrue(!NettyTailNoise.isMissingResponseNoise(r))
    },
    test("keeps a PrematureChannelClosureException with no message") {
      val r = record(Level.WARNING, Some(new PrematureChannelClosureException()))
      assertTrue(!NettyTailNoise.isMissingResponseNoise(r))
    },
    // A count above one would mean a response went missing on a path we don't run (zio-http doesn't pipeline), so it
    // is new information rather than the echo this filter exists to drop.
    test("keeps a count above one") {
      val r = record(
        Level.WARNING,
        Some(new PrematureChannelClosureException("channel gone inactive with 2 missing response(s)"))
      )
      assertTrue(!NettyTailNoise.isMissingResponseNoise(r))
    },
    test("keeps another exception type carrying the same message") {
      val r = record(Level.WARNING, Some(new RuntimeException("channel gone inactive with 1 missing response(s)")))
      assertTrue(!NettyTailNoise.isMissingResponseNoise(r))
    },
    test("keeps a record with no throwable") {
      assertTrue(!NettyTailNoise.isMissingResponseNoise(record(Level.WARNING, None)))
    },
    test("keeps the same throwable logged below WARNING") {
      val r = record(
        Level.FINE,
        Some(new PrematureChannelClosureException("channel gone inactive with 1 missing response(s)"))
      )
      assertTrue(!NettyTailNoise.isMissingResponseNoise(r))
    },
    // The two ways this feature silently becomes a no-op — a wrong logger name, or the layer dropping the install —
    // are invisible to the predicate tests above, since JUL consults a filter only on the logger it is set on.
    test("HttpClientLayer.live installs the filter on Netty's pipeline logger") {
      val logger = JLogger.getLogger(classOf[DefaultChannelPipeline].getName)
      val prev   = Option(logger.getFilter)
      logger.setFilter(null)
      ZIO
        .scoped(HttpClientLayer.live.build)
        .map { _ =>
          val installed = Option(logger.getFilter)
          assertTrue(
            installed.exists(f => !f.isLoggable(nettyMissingResponse)),
            installed.exists(_.isLoggable(record(Level.WARNING, Some(new RuntimeException("boom")))))
          )
        }
        // Restores a filter that was already there, but leaves ours in place otherwise: `install` is documented to
        // hold the slot for the process, and this suite shares its JVM with every other one.
        .ensuring(ZIO.succeed(prev.foreach(logger.setFilter)))
    },
    // A real SLF4J binding (or a transitive `log4j-core`) routes Netty away from JUL and silently retires the filter;
    // nothing else in the build catches that.
    test("Netty resolves its logger factory to JUL — the premise the filter rests on") {
      assertTrue(InternalLoggerFactory.getDefaultFactory.isInstanceOf[JdkLoggerFactory])
    }
    // One test mutates the process-global JUL filter slot; ZIO Test runs a suite's tests in parallel by default.
  ) @@ TestAspect.sequential
}
