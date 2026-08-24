package ccas.utils.client

import java.util.logging.{Filter, Level, LogRecord, Logger as JLogger}

import io.netty.channel.DefaultChannelPipeline
import io.netty.handler.codec.PrematureChannelClosureException

/** Drops one Netty WARN — `PrematureChannelClosureException: channel gone inactive with N missing response(s)`, logged
  * by `DefaultChannelPipeline`'s tail. It is a second exception for a channel whose failure we have already counted,
  * written to `api_fetch_failure` and retried, and nothing downstream reads it. It is also uncatchable: Netty defers
  * `fireChannelInactive`, and by the time it runs `ZClient`'s `resetChannel` has removed `ClientFailureHandler`, the
  * per-request handler that terminates `exceptionCaught` (`ClientInboundHandler` goes with it but only ever re-fires).
  * Background in #225.
  *
  * A JUL filter and not a `ZLogger` one because Netty logs this itself, outside ZIO, so it prints through the progress
  * bars; `ProgressDisplay.isBenignReadIdleReap` is the same idea one layer up.
  *
  * A `Filter` and not `setLevel(SEVERE)`: `DefaultChannelPipeline` never logs at SEVERE, and on a channel idle in
  * zio-http's pool both per-request handlers are gone, so the tail is the only place an `SSLException` or an
  * `OutOfDirectMemoryError` could still surface. The match is over-specified for the same reason and fails ''open'' —
  * level, type and the whole message must hold, so a Netty reword brings the harmless noise back rather than hiding
  * anything. Two near misses are deliberately kept: zio-http's identically-typed "Channel closed while executing the
  * request…", and any count above one, which no path we run should be able to reach.
  *
  * Exit condition: zio-http hardcodes `HttpClientCodec(failOnMissingResponse = true)` in `NettyConnectionPool`; delete
  * this when that changes.
  */
object NettyTailNoise {

  private val PipelineLoggerName = classOf[DefaultChannelPipeline].getName

  private val MissingResponseMessage = "channel gone inactive with 1 missing response(s)"

  // Pinned in a val: `LogManager` holds only a WeakReference to the loggers it hands out, so a filter set on a logger
  // nobody else references is discarded at the next GC.
  private val pipelineLogger: JLogger = JLogger.getLogger(PipelineLoggerName)

  // JUL gives a logger a single filter slot, which may already be spoken for (a `-Djava.util.logging.config.file`, an
  // embedding host). Chain onto whoever held it rather than dropping their records silently.
  private val displaced: Option[Filter] = Option(pipelineLogger.getFilter)

  // Keyed on the throwable, not the record: the record's own message is Netty's generic tail text, shared with every
  // unhandled exception that reaches it. `Level` compares by int value, so spell that out rather than implying identity.
  private[client] def isMissingResponseNoise(record: LogRecord): Boolean =
    record.getLevel.intValue == Level.WARNING.intValue && (record.getThrown match {
      case e: PrematureChannelClosureException => Option(e.getMessage).contains(MissingResponseMessage)
      case _                                   => false
    })

  private val filter: Filter = (record: LogRecord) =>
    displaced.forall(_.isLoggable(record)) && !isMissingResponseNoise(record)

  /** Idempotent, and order-free: JUL consults a filter per log call, so this need not run before Netty initialises its
    * logger. Takes the logger's single JUL filter slot, unscoped, for the process lifetime.
    */
  def install(): Unit = pipelineLogger.setFilter(filter)
}
