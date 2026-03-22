package ccas.utils.client

import zio.{durationInt, Chunk, Duration, Ref, Schedule, Semaphore, Task, ZIO, ZLayer}
import zio.http.{Client, Header, Headers, Request, Status, URL}
import zio.http.Method.GET
import zio.json.JsonDecoder

import ccas.info.BuildInfo
import ccas.utils.errors.ExternalException
import ccas.utils.json.JsonDecodingException

final class ChessComClient(
  client: Client,
  headers: Headers,
  semaphore: Semaphore,
  mutex: Semaphore,
  throttled: Ref[Boolean],
  cooldown: Duration,
  retryBase: Duration = 1.second
) {
  private val batchedClient = client.batched

  private def rawGet[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = for {
    response <- batchedClient(Request(method = GET, url = url).addHeaders(headers))
    _ <- ZIO.whenDiscard(response.status == Status.TooManyRequests)(
      activateThrottle *> ZIO.fail(RateLimitedException(url))
    )
    _ <- ZIO.whenDiscard(!response.status.isSuccess)(
      ZIO.fail(ExternalException(s"HTTP ${response.status.code} for $url"))
    )
    string <- response.body.asString
    value  <- ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
  } yield value

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = {
    val acquireAndCall = for {
      isThrottled <- throttled.get
      permit =
        if (isThrottled) { mutex }
        else { semaphore }
      result <- permit.withPermit(rawGet(url))
    } yield result
    acquireAndCall.retry(retrySchedule)
  }

  def getAll[T](urls: Iterable[URL])(using jsonDecoder: JsonDecoder[T]): Task[Chunk[T]] =
    ZIO.foreachPar(Chunk.from(urls))(get)

  // Fiber safety: getAndSet is atomic, so only the first 429 forks a cooldown fiber.
  // Subsequent 429s during the cooldown see wasThrottled=true and skip the fork.
  private def activateThrottle: Task[Unit] =
    throttled.getAndSet(true).flatMap { wasThrottled =>
      ZIO.unlessDiscard(wasThrottled)(throttled.set(false).delay(cooldown).forkDaemon.unit)
    }

  private val retrySchedule: Schedule[Any, Throwable, Any] =
    Schedule.exponential(retryBase) && Schedule.recurs(4) && Schedule.recurWhile[Throwable] {
      case _: RateLimitedException => true
      case _                       => false
    }
}

object ChessComClient {
  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"))

  def live(
    permits: Long = 5,
    cooldown: Duration = 30.seconds
  ): ZLayer[Client, Throwable, ChessComClient] =
    ZLayer.fromZIO {
      for {
        contactEmail <- ZIO.fromOption(Option(System.getenv("CCAS_CONTACT_EMAIL")))
          .orElseFail(IllegalStateException("CCAS_CONTACT_EMAIL environment variable is required"))
        client    <- ZIO.service[Client]
        semaphore <- Semaphore.make(permits)
        mutex     <- Semaphore.make(1)
        throttled <- Ref.make(false)
      } yield ChessComClient(client, userAgentHeaders(contactEmail), semaphore, mutex, throttled, cooldown)
    }
}
