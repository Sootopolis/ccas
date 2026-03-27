package ccas.utils.client

import java.time.Instant

import com.augustnagro.magnum.Transactor
import com.typesafe.config.ConfigFactory
import zio.{durationInt, durationLong, Chunk, Duration, Ref, Schedule, Semaphore, Task, ZEnvironment, ZIO, ZLayer}
import zio.http.{Client, Header, Headers, Request, Status, URL, ZClientAspect}
import zio.http.Method.GET
import zio.json.JsonDecoder

import ccas.analysis.tables.ApiFetchFailure
import ccas.info.BuildInfo

import ccas.utils.json.JsonDecodingException

final class ChessComClient(
  client: Client,
  transactor: Transactor,
  headers: Headers,
  semaphore: Semaphore,
  mutex: Semaphore,
  throttled: Ref[Boolean],
  cooldown: Duration,
  retryBase: Duration = 1.second
) {
  private val batchedClient = (client @@ ZClientAspect.followRedirects(3) { (_, message) =>
    ZIO.fail(Exception(s"Redirect failed: $message"))
  }).batched

  private def rawGet[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = for {
    response <- batchedClient(Request(method = GET, url = url).addHeaders(headers))
    _ <- ZIO.whenDiscard(response.status == Status.TooManyRequests)(activateThrottle)
    _ <- ZIO.whenDiscard(!response.status.isSuccess)(
      ZIO.fail(HttpStatusException(response.status.code, url))
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
    acquireAndCall.retry(retrySchedule).tapError { error =>
      ApiFetchFailure
        .insert(ApiFetchFailure(url.encode, error.getClass.getSimpleName, Option(error.getMessage), Instant.now()))
        .provideEnvironment(ZEnvironment(transactor))
        .ignore
    }
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
      case e: HttpStatusException => e.statusCode == 429 || e.statusCode == 403
      case _                      => false
    }
}

object ChessComClient {
  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(Header.Custom("User-Agent", s"${BuildInfo.name.toUpperCase}/${BuildInfo.version} (contact: $contactEmail)"))

  def live: ZLayer[Client & Transactor, Throwable, ChessComClient] =
    ZLayer.fromZIO {
      val config   = ConfigFactory.load().getConfig("chess-com-client")
      val permits  = config.getLong("permits")
      val cooldown = config.getLong("cooldown-seconds").seconds
      for {
        contactEmail <- ZIO.attempt(config.getString("contact-email"))
        client       <- ZIO.service[Client]
        transactor   <- ZIO.service[Transactor]
        semaphore    <- Semaphore.make(permits)
        mutex        <- Semaphore.make(1)
        throttled    <- Ref.make(false)
      } yield ChessComClient(client, transactor, userAgentHeaders(contactEmail), semaphore, mutex, throttled, cooldown)
    }
}
