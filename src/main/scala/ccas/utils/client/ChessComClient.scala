package ccas.utils.client

import ccas.utils.json.JsonDecodingException
import zio.http.{Client, Header, Headers, Request, URL}
import zio.http.Method.GET
import zio.json.JsonDecoder
import zio.{Chunk, Semaphore, Task, ZIO, ZLayer}

final class ChessComClient(client: Client, headers: Headers, semaphore: Semaphore) {
  private val batchedClient = client.batched

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = for {
    response <- batchedClient.request(Request(method = GET, url = url).addHeaders(headers))
    string <- response.body.asString
    value <- ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
  } yield { value }

  def getAll[T](urls: Iterable[URL])(using jsonDecoder: JsonDecoder[T]): Task[Chunk[T]] = Chunk.from(urls).mapZIO(get)

  def getWithPermit[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] =
    semaphore.withPermit(get(url))

  def getAllWithPermit[T](urls: Iterable[URL])(using jsonDecoder: JsonDecoder[T]): Task[Chunk[T]] =
    Chunk.from(urls).mapZIO(getWithPermit)
}

object ChessComClient {
  private def userAgentHeaders(contactEmail: String): Headers =
    Headers(Header.Custom("User-Agent", s"CCAS/1.0 (contact: $contactEmail)"))

  def live(permits: Long = 1, headers: Headers = Headers.empty): ZLayer[Client, Throwable, ChessComClient] =
    ZLayer.fromZIO {
      for {
        contactEmail <- ZIO.fromOption(Option(System.getenv("CCAS_CONTACT_EMAIL")))
                          .orElseFail(IllegalStateException("CCAS_CONTACT_EMAIL environment variable is required"))
        client       <- ZIO.service[Client]
        semaphore    <- Semaphore.make(permits)
      } yield ChessComClient(client, userAgentHeaders(contactEmail) ++ headers, semaphore)
    }
}
