package ccas.utils.client

import ccas.utils.json.JsonDecodingException
import zio.http.Method.GET
import zio.http.{Client, Headers, Request, URL}
import zio.json.JsonDecoder
import zio.{Chunk, Semaphore, Task, ZIO, ZLayer}

final class CcasClient(client: Client, headers: Headers, semaphore: Semaphore) {
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

object CcasClient {
  def live(permits: Long = 1, headers: Headers = Headers.empty): ZLayer[Client, Nothing, CcasClient] =
    ZLayer.fromZIO {
      for {
        client    <- ZIO.service[Client]
        semaphore <- Semaphore.make(permits)
      } yield CcasClient(client, headers, semaphore)
    }
}
