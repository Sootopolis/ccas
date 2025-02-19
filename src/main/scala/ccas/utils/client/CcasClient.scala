package ccas.utils.client

import ccas.utils.json.JsonDecodingException
import zio.http.Method.GET
import zio.http.{Client, Headers, Request, URL, ZClient}
import zio.json.JsonDecoder
import zio.{Chunk, RIO, Scope, Task, ZIO}

final class CcasClient(client: Client, headers: Headers) {
  private val batchedClient = client.batched

  def get[T](url: URL)(using jsonDecoder: JsonDecoder[T]): Task[T] = for {
    response <- batchedClient.request(Request(method = GET, url = url).addHeaders(headers))
    string <- response.body.asString
    value <- ZIO.fromEither(jsonDecoder.decodeJson(string)).mapError(JsonDecodingException(_))
  } yield { value }

  def getAll[T](urls: Iterable[URL])(using jsonDecoder: JsonDecoder[T]): Task[Chunk[T]] = Chunk.from(urls).mapZIO(get)
}

object CcasClient {
  def create(headers: Headers = Headers.empty): RIO[Client, CcasClient] =
    ZIO.serviceWith[Client](CcasClient(_, headers))
}
