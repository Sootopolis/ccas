package ccas.api.misc

import ccas.utils.json.JsonDecoding
import zio.http.{Client, Request, URL}
import zio.{RIO, Semaphore, Unsafe}

object CcasApiClient {
  val semaphore: Semaphore = Semaphore.unsafe.make(1)(Unsafe)

  private def getUnsafe(url: URL) = Client.batched(Request.get(url))

  def get[T: JsonDecoding](url: URL): RIO[Client, T] =
    semaphore.withPermit(getUnsafe(url))
      .mapError(e => ApiErrorMessage(e.getMessage))
      .flatMap(_.body.asString).flatMap(_.decodeJsonZIO)
}
