package ccas.utils.client

import io.netty.handler.codec.PrematureChannelClosureException
import zio.http.URL
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.utils.json.JsonDecodingException

object TestIsConnectionError extends ZIOSpecDefault {

  private val url: URL = URL.decode("https://api.chess.com/pub/player/x").toOption.get

  override def spec: Spec[Any, Nothing] = suite("ConnectionError.isConnectionError")(
    test("UnknownHostException (DNS 'Temporary failure in name resolution') is a connection error") {
      assertTrue(
        ConnectionError.isConnectionError(
          new java.net.UnknownHostException("api.chess.com: Temporary failure in name resolution")
        )
      )
    },
    test("generic IOException (Connection reset) is a connection error") {
      assertTrue(ConnectionError.isConnectionError(new java.io.IOException("Connection reset")))
    },
    test("PrematureChannelClosureException is a connection error") {
      assertTrue(ConnectionError.isConnectionError(new PrematureChannelClosureException()))
    },
    test("ConnectException (Connection refused) is a connection error (IOException subtype)") {
      assertTrue(ConnectionError.isConnectionError(new java.net.ConnectException("Connection refused")))
    },
    test("SocketException (Network is unreachable) is a connection error (IOException subtype)") {
      assertTrue(ConnectionError.isConnectionError(new java.net.SocketException("Network is unreachable")))
    },
    test("non-IOException whose message merely mentions name resolution is NOT a connection error (type-only)") {
      assertTrue(!ConnectionError.isConnectionError(new RuntimeException("Temporary failure in name resolution")))
    },
    test("HttpStatusException (500) is NOT a connection error") {
      assertTrue(!ConnectionError.isConnectionError(new HttpStatusException(500, url, "boom")))
    },
    test("ReportedNotFound (404) is NOT a connection error") {
      assertTrue(!ConnectionError.isConnectionError(new ReportedNotFound(url, "\"x\" not found.")))
    },
    test("JsonDecodingException is NOT a connection error even if its message looks network-y (type excluded first)") {
      assertTrue(!ConnectionError.isConnectionError(new JsonDecodingException("Connection reset")))
    },
    test("NetworkUnavailableException is NOT reclassified (no double-wrap)") {
      assertTrue(!ConnectionError.isConnectionError(new NetworkUnavailableException(new java.io.IOException("down"))))
    },
    test("generic RuntimeException with unrelated message is NOT a connection error") {
      assertTrue(!ConnectionError.isConnectionError(new RuntimeException("something went wrong")))
    },
    test("exception with null message is NOT a connection error") {
      assertTrue(!ConnectionError.isConnectionError(new RuntimeException()))
    }
  )
}
