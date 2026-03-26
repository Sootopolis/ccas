package ccas.utils.client

import zio.http.URL

class HttpStatusException(val statusCode: Int, val url: URL) extends Exception(s"HTTP $statusCode for: $url")
