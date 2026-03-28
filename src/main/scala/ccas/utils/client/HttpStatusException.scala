package ccas.utils.client

import zio.http.URL

class HttpStatusException(val statusCode: Int, val url: URL, val responseBody: String)
    extends Exception(s"HTTP $statusCode for: $url")
