package ccas.utils.client

import zio.http.URL

class RateLimitedException(val url: URL) extends Exception(s"Rate limited (429) for: $url")
