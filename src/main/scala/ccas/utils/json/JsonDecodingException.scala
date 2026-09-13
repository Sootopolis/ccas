package ccas.utils.json

/** `responseBody` is the text that failed to decode, kept so `api_fetch_failure` can record what did not parse. */
class JsonDecodingException(message: String, val responseBody: Option[String]) extends Exception(message)
