package ccas.utils.json

class JsonDecodingException(message: String) extends Exception(message) {
  def this(throwable: Throwable) = this(throwable.getMessage)
}
