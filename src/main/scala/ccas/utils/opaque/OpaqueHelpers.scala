package ccas.utils.opaque

private[opaque] object OpaqueHelpers {
  extension [T](e: Either[String, T]) {
    def orThrowDbRead(name: String): T = e.fold(
      msg => throw new IllegalStateException(s"Invalid $name in database: $msg"),
      identity
    )
  }
}
