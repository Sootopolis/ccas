package ccas.utils.errors

class ExternalException(message: String) extends Exception(message)

extension (error: Throwable) def safeMessage: String = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
