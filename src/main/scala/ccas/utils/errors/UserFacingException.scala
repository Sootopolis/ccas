package ccas.utils.errors

class UserFacingException(message: String) extends Exception(message)
class BadRequestException(message: String) extends UserFacingException(message)
class NotFoundException(message: String)   extends UserFacingException(message)

extension (error: Throwable) def safeMessage: String = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
