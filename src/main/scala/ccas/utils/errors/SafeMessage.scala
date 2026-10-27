package ccas.utils.errors

/** `getMessage`, or the class's simple name when it is null. Safe against a null message only: whether a message is fit
  * to show a user is [[UserFacingError]]'s question.
  */
extension (error: Throwable) def safeMessage: String = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
