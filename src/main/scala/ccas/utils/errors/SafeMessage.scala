package ccas.utils.errors

import java.nio.file.{
  AccessDeniedException,
  DirectoryNotEmptyException,
  FileAlreadyExistsException,
  FileSystemException,
  NoSuchFileException,
  NotDirectoryException,
  Path
}

extension (error: Throwable) {

  /** `getMessage`, or the class's simple name when it is null; a file error the JDK left without a reason gets one
    * (below). Safe against a null message only: whether a message is fit to show a user is [[UserFacingError]]'s
    * question.
    *
    * For the commonest file errors the JDK moves the reason into the exception's class and leaves a message that is
    * only the path. Those get the operating system's words back, in the JDK's own `path: reason` form: `missing.json:
    * No such file or directory`.
    */
  def safeMessage: String = error match {
    case e: FileSystemException if e.getReason == null =>
      val reason = reasonOf(e)
      Option(e.getMessage).fold(reason)(paths => s"$paths: $reason")
    case _ => Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
  }

  /** [[safeMessage]] as `path: reason`, naming `path` once: a JDK file error about `path` already starts with it, and
    * a message that names `path` itself first would name it twice (#374).
    */
  def safeMessageNaming(path: Path): String = error match {
    case e: FileSystemException if e.getFile == path.toString => error.safeMessage
    case _                                                    => s"$path: ${error.safeMessage}"
  }
}

// strerror's wording for the errno each class stands for (sun.nio.fs.UnixException, UnixFileSystemProvider).
private def reasonOf(e: FileSystemException): String = e match {
  case _: NoSuchFileException        => "No such file or directory"
  case _: AccessDeniedException      => "Permission denied"
  case _: FileAlreadyExistsException => "File exists"
  case _: DirectoryNotEmptyException => "Directory not empty"
  case _: NotDirectoryException      => "Not a directory"
  case _                             => e.getClass.getSimpleName
}
