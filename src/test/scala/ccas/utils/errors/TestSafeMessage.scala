package ccas.utils.errors

import java.io.IOException
import java.nio.file.{
  AccessDeniedException,
  DirectoryNotEmptyException,
  FileAlreadyExistsException,
  Files,
  FileSystemException,
  NoSuchFileException,
  NotDirectoryException,
  NotLinkException,
  Path
}

import zio.ZIO
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestSafeMessage extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("safeMessage")(
    test("an exception's message, or its simple class name when the message is null") {
      assertTrue(
        new IllegalStateException("no schedule").safeMessage == "no schedule",
        new NullPointerException().safeMessage == "NullPointerException"
      )
    },
    test("a file error the JDK left without a reason reads as the OS would put it") {
      assertTrue(
        new NoSuchFileException("missing.json").safeMessage == "missing.json: No such file or directory",
        new AccessDeniedException("/r/x.txt").safeMessage == "/r/x.txt: Permission denied",
        new FileAlreadyExistsException("/r/a", "/r/b", null).safeMessage == "/r/a -> /r/b: File exists",
        new DirectoryNotEmptyException("/r").safeMessage == "/r: Directory not empty",
        new NotDirectoryException("/r/x.txt").safeMessage == "/r/x.txt: Not a directory",
        new NoSuchFileException(null).safeMessage == "No such file or directory"
      )
    },
    test("a reasonless file error with no OS wording to give is named by its class") {
      assertTrue(new NotLinkException("/r/x.txt").safeMessage == "/r/x.txt: NotLinkException")
    },
    test("a file error that carries its own reason keeps it as given") {
      assertTrue(
        new FileSystemException("/r", null, "Read-only file system").safeMessage == "/r: Read-only file system"
      )
    },
    test("a real missing file reads as the OS would put it") {
      for {
        dir <- ZIO.attemptBlocking(Files.createTempDirectory("ccas-safe-message"))
        path = dir.resolve("missing.json")
        read <- ZIO.attemptBlocking(Files.readString(path)).either
        _    <- ZIO.attemptBlocking(Files.delete(dir))
      } yield assertTrue(read.left.map(_.safeMessage) == Left(s"$path: No such file or directory"))
    },
    test("a message naming a file error's own path names it once") {
      assertTrue(
        new AccessDeniedException("/r/x.txt").safeMessageNaming(Path.of("/r/x.txt")) == "/r/x.txt: Permission denied"
      )
    },
    test("a message naming a path the error doesn't carry puts it first") {
      assertTrue(
        new IOException("Is a directory").safeMessageNaming(Path.of("/r/ccas.env")) == "/r/ccas.env: Is a directory",
        new FileAlreadyExistsException("/r/archive").safeMessageNaming(Path.of("/r/x.txt")) ==
          "/r/x.txt: /r/archive: File exists"
      )
    },
    test("a move refused names its source once, then its target") {
      val refused = new AccessDeniedException("/r/x.txt", "/r/archive/x.txt", null)
      assertTrue(refused.safeMessageNaming(Path.of("/r/x.txt")) == "/r/x.txt -> /r/archive/x.txt: Permission denied")
    }
  )
}
