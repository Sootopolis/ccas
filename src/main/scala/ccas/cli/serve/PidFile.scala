package ccas.cli.serve

import java.nio.charset.CharacterCodingException
import java.nio.file.{Files, NoSuchFileException, Path}

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

/** Pid-file helpers shared by the detached-serve parent ([[Detach]]), [[ServerStatus]], [[ServerList]] and
  * [[ServerStop]]. The pure pieces ([[parse]], [[portOf]], [[alreadyRunning]]) carry the decision logic so they can be
  * unit-tested without a real process or filesystem; the effectful reads/writes block, and the call sites put them on
  * ZIO's blocking pool.
  */
object PidFile {

  /** The pid file of the server on `port`, in `stateDir` (`XdgPaths.stateDir` outside tests). Keyed by port, since
    * that is what tells two servers on one machine apart (#326).
    */
  def path(stateDir: Path, port: Int): Path = stateDir.resolve(s"ccas-$port.pid")

  private val Name = """ccas-([1-9][0-9]*)\.pid""".r

  /** The port a pid file in the state directory belongs to, read from its name: the inverse of [[path]]. `None` for
    * any other file. Pure.
    */
  def portOf(fileName: String): Option[Int] = fileName match {
    case Name(digits) => digits.toIntOption.filter(HealthProbe.isPort)
    case _            => None
  }

  /** The ports that have a pid file in `stateDir`; none when the directory doesn't exist yet. */
  def ports(stateDir: Path): List[Int] =
    if (Files.exists(stateDir)) {
      Using.resource(Files.list(stateDir))(_.iterator.asScala.flatMap(p => portOf(p.getFileName.toString)).toList)
    } else { Nil }

  /** Parse pid-file content to a positive pid; `None` for blank or non-numeric content. Pure. */
  def parse(content: String): Option[Long] = content.trim.toLongOption.filter(_ > 0)

  /** Read and parse the pid file; `None` if the file is absent or its content isn't a positive integer. */
  def read(p: Path): Option[Long] =
    if (Files.exists(p)) parse(Files.readString(p)) else None

  /** What a pid file says about its server. */
  enum State {
    case Live(pid: Long)
    case Dead(pid: Long)
    case Corrupt
    case Unreadable
    case Absent
  }

  /** What the pid file at `p` says, from a single read, so a file removed by a stopping server reads as absent rather
    * than corrupt. Content that isn't text is [[State.Corrupt]] like any other that isn't a pid. A file that can't be
    * read is [[State.Unreadable]], not an exception: one bad pid file mustn't hide the rest from `ccas server list`.
    */
  def state(p: Path): State =
    Try(Files.readString(p)) match {
      case Success(content) =>
        parse(content).fold(State.Corrupt) { pid =>
          if (isAlive(pid)) { State.Live(pid) }
          else { State.Dead(pid) }
        }
      case Failure(_: NoSuchFileException)      => State.Absent
      case Failure(_: CharacterCodingException) => State.Corrupt
      case Failure(_)                           => State.Unreadable
    }

  /** True if a live OS process currently has this pid. */
  def isAlive(pid: Long): Boolean = ProcessHandle.of(pid).filter(_.isAlive).isPresent

  /** Decide whether a detached server is already running: the recorded pid, only when it maps to a live process.
    * `alive` is injected so the branch logic is unit-testable. Pure.
    */
  def alreadyRunning(pidOption: Option[Long], alive: Long => Boolean): Option[Long] = pidOption.filter(alive)

  /** Best-effort delete; absent file is fine. */
  def remove(p: Path): Unit = { Files.deleteIfExists(p); () }
}
