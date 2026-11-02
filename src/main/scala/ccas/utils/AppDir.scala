package ccas.utils

import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import zio.{Task, ZIO}

/** A directory CCAS writes to: the operator's setting at `configKey` when there is one, else `<XDG base>/ccas/<leaf>`.
  * Resolved in code because a relative HOCON literal lands wherever the process was launched (#344). Unless set, every
  * process on a machine shares each one, whatever database it serves: see docs/adr/0024-one-directory-per-machine.md.
  */
final case class AppDir(configKey: String, overrideVar: String, base: AppDir.XdgBase, leaf: String) {

  /** `configured` when non-blank, since `Paths.get("")` is the launch directory, else `<leaf>` under
    * [[AppDir.XdgBase.ccasDir]]. With no base to resolve against it is a `Left` naming the variable to set, never a
    * guess. A configured relative path is returned as given.
    */
  def resolve(
    configured: Option[String],
    xdgHome: Option[String],
    userHome: Option[String]
  ): Either[String, Path] =
    AppDir.nonBlank(configured) match {
      case Some(path) => Right(Paths.get(path))
      case None =>
        base
          .ccasDir(xdgHome, userHome)
          .toRight(s"Cannot resolve $configKey: ${base.noAbsolutePath}. Set $overrideVar to an absolute path.")
          .map(_.resolve(leaf))
    }

  /** [[resolve]] of `configured` against this process's environment. */
  def fromEnvironment(configured: Option[String]): Either[String, Path] =
    resolve(configured = configured, xdgHome = base.xdgHomeHere, userHome = AppDir.userHomeHere)

  /** [[resolve]] against this process's config and environment. Not absolutised, so a caller can tell a relative
    * setting from the default.
    */
  def fromConfig: Task[Path] =
    ZIO
      .attempt {
        val config = ConfigFactory.load()
        fromEnvironment(Option.when(config.hasPath(configKey))(config.getString(configKey)))
      }
      .flatMap(resolved => ZIO.fromEither(resolved).mapError(new IllegalArgumentException(_)))
}

object AppDir {

  enum XdgBase(val envVar: String, val homeRelative: String) {
    case Cache  extends XdgBase("XDG_CACHE_HOME", ".cache")
    case Config extends XdgBase("XDG_CONFIG_HOME", ".config")
    case Data   extends XdgBase("XDG_DATA_HOME", ".local/share")
    case State  extends XdgBase("XDG_STATE_HOME", ".local/state")

    /** `$<envVar>/ccas`, else `$HOME/<homeRelative>/ccas`. A blank counts as absent, and so does a relative base: an XDG
      * variable (as the XDG spec requires) or a `user.home` such as the JDK's `?` for a user with no home.
      * `CompletionEmitter`'s helpers and `scripts/backup-neon.sh` repeat this for the files they read: change them too.
      */
    def ccasDir(xdgHome: Option[String], userHome: Option[String]): Option[Path] =
      absolute(xdgHome)
        .orElse(absolute(userHome).map(home => s"$home/$homeRelative"))
        .map(root => Paths.get(root, "ccas"))

    /** [[ccasDir]] against this process's environment, a `Left` naming the variable to set. */
    def ccasDirHere: Either[String, Path] =
      ccasDir(xdgHome = xdgHomeHere, userHome = userHomeHere)
        .toRight(s"$noAbsolutePath. Set $envVar to an absolute path.")

    private[AppDir] def noAbsolutePath: String =
      s"neither $envVar nor the user.home JVM property is an absolute path"

    private[AppDir] def xdgHomeHere: Option[String] = Option(System.getenv(envVar))
  }

  private def userHomeHere: Option[String] = Option(System.getProperty("user.home"))

  private def nonBlank(value: Option[String]): Option[String] = value.map(_.trim).filter(_.nonEmpty)

  private def absolute(value: Option[String]): Option[String] = nonBlank(value).filter(Paths.get(_).isAbsolute)
}
