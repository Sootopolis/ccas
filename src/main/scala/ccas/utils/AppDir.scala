package ccas.utils

import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import zio.{Task, ZIO}

/** A directory CCAS writes to: the operator's setting at `configKey` when there is one, else `<XDG base>/ccas/<leaf>`.
  * Resolved in code because a relative HOCON literal lands wherever the process was launched (#344). Unless set, every
  * process on a machine shares each one, whatever database it serves: see docs/adr/0024-one-directory-per-machine.md.
  */
final case class AppDir(configKey: String, overrideVar: String, base: AppDir.XdgBase, leaf: String) {

  /** `configured` when non-blank, else `$<base.envVar>/ccas/<leaf>`, else `$HOME/<base.homeRelative>/ccas/<leaf>`. A
    * blank counts as absent, since `Paths.get("")` is the launch directory, and so does a relative base: an XDG
    * variable (as the XDG spec requires) or a `user.home` such as the JDK's `?` for a user with no home. With no base
    * to resolve against it is a `Left` naming the variable to set, never a guess. A configured relative path is
    * returned as given.
    */
  def resolve(
    configured: Option[String],
    xdgHome: Option[String],
    userHome: Option[String]
  ): Either[String, Path] =
    AppDir.nonBlank(configured) match {
      case Some(path) => Right(Paths.get(path))
      case None =>
        AppDir
          .absolute(xdgHome)
          .orElse(AppDir.absolute(userHome).map(home => s"$home/${base.homeRelative}"))
          .toRight(
            s"Cannot resolve $configKey: neither ${base.envVar} nor the user.home JVM property is an " +
              s"absolute path. Set $overrideVar to an absolute path."
          )
          .map(root => Paths.get(root, "ccas", leaf))
    }

  /** [[resolve]] against this process's config and environment. Not absolutised, so a caller can tell a relative
    * setting from the default.
    */
  def fromConfig: Task[Path] =
    ZIO
      .attempt {
        val config = ConfigFactory.load()
        resolve(
          configured = Option.when(config.hasPath(configKey))(config.getString(configKey)),
          xdgHome = Option(System.getenv(base.envVar)),
          userHome = Option(System.getProperty("user.home"))
        )
      }
      .flatMap(resolved => ZIO.fromEither(resolved).mapError(new IllegalArgumentException(_)))
}

object AppDir {

  enum XdgBase(val envVar: String, val homeRelative: String) {
    case Data  extends XdgBase("XDG_DATA_HOME", ".local/share")
    case State extends XdgBase("XDG_STATE_HOME", ".local/state")
  }

  private def nonBlank(value: Option[String]): Option[String] = value.map(_.trim).filter(_.nonEmpty)

  private def absolute(value: Option[String]): Option[String] = nonBlank(value).filter(Paths.get(_).isAbsolute)
}
