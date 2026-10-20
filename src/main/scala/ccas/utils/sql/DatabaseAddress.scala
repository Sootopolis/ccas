package ccas.utils.sql

import java.net.InetAddress

import scala.util.Try

/** Where a pool connects, as its URL wrote it: `host[:port]` and the `/database` path, without the URL's userinfo or
  * parameters. For telling an operator which database a server is on (#320); built by
  * [[PostgresClient.databaseAddress]].
  */
final case class DatabaseAddress(hostPort: String, path: String) {

  def render: String = s"$hostPort$path"

  /** `localhost`, or a loopback IP literal (`127.x.x.x`, `[::1]` in any spelling). Never a DNS lookup. A multi-host
    * address never is, since one of its hosts may not be. Only the address is read, so a tunnel through `localhost` to
    * a remote database is loopback too.
    */
  def isLoopback: Boolean =
    !hostPort.contains(',') && {
      val host =
        if (hostPort.startsWith("[")) { hostPort.drop(1).takeWhile(_ != ']') }
        else { hostPort.takeWhile(_ != ':') }
      // `ofLiteral` parses an IP literal and refuses anything else, so a host name never reaches DNS.
      host.equalsIgnoreCase("localhost") || Try(InetAddress.ofLiteral(host)).toOption.exists(_.isLoopbackAddress)
    }
}
