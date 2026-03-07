package ccas.analysis.apps.membership

import ccas.utils.client.CcasClient
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

object MembershipApp extends ZIOAppDefault {
  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] = ???
}
