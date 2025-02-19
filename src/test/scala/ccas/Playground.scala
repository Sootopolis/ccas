package ccas

import zio.schema.{DeriveSchema, Schema, derived}
import zio.{Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import java.time.{Instant, LocalDateTime}

object Playground extends ZIOAppDefault {
  def run = ZIO.debug(LocalDateTime.parse("2023-10-31T00:00:01"))
}
