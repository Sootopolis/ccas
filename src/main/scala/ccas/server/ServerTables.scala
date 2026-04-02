package ccas.server

import zio.{RIO, Scope, ZIOAppDefault}

import ccas.analysis.tables.Tables
import ccas.server.jobs.JobRun
import ccas.server.scheduler.JobSchedule
import ccas.utils.sql.PostgresClient
import ccas.utils.CcasLogger

object ServerTables extends ZIOAppDefault {

  override def run: RIO[Scope, Unit] =
    (ensureTables.provide(PostgresClient.live()) <* CcasLogger.info("All tables ensured"))
      .provideSome[Scope](CcasLogger.live())

  def ensureTables: RIO[PostgresClient, Unit] =
    for {
      _ <- Tables.ensureTables
      _ <- JobRun.createTable
      _ <- JobSchedule.createTable
    } yield ()
}
