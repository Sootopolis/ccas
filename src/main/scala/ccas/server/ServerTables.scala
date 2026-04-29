package ccas.server

import zio.{RIO, Scope, ZIO, ZIOAppDefault}

import ccas.analysis.tables.Tables
import ccas.server.jobs.JobRun
import ccas.server.scheduler.JobSchedule
import ccas.utils.ProgressDisplay
import ccas.utils.sql.PostgresClient

object ServerTables extends ZIOAppDefault {

  override def run: RIO[Scope, Unit] =
    for {
      _ <- ProgressDisplay.live(showProgress = false).build
      _ <- ensureTables.provide(PostgresClient.live())
      _ <- ZIO.logInfo("All tables ensured")
    } yield ()

  def ensureTables: RIO[PostgresClient, Unit] =
    for {
      _ <- Tables.ensureTables
      _ <- JobRun.createTable
      _ <- JobSchedule.createTable
    } yield ()
}
