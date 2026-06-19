package ccas.server

import zio.{RIO, Scope, ZIO, ZIOAppDefault}

import ccas.analysis.tables.Tables
import ccas.server.jobs.JobRun
import ccas.server.scheduler.{JobSchedule, SchedulerDefaults}
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
      // ZIO.attempt so a malformed scheduler.defaults value (non-int, non-bool, out-of-range) fails boot
      // as a typed error rather than a ZIO defect.
      seeds <- ZIO.attempt(SchedulerDefaults.fromConfig)
      _     <- ZIO.foreachDiscard(seeds)(JobSchedule.seedGlobalIfAbsent)
    } yield ()
}
