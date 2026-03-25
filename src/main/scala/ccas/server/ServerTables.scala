package ccas.server

import com.augustnagro.magnum.Transactor
import zio.{RIO, Scope, ZIO, ZIOAppDefault}
import ccas.analysis.tables.Tables
import ccas.analysis.tables.Tables.ensureTables
import ccas.server.jobs.JobRun
import ccas.server.scheduler.JobSchedule
import ccas.utils.sql.DataSourceLayer

object ServerTables extends ZIOAppDefault {
  override def run: RIO[Scope, Unit] =
    ensureTables.provide(DataSourceLayer.liveFromPrefix()) <* ZIO.logInfo("All tables ensured")

  def ensureTables: RIO[Transactor, Unit] =
    for {
      _ <- Tables.ensureTables
      _ <- JobRun.createTable
      _ <- JobSchedule.createTable
    } yield ()
}
