package ccas.server

import com.augustnagro.magnum.Transactor
import zio.RIO

import ccas.analysis.tables.Tables
import ccas.server.jobs.JobRun
import ccas.server.scheduler.JobSchedule

object ServerTables {
  def ensureTables: RIO[Transactor, Unit] =
    for {
      _ <- Tables.ensureTables
      _ <- JobRun.createTable
      _ <- JobSchedule.createTable
    } yield ()
}
