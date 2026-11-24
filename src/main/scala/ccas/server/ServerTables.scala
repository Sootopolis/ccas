package ccas.server

import zio.{RIO, Scope, ZIO, ZIOAppDefault}

import ccas.analysis.tables.{ManagedClub, Tables}
import ccas.server.jobs.JobRun
import ccas.server.scheduler.{JobSchedule, SchedulerConfig, SchedulerDefaults}
import ccas.utils.ProgressDisplay
import ccas.utils.config.Settings
import ccas.utils.sql.PostgresClient

object ServerTables extends ZIOAppDefault {

  override def run: RIO[Scope, Unit] =
    ProgressDisplay.installed(showProgress = false) {
      for {
        _ <- ensureTables.provide(PostgresClient.live(), Settings.live)
        _ <- ZIO.logInfo("All tables ensured")
      } yield ()
    }

  def ensureTables: RIO[PostgresClient & Settings, Unit] =
    for {
      _      <- Tables.ensureTablesOnInit
      _      <- JobRun.createTable
      _      <- JobSchedule.createTable
      config <- SchedulerConfig.load
      // ZIO.attempt so an out-of-range scheduler.defaults interval fails boot as a typed error rather than a defect.
      seeds <- ZIO.attempt(SchedulerDefaults.fromConfig(config))
      _     <- ZIO.foreachDiscard(seeds)(JobSchedule.seedGlobalIfAbsent)
      // Per managed-club seeds (#102): History + Membership for each managed club that holds a name. Never all
      // known clubs — selectClubIds is the managed source, so heavy History crawls never target scouted/opponent
      // clubs. Fresh DB / no managed clubs → empty list → no-op.
      perClubSeeds <- ZIO.attempt(SchedulerDefaults.perClubFromConfig(config))
      managedIds   <- ManagedClub.selectClubIds
      _ <- ZIO.foreachDiscard(managedIds) { clubId =>
        ZIO.foreachDiscard(perClubSeeds)(seed => JobSchedule.seedPerClubIfAbsent(clubId, seed))
      }
    } yield ()
}
