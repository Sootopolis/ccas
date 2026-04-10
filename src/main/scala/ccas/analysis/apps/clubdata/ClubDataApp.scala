package ccas.analysis.apps.clubdata

import java.time.Instant

import zio.{RIO, Ref, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.{Club, ClubAdmin, Player, Tables}
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.{CcasLogger, OutputFile}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

object ClubDataApp extends ZIOAppDefault {

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      data <- refresh
      _    <- OutputFile.writeAndLogGlobal("clubdata", formatReport(data), "_ccas")
    } yield ()).provideSome[Scope](
      CcasLogger.live(showProgress = true),
      ChessComClient.live("clubdata"),
      Client.default,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  case class RefreshResult(clubsProcessed: Int, clubsFailed: Int, clubsAdminChanged: Int, totalAdmins: Int)

  private case class ClubResult(adminCount: Int, failed: Boolean, adminChanged: Boolean)
  private val ClubFailed = ClubResult(0, failed = true, adminChanged = false)

  /** Refreshes club profile data (admins, member count, slug) for all known clubs. */
  def refresh: RIO[CcasLogger & ChessComClient & PostgresClient, RefreshResult] = ZIO.scoped {
    for {
      client    <- ZIO.service[ChessComClient]
      clubs     <- Club.selectAll
      total     = clubs.size
      _         <- CcasLogger.info(s"[ClubData] Refreshing $total clubs")
      bar       <- CcasLogger.progressBar
      resultRef <- Ref.make(RefreshResult(0, 0, 0, 0))
      _ <- ZIO.foreachPar(clubs) { club =>
        for {
          r <- refreshClub(client, club).catchAll { error =>
            CcasLogger.info(s"[ClubData] Failed to refresh ${club.slug}: ${error.getMessage}").as(ClubFailed)
          }
          updated <- resultRef.updateAndGet(acc =>
            acc.copy(
              clubsProcessed = acc.clubsProcessed + 1,
              clubsFailed = acc.clubsFailed + (if (r.failed) 1 else 0),
              clubsAdminChanged = acc.clubsAdminChanged + (if (r.adminChanged) 1 else 0),
              totalAdmins = acc.totalAdmins + r.adminCount
            )
          )
          _ <- bar.print(updated.clubsProcessed, total, s"Refreshing clubs: ${updated.clubsProcessed}/$total")
        } yield ()
      }.withParallelism(8).onInterrupt {
        (for {
          partial <- resultRef.get
          _       <- logSummary("Interrupted", partial, total)
        } yield ()).orDie
      }
      result <- resultRef.get
      _      <- logSummary("Done", result, total)
    } yield result
  }

  private def logSummary(label: String, r: RefreshResult, total: Int): RIO[CcasLogger, Unit] =
    CcasLogger.info(
      s"[ClubData] $label: ${r.clubsProcessed}/$total clubs, ${r.clubsFailed} failed, " +
        s"${r.clubsAdminChanged} admin changes, ${r.totalAdmins} total admins"
    )

  private def refreshClub(client: ChessComClient, club: Club): RIO[CcasLogger & PostgresClient, ClubResult] =
    for {
      apiClub <- ApiClub.get(client, club.slug)
      _       <- Club.upsertResolvingSlugConflict(Club.fromApi(apiClub, club.slug), client)

      adminUsernames   = ClubAdmin.extractAdminUsernames(apiClub)
      existingAdminIds <- ClubAdmin.selectPlayerIdsByClub(club.clubId)
      result           <- resolveAndUpdateAdmins(client, club.clubId, adminUsernames, existingAdminIds)
    } yield result

  /** Resolves admin usernames to player IDs and updates club_admin if changed. */
  private def resolveAndUpdateAdmins(
    client: ChessComClient,
    clubId: ClubId,
    adminUsernames: Set[Username],
    existingAdminIds: Set[PlayerId]
  ): RIO[CcasLogger & PostgresClient, ClubResult] =
    if (adminUsernames.isEmpty) {
      ZIO.whenDiscard(existingAdminIds.nonEmpty)(ClubAdmin.deleteByClub(clubId))
        .as(ClubResult(0, failed = false, adminChanged = existingAdminIds.nonEmpty))
    } else {
      for {
        knownPlayers <- Player.selectByUsernames(adminUsernames)
        knownByUsername = knownPlayers.map(p => p.username -> p.playerId).toMap
        unknownUsernames = adminUsernames -- knownByUsername.keySet

        resolvedUnknowns <- ZIO.foreach(unknownUsernames.toList) { username =>
          (for {
            apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
            player = Player(
              apiPlayer.playerId,
              apiPlayer.joinedAt,
              apiPlayer.username,
              apiPlayer.status.category,
              apiPlayer.title,
              Instant.now()
            )
            _ <- Player.insertIfNew(player)
          } yield Some(apiPlayer.username -> apiPlayer.playerId)).catchAll { error =>
            CcasLogger.info(s"[ClubData] Could not resolve admin '$username': ${error.getMessage}")
              .as(None)
          }
        }.map(_.flatten.toMap)

        allAdminIds = (knownByUsername ++ resolvedUnknowns).values.toSet
        changed     = allAdminIds != existingAdminIds
        _ <- ZIO.whenDiscard(changed)(ClubAdmin.replaceForClub(clubId, allAdminIds))
      } yield ClubResult(allAdminIds.size, failed = false, adminChanged = changed)
    }

  private def formatReport(data: RefreshResult): String =
    s"""Clubs processed: ${data.clubsProcessed}
       |Clubs failed: ${data.clubsFailed}
       |Clubs with admin changes: ${data.clubsAdminChanged}
       |Total admins stored: ${data.totalAdmins}
       |""".stripMargin
}
