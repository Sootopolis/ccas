package ccas.analysis.apps.recruitment

import java.time.Instant

import com.augustnagro.magnum.Transactor
import zio.{Console, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client

import ccas.analysis.tables.*
import ccas.api.club.{ApiClub, ApiClubMembers}
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer

object RecruitmentApp extends ZIOAppDefault {

  override def run: ZIO[Any & ZIOAppArgs & Scope, Any, Any] =
    for {
      args <- ZIOAppArgs.getArgs
      _ <- (args.toList match
        case "report" :: clubStr :: rest => showReport(ClubUrlName.wrap(clubStr), rest.headOption)
        case clubStr :: rest => recruit(ClubUrlName.wrap(clubStr), rest.headOption.getOrElse("default"))
        case _ =>
          ZIO.fail(
            ExternalException(
              "Usage: RecruitmentApp <club-url-name> [config-name]\n       RecruitmentApp report <club-url-name> [run-id]"
            )
          )
      ).provide(
        ChessComClient.live(),
        Client.default,
        DataSourceLayer.liveFromPrefix()
      )
    } yield ()

  // --- Phase 1: Initialize ---

  private[recruitment] def recruit(clubUrlName: ClubUrlName, configName: String)
      : ZIO[ChessComClient & Transactor, Throwable, RecruitmentRun] =
    for {
      _       <- ensureTables
      client  <- ZIO.service[ChessComClient]
      apiClub <- ApiClub.get(client, clubUrlName)
      clubId = apiClub.clubId
      club   = Club(clubId, Instant.ofEpochSecond(apiClub.created), clubUrlName)
      _ <- Club.upsert(club)
      config <- RecruitmentConfig.select(clubId, configName)
        .someOrFail(ExternalException(s"No recruitment config '$configName' found for club '$clubUrlName'"))
      now = Instant.now()
      runId <- RecruitmentRun.insert(clubId, configName, now)

      // --- Phase 2: Gather candidate usernames ---
      candidates <- gatherCandidates(client, clubId, clubUrlName, config)

      // --- Phase 3: Evaluate candidates (placeholder) ---
      invited <- evaluateCandidates(client, runId, candidates, config.maxCandidates)

      // --- Phase 4: Finalize ---
      completedAt = Instant.now()
      finalRun    = RecruitmentRun(runId, clubId, configName, now, Some(completedAt), invited.size)
      _ <- RecruitmentRun.update(finalRun)
      _ <- Console.printLine(s"=== Recruitment Complete ===").orDie
      _ <- Console.printLine(s"Candidates evaluated: ${candidates.size}").orDie
      _ <- Console.printLine(s"Invited: ${invited.size}").orDie
      _ <- ZIO.foreachDiscard(invited)(u => Console.printLine(s"  $u").orDie)
    } yield finalRun

  private def ensureTables: ZIO[Transactor, Throwable, Unit] =
    for {
      _ <- Player.createTable
      _ <- PlayerSnapshot.createTable
      _ <- Club.createTable
      _ <- ClubMember.createTable
      _ <- RecruitmentConfig.createTable
      _ <- RecruitmentRun.createTable
      _ <- RecruitmentCandidate.createTable
    } yield ()

  // --- Phase 2: Gather candidate usernames ---

  private[recruitment] def gatherCandidates(
      client: ChessComClient,
      clubId: ClubId,
      clubUrlName: ClubUrlName,
      config: RecruitmentConfig
    ): ZIO[Transactor, Throwable, List[Username]] =
    for {
      // Fetch target club members
      targetMembers <- ApiClubMembers.get(client, clubUrlName)
      existingUsernames = targetMembers.toMap.keySet

      // Fetch members from all source clubs
      sourceMembers <- ZIO.foreach(config.sourceClubNames) { sourceClubName =>
        ApiClubMembers.get(client, sourceClubName).map(_.toMap.keySet)
      }

      // Combine, deduplicate, and filter out existing members
      allSourceUsernames = sourceMembers.foldLeft(Set.empty[Username])(_ ++ _)
      candidates         = (allSourceUsernames -- existingUsernames).toList
    } yield candidates

  // --- Phase 3: Evaluate candidates (placeholder) ---

  private[recruitment] def evaluateCandidates(
      client: ChessComClient,
      runId: Long,
      candidates: List[Username],
      maxCandidates: Int
    ): ZIO[Transactor, Throwable, List[Username]] = {
    // TODO: implement filter chain — see filter evaluation order below
    // 1. DB check: selectLatestInvited → InvitedTooRecently
    // 2. ApiPlayer: status, registration age, nationality → upsert Player + PlayerSnapshot
    // 3. ApiPlayerClubs: club count, excluded clubs
    // 4. ApiPlayerStats: daily Elo, timeout rate, games finished
    // 5. ApiPlayerGamesCurrent: ongoing games count
    // 6. ApiPlayerMatches: team matches, team match timeout rate

    val toEvaluate = candidates.take(maxCandidates)
    ZIO.foldLeft(toEvaluate)(List.empty[Username]) { case (invited, username) =>
      val now = Instant.now()
      (for {
        apiPlayer <- client.getWithPermit[ApiPlayer](ApiPlayer.getUrl(username))
        playerId  = apiPlayer.playerId
        statusCat = apiPlayer.status.category

        // Persist player data (same as MembershipApp does for caching)
        existingPlayer <- Player.selectId(playerId)
        _ <- ZIO.when(existingPlayer.isEmpty) {
          Player.insert(Player(playerId, Instant.ofEpochSecond(apiPlayer.joined), None))
        }
        _ <- PlayerSnapshot.insert(PlayerSnapshot(playerId, now, username, statusCat, apiPlayer.title))

        // Placeholder: accept all candidates
        outcome   = CandidateOutcome.Invited
        candidate = RecruitmentCandidate(runId, username, now, outcome, None)
        _ <- RecruitmentCandidate.insert(candidate)
      } yield invited :+ username).catchAll { error =>
        // On error fetching player data, record as Error outcome
        val candidate = RecruitmentCandidate(
          runId,
          username,
          now,
          CandidateOutcome.Error,
          Some(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
        )
        RecruitmentCandidate.insert(candidate).as(invited)
      }
    }
  }

  // --- Report mode ---

  private[recruitment] def showReport(clubUrlName: ClubUrlName, runIdOpt: Option[String])
      : ZIO[Transactor, Throwable, Unit] =
    for {
      clubs <- Club.selectAll
      club <- ZIO.fromOption(clubs.find(_.urlName == clubUrlName))
        .orElseFail(ExternalException(s"Club '$clubUrlName' not found in database"))
      clubId = club.clubId
      run <- runIdOpt match {
        case Some(id) =>
          ZIO.attempt(id.toLong)
            .orElseFail(ExternalException(s"Invalid run ID: '$id' (expected a number)"))
            .flatMap(RecruitmentRun.selectId)
            .someOrFail(ExternalException(s"Run $id not found"))
        case None =>
          RecruitmentRun.selectLatest(clubId)
            .flatMap(ZIO.fromOption(_).orElseFail(ExternalException(s"No runs found for club '$clubUrlName'")))
      }
      invited <- RecruitmentCandidate.selectInvitedByRun(run.runId)
      _       <- Console.printLine(s"=== Recruitment Report for $clubUrlName (run ${run.runId}) ===").orDie
      _       <- Console.printLine(s"Started: ${run.startedAt}").orDie
      _       <- Console.printLine(s"Completed: ${run.completedAt.getOrElse("in progress")}").orDie
      _       <- Console.printLine(s"Invited: ${invited.size}").orDie
      _       <- ZIO.foreachDiscard(invited)(c => Console.printLine(s"  ${c.username}").orDie)
    } yield ()
}
