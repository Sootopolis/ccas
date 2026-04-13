package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant}

import ccas.utils.sql.PostgresClient
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.{ZIO, ZLayer}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Username}
import ccas.utils.TestCcasLogger
import ccas.utils.sql.FreshSchemaLayer

object TestRecruitmentBlacklist extends ZIOSpecDefault {

  private val blacklistClubSlug = ClubSlug("blacklist-club")

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentBlacklist")(
    suiteBlacklist,
    suiteBlacklistApp
  ).provideShared(
    FreshSchemaLayer("test_recruitment_blacklist", onInit = Tables.ensureTables),
    ZLayer.succeed(TestCcasLogger.noop)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: Blacklist
  // ==========================================================================

  private def suiteBlacklist = suite("blacklist")(
    testBlacklistedPlayerRejected,
    testExpiredBlacklistDoesNotReject
  )

  private def testBlacklistedPlayerRejected = test("blacklisted player is rejected during evaluation") {
    val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
    val criteria  = makeCriteria()
    for {
      _          <- seedDb
      _          <- seedPlayer(pid0)
      criteriaId <- seedCriteria(criteria)
      // Blacklist alice (indefinite)
      _ <- RecruitmentBlacklist.insert(
        RecruitmentBlacklist(clubId, pid0, Times.t0, expiresAt = None, reason = Some("banned"))
      )
      runId  <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
      client <- fakeChessComClient(responses)
      _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
      cands  <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      cands.size == 1,
      cands.head.outcome == CandidateOutcome.Rejected
    )
  }

  private def testExpiredBlacklistDoesNotReject = test("expired blacklist entry does not reject the player") {
    val now       = Instant.now()
    val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
    val criteria  = makeCriteria()
    for {
      _          <- seedDb
      _          <- seedPlayer(pid0)
      criteriaId <- seedCriteria(criteria)
      // Blacklist alice with an already-expired entry
      _ <- RecruitmentBlacklist.insert(
        RecruitmentBlacklist(
          clubId,
          pid0,
          Times.t0,
          expiresAt = Some(now.minus(java.time.Duration.ofDays(1))),
          reason = Some("temp ban")
        )
      )
      runId  <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, now)
      client <- fakeChessComClient(responses)
      _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
      cands  <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      cands.size == 1,
      cands.head.outcome == CandidateOutcome.Deferred
    )
  }

  // ==========================================================================
  // Suite: BlacklistApp
  // ==========================================================================

  private def suiteBlacklistApp = suite("BlacklistApp")(
    testInsertWithReasonAndExpiresAt,
    testInsertWithoutOptionalFields,
    testUpsertExistingEntry,
    testListShowsActiveEntries,
    testRemoveDeletesByUsername,
    testHandlesMultipleUsernames,
    testUpsertsClubBeforeInsert
  )

  private def testInsertWithReasonAndExpiresAt = test("inserts blacklist entry with reason and expiresAt") {
    val futureInstant = Times.t3
    val responses = Map(
      s"club/$blacklistClubSlug" -> apiClubJson(700, blacklistClubSlug.value, Nil),
      "player/target-player"     -> apiPlayerJson(203, "target-player")
    )
    for {
      _      <- seedDb
      client <- fakeChessComClient(responses)
      pgClient <- ZIO.service[PostgresClient]
      _ <- BlacklistApp.addToBlacklist(
        blacklistClubSlug,
        List(Username("target-player")),
        Some("toxic"),
        Some(futureInstant)
      )
        .provideEnvironment(zio.ZEnvironment(client, pgClient, TestCcasLogger.noop))
      entries <- RecruitmentBlacklist.selectByClub(blacklistClubId)
    } yield assertTrue(
      entries.size == 1,
      entries.head.playerId == pid3,
      entries.head.reason.contains("toxic"),
      entries.head.expiresAt.contains(futureInstant)
    )
  }

  private def testInsertWithoutOptionalFields = test("inserts blacklist entry without optional fields") {
    val responses = Map(
      s"club/$blacklistClubSlug" -> apiClubJson(700, blacklistClubSlug.value, Nil),
      "player/target-player"     -> apiPlayerJson(203, "target-player")
    )
    for {
      _      <- seedDb
      client <- fakeChessComClient(responses)
      pgClient <- ZIO.service[PostgresClient]
      _ <- BlacklistApp.addToBlacklist(blacklistClubSlug, List(Username("target-player")), None, None)
        .provideEnvironment(zio.ZEnvironment(client, pgClient, TestCcasLogger.noop))
      entries <- RecruitmentBlacklist.selectByClub(blacklistClubId)
    } yield assertTrue(
      entries.size == 1,
      entries.head.reason.isEmpty,
      entries.head.expiresAt.isEmpty
    )
  }

  private def testUpsertExistingEntry = test("upserts existing blacklist entry instead of failing on duplicate") {
    val responses = Map(
      s"club/$blacklistClubSlug" -> apiClubJson(700, blacklistClubSlug.value, Nil),
      "player/target-player"     -> apiPlayerJson(203, "target-player")
    )
    for {
      _      <- seedDb
      client <- fakeChessComClient(responses)
      pgClient <- ZIO.service[PostgresClient]
      _ <- BlacklistApp.addToBlacklist(blacklistClubSlug, List(Username("target-player")), Some("first"), None)
        .provideEnvironment(zio.ZEnvironment(client, pgClient, TestCcasLogger.noop))
      _ <- BlacklistApp.addToBlacklist(
        blacklistClubSlug,
        List(Username("target-player")),
        Some("updated"),
        Some(Times.t3)
      )
        .provideEnvironment(zio.ZEnvironment(client, pgClient, TestCcasLogger.noop))
      entries <- RecruitmentBlacklist.selectByClub(blacklistClubId)
    } yield assertTrue(
      entries.size == 1,
      entries.head.reason.contains("updated"),
      entries.head.expiresAt.contains(Times.t3)
    )
  }

  private def testListShowsActiveEntries = test("listBlacklist shows active entries") {
    for {
      _ <- seedDb
      _ <- Club.upsert(Club(blacklistClubId, Times.t0, blacklistClubSlug, "Blacklist Club", None, None, None))
      _ <- Player.insert(Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0))
      _ <- RecruitmentBlacklist.insert(
        RecruitmentBlacklist(blacklistClubId, pid0, Times.t0, None, Some("indefinite"))
      )
      // Expired entry should not appear
      _ <- seedPlayer(pid1)
      _ <- RecruitmentBlacklist.insert(
        RecruitmentBlacklist(
          blacklistClubId,
          pid1,
          Times.t0,
          Some(Instant.now().minus(Duration.ofDays(1))),
          Some("expired")
        )
      )
      entries <- RecruitmentBlacklist.selectActiveByClub(blacklistClubId, Instant.now())
    } yield assertTrue(
      entries.size == 1,
      entries.head.playerId == pid0,
      entries.head.username.contains(Username("alice")),
      entries.head.reason.contains("indefinite")
    )
  }

  private def testRemoveDeletesByUsername = test("removeFromBlacklist deletes entry by username") {
    for {
      _ <- seedDb
      _ <- Club.upsert(Club(blacklistClubId, Times.t0, blacklistClubSlug, "Blacklist Club", None, None, None))
      _ <- Player.insert(Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0))
      _ <- RecruitmentBlacklist.insert(
        RecruitmentBlacklist(blacklistClubId, pid0, Times.t0, None, Some("banned"))
      )
      before <- RecruitmentBlacklist.selectByClub(blacklistClubId)
      _      <- BlacklistApp.removeFromBlacklist(blacklistClubSlug, Username("alice"))
      after  <- RecruitmentBlacklist.selectByClub(blacklistClubId)
    } yield assertTrue(
      before.size == 1,
      after.isEmpty
    )
  }

  private def testHandlesMultipleUsernames = test("addToBlacklist handles multiple usernames") {
    val responses = Map(
      s"club/$blacklistClubSlug" -> apiClubJson(700, blacklistClubSlug.value, Nil),
      "player/alice"             -> apiPlayerJson(200, "alice"),
      "player/bob"               -> apiPlayerJson(201, "bob"),
      "player/charlie"           -> apiPlayerJson(202, "charlie")
    )
    for {
      _      <- seedDb
      client <- fakeChessComClient(responses)
      pgClient <- ZIO.service[PostgresClient]
      _ <- BlacklistApp.addToBlacklist(
        blacklistClubSlug,
        List(Username("alice"), Username("bob"), Username("charlie")),
        Some("batch ban"),
        None
      ).provideEnvironment(zio.ZEnvironment(client, pgClient, TestCcasLogger.noop))
      entries <- RecruitmentBlacklist.selectByClub(blacklistClubId)
    } yield assertTrue(
      entries.size == 3,
      entries.map(_.playerId).toSet == Set(pid0, pid1, pid2),
      entries.forall(_.reason.contains("batch ban")),
      entries.forall(_.expiresAt.isEmpty)
    )
  }

  private def testUpsertsClubBeforeInsert = test("upserts club before inserting blacklist entry") {
    val freshClubId   = ClubId(701)
    val freshClubSlug = ClubSlug("fresh-club")
    val responses = Map(
      s"club/$freshClubSlug" -> apiClubJson(701, freshClubSlug.value, Nil),
      "player/target-player" -> apiPlayerJson(203, "target-player")
    )
    for {
      _      <- seedDb
      client <- fakeChessComClient(responses)
      pgClient <- ZIO.service[PostgresClient]
      before <- Club.selectId(freshClubId)
      _ <- BlacklistApp.addToBlacklist(freshClubSlug, List(Username("target-player")), None, None)
        .provideEnvironment(zio.ZEnvironment(client, pgClient, TestCcasLogger.noop))
      after <- Club.selectId(freshClubId)
    } yield assertTrue(
      before.isEmpty,
      after.isDefined,
      after.get.slug == freshClubSlug
    )
  }
}
