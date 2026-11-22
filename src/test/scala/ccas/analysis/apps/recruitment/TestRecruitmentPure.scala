package ccas.analysis.apps.recruitment

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.TestTimes
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.makeCriteria
import ccas.analysis.tables.{
  CandidateOutcome,
  PlayerRecruitmentCache,
  RecruitmentAlias,
  RecruitmentCriteria,
  RejectionCount,
  RejectionReason
}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo, PlayerId}

object TestRecruitmentPure extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentPure")(
    suiteParseRecruitArgs,
    suiteRejectionLines,
    suiteCriteriaHelpers,
    suiteCriteriaBlock,
    suite("cached criteria")(cachedCriteriaTests*)
  )

  // ==========================================================================
  // Suite: The criteria recap a run logs and reports, and the CLI confirms (pure)
  // ==========================================================================

  /** Criteria with nothing set, which `makeCriteria` alone is not: it sets `excludeSourceAdmins`. */
  private val nothingSet = makeCriteria().copy(excludeSourceAdmins = false)

  private def aliasSet(alias: String, since: String): RecruitmentAlias =
    RecruitmentAlias(ClubId(1), alias, Instant.parse(since), criteriaId = 7)

  /** The slug of the club `aliasSet`'s rows belong to. */
  private val teamAlpha = Map(ClubId(1) -> ClubSlug("team-alpha"))

  private def suiteCriteriaBlock = suite("criteria recap")(
    test("the criteria recap puts one setting per line, aligned, with what it means") {
      val criteria = nothingSet.copy(
        maxClubs = Some(40),
        excludeSourceAdmins = true,
        dailyMinElo = Some(Elo(1200)),
        dailyMaxTmTimeoutPercent = Some(0.0)
      )
      assertTrue(
        CriteriaRecap.settingLines(criteria, Map.empty) == List(
          "maxClubs                  40    in at most this many clubs",
          "excludeSourceAdmins       yes   not an admin of a club being scouted",
          "dailyMinElo               1200  daily rating at least this",
          "dailyMaxTmTimeoutPercent  0     timed out of at most this % of team-match games of any kind in the last 90 days"
        )
      )
    },
    test("the criteria recap names the club, the criteria and their version, not the criteria id") {
      val lines = CriteriaRecap.lines(List(aliasSet("default", "2026-03-18T23:30:00Z")), makeCriteria(), teamAlpha)
      assertTrue(
        lines.headOption.contains("team-alpha's criteria \"default\", version of 2026-03-18 23:30 UTC:"),
        !lines.exists(_.contains("7"))
      )
    },
    test("criteria several names point at name each, with its version") {
      val aliases = List(aliasSet("loose", "2026-01-02T00:00:00Z"), aliasSet("strict", "2026-02-03T00:00:00Z"))
      assertTrue(
        CriteriaRecap.header(aliases, teamAlpha) ==
          "team-alpha's criteria \"loose\", version of 2026-01-02 00:00 UTC; " +
          "team-alpha's criteria \"strict\", version of 2026-02-03 00:00 UTC"
      )
    },
    test("two versions set on one day read apart, by the minute in UTC") {
      val morning   = CriteriaRecap.header(List(aliasSet("default", "2026-11-15T09:05:00Z")), teamAlpha)
      val afternoon = CriteriaRecap.header(List(aliasSet("default", "2026-11-15T14:32:59Z")), teamAlpha)
      assertTrue(
        morning == "team-alpha's criteria \"default\", version of 2026-11-15 09:05 UTC",
        afternoon == "team-alpha's criteria \"default\", version of 2026-11-15 14:32 UTC"
      )
    },
    test("every setting the operator can set appears in the recap once set") {
      val everySet = RecruitmentCriteria.defaultDaily.copy(
        nationalityCountries = List("GB"),
        excludeClubs = List(ClubId(7)),
        avoidAdminMinClubSize = Some(100),
        dailyMaxElo = Some(Elo(2000)),
        dailyMinOngoingGames = Some(1),
        dailyMinOngoingTeamMatches = Some(1)
      )
      val shown = CriteriaRecap.settingLines(everySet, Map.empty).map(_.takeWhile(_ != ' '))
      // nationalityExclude reads as the nationality list's sense, not under its own name.
      val settable = CriteriaSpec.fromCriteria(everySet).productElementNames.toSet - "nationalityExclude"
      assertTrue(settable.diff(shown.toSet).isEmpty, shown.size == shown.distinct.size)
    },
    test("a nationality list says whether it excludes or requires its countries") {
      val listed = nothingSet.copy(nationalityCountries = List("GB", "FR"))
      assertTrue(
        CriteriaRecap.settingLines(listed.copy(nationalityExclude = true), Map.empty) ==
          List("nationalityCountries  from none of these countries: GB, FR"),
        CriteriaRecap.settingLines(listed, Map.empty) == List(
          "nationalityCountries  from one of these countries: GB, FR"
        ),
        CriteriaRecap.settingLines(nothingSet.copy(nationalityExclude = true), Map.empty).isEmpty
      )
    },
    test("a list-valued setting follows its meaning, leaving the others aligned") {
      // An excluded club reads by slug, or by id when no club row has it.
      val criteria = nothingSet.copy(maxClubs = Some(40), excludeClubs = List(ClubId(1), ClubId(2)))
      assertTrue(
        CriteriaRecap.settingLines(criteria, Map(ClubId(1) -> ClubSlug("club-a"))) == List(
          "maxClubs      40  in at most this many clubs",
          "excludeClubs      in none of these clubs: club-a, #2"
        )
      )
    },
    test("a whole-number limit reads without a decimal point") {
      val criteria = nothingSet.copy(dailyMinScoreRate = Some(3.0 / 8), dailyMaxTimeoutPercent = Some(5.0))
      assertTrue(
        CriteriaRecap.settingLines(criteria, Map.empty) == List(
          "dailyMinScoreRate       0.375  daily score rate at least this, draws counting half",
          "dailyMaxTimeoutPercent  5      timed out of at most this % of daily games, by Chess.com's figure"
        )
      )
    },
    test("a daily timeout limit of 0 reads as no timeout in the window, which the cached check also enforces") {
      val zero = CriteriaRecap.settingLines(nothingSet.copy(dailyMaxTimeoutPercent = Some(0.0)), Map.empty)
      val five = CriteriaRecap.settingLines(nothingSet.copy(dailyMaxTimeoutPercent = Some(5.0)), Map.empty)
      assertTrue(
        zero == List(
          "dailyMaxTimeoutPercent  0  no daily game of any kind, team matches included, timed out in the last 90 days"
        ),
        five == List("dailyMaxTimeoutPercent  5  timed out of at most this % of daily games, by Chess.com's figure")
      )
    },
    test("criteria with nothing set say so") {
      assertTrue(
        CriteriaRecap.lines(List(aliasSet("loose", "2026-03-18T00:00:00Z")), nothingSet, teamAlpha) ==
          List("team-alpha's criteria \"loose\", version of 2026-03-18 00:00 UTC: none set")
      )
    }
  )

  // ==========================================================================
  // Suite: CLI argument parsing (pure)
  // ==========================================================================

  private def suiteParseRecruitArgs = suite("parseRecruitArgs")(
    test("alias and target from positional + flag") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target", "3"))
      assertTrue(
        r.alias == "default",
        r.target.contains(3),
        r.sourceClubs.isEmpty,
        !r.cumulative,
        !r.focus
      )
    },
    test("source clubs before flags") {
      val r = RecruitmentApp.parseRecruitArgs(List("myalias", "club-a", "club-b", "--target", "10"))
      assertTrue(
        r.alias == "myalias",
        r.sourceClubs.map(ClubSlug.unwrap) == List("club-a", "club-b"),
        r.target.contains(10)
      )
    },
    test("no flags defaults") {
      val r = RecruitmentApp.parseRecruitArgs(List("myalias"))
      assertTrue(
        r.alias == "myalias",
        r.target.isEmpty,
        r.sourceClubs.isEmpty,
        !r.cumulative,
        !r.focus
      )
    },
    test("empty args gives default alias") {
      val r = RecruitmentApp.parseRecruitArgs(Nil)
      assertTrue(r.alias == "default", r.target.isEmpty)
    },
    test("boolean flags without target") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--cumulative", "--focus"))
      assertTrue(r.cumulative, r.focus, r.target.isEmpty)
    },
    test("target between boolean flags") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--cumulative", "--target", "5", "--focus"))
      assertTrue(r.cumulative, r.focus, r.target.contains(5))
    },
    test("target without value is None") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target"))
      assertTrue(r.target.isEmpty)
    },
    test("target with non-int value is None") {
      val r = RecruitmentApp.parseRecruitArgs(List("default", "--target", "abc"))
      assertTrue(r.target.isEmpty)
    },
    test("flags only, no positional") {
      val r = RecruitmentApp.parseRecruitArgs(List("--target", "20", "--cumulative"))
      assertTrue(r.alias == "default", r.target.contains(20), r.cumulative)
    }
  )

  // ==========================================================================
  // Suite: a run's rejections in its report and log (pure)
  // ==========================================================================

  /** The criteria field a reason enforces, if it enforces one. */
  private def fieldOf(reason: RejectionReason): Option[String] =
    reason.check match {
      case RejectionReason.Check.Field(name, _) => Some(name)
      case RejectionReason.Check.Standing(_)    => None
    }

  private def suiteRejectionLines = suite("rejectionLines")(
    test("rejection reasons are listed by count, largest first, ties in declaration order") {
      val lines = RecruitmentApp.rejectionLines(
        List(
          RejectionCount(Some(RejectionReason.DailyMinElo), CandidateOutcome.Rejected, 2),
          RejectionCount(None, CandidateOutcome.Rejected, 2),
          RejectionCount(Some(RejectionReason.MaxClubs), CandidateOutcome.Rejected, 5),
          RejectionCount(Some(RejectionReason.AccountNotActive), CandidateOutcome.Rejected, 2)
        )
      )
      assertTrue(
        lines == List(
          "Rejected: 11",
          "  maxClubs            5",
          "  account not active  2",
          "  dailyMinElo         2",
          "  no reason recorded  2"
        )
      )
    },
    test("a cache rejection counts under its reason and in the cached total") {
      val lines = RecruitmentApp.rejectionLines(
        List(
          RejectionCount(Some(RejectionReason.MaxClubs), CandidateOutcome.Rejected, 3),
          RejectionCount(Some(RejectionReason.MaxClubs), CandidateOutcome.RejectedFromCache, 4),
          RejectionCount(Some(RejectionReason.DailyMinElo), CandidateOutcome.RejectedFromCache, 1)
        )
      )
      assertTrue(
        lines == List(
          "Rejected: 8, 5 of them on cached figures",
          "  maxClubs     7",
          "  dailyMinElo  1"
        )
      )
    },
    test("a run that rejected no one has no rejection block") {
      assertTrue(RecruitmentApp.rejectionLines(Nil).isEmpty)
    },
    test("the rejection summary names the field alone") {
      assertTrue(
        RejectionReason.DailyMinElo.label == "dailyMinElo",
        RejectionReason.MaxClubs.label == "maxClubs",
        RejectionReason.DailyMinTmGamesFinished.label == "dailyMinTmGamesFinished"
      )
    },
    test("a field with two checks keeps a qualifier in the rejection summary") {
      val shared = RejectionReason.values.toList.groupBy(fieldOf).collect {
        case (Some(_), reasons) if reasons.size > 1 => reasons
      }.flatten.toList
      assertTrue(
        shared.map(_.label).sorted == List(
          "avoidAdminMinClubSize (in their club list)",
          "avoidAdminMinClubSize (on record)",
          "dailyMaxTimeoutPercent (a timeout in 90 days)",
          "dailyMaxTimeoutPercent (rate)",
          "dailyMaxTmTimeoutPercent (a timeout in 90 days)",
          "dailyMaxTmTimeoutPercent (rate)"
        )
      )
    },
    test("a reason that enforces no field keeps its text") {
      assertTrue(
        RejectionReason.values.toList.filter(fieldOf(_).isEmpty).map(_.label) == List(
          "account not active",
          "blacklisted",
          "in one of the club's matches",
          "no daily stats"
        )
      )
    },
    test("every criteria field a rejection reason names is one the operator can set") {
      val settable = CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily).productElementNames.toSet
      val named    = RejectionReason.values.toList.flatMap(fieldOf)
      assertTrue(named.filterNot(settable).isEmpty)
    }
  )

  // ==========================================================================
  // Suite: Criteria helper methods (pure)
  // ==========================================================================

  private def suiteCriteriaHelpers = suite("criteria helpers")(
    test("defaultDaily returns expected field values") {
      val criteria = RecruitmentCriteria.defaultDaily
      assertTrue(
        criteria.criteriaId == 0,
        criteria.minDaysSinceRegistration.contains(90),
        criteria.daysSinceLastInvited.contains(180),
        criteria.daysSinceRejected.contains(30),
        !criteria.nationalityExclude,
        criteria.nationalityCountries.isEmpty,
        criteria.excludeClubs.isEmpty,
        criteria.maxClubs.contains(40),
        criteria.excludeSourceAdmins,
        criteria.excludeFormerMembers,
        criteria.dailyMinElo.contains(1000),
        criteria.dailyMaxElo.isEmpty,
        criteria.dailyMinScoreRate.contains(3.0 / 8),
        criteria.dailyMaxScoreRate.contains(7.0 / 8),
        criteria.dailyMinGamesFinished.contains(20),
        criteria.dailyMinTmGamesFinished.contains(10),
        criteria.dailyMaxTimeoutPercent.contains(5.0),
        criteria.dailyMaxTmTimeoutPercent.contains(0.0),
        criteria.dailyMaxHoursPerMove.contains(12),
        criteria.dailyMinOngoingGames.isEmpty,
        criteria.dailyMaxOngoingGames.contains(60),
        criteria.dailyMinOngoingTeamMatches.isEmpty
      )
    }
  )

  // ==========================================================================
  // Suite: Cached criteria (pure), one row per `FigureCriterion`
  // ==========================================================================

  private case class FigureCriterionRow(
    name: String,
    entry: FigureCriterion,
    stalenessDays: Long,
    criteria: RecruitmentCriteria,
    pastLimit: PlayerRecruitmentCache => PlayerRecruitmentCache,
    notPastLimit: PlayerRecruitmentCache => PlayerRecruitmentCache
  )

  private val now: Instant = TestTimes.t0

  private def cacheAged(days: Long): PlayerRecruitmentCache =
    PlayerRecruitmentCache.empty(PlayerId(1), now.minus(days, ChronoUnit.DAYS))

  // A timeout newer than its row: still inside the window at the limit, so the row's age decides.
  private def duringRun(cache: PlayerRecruitmentCache): Option[Instant] =
    Some(cache.fetchedAt.plusSeconds(1))

  private val onWindowEdge: Instant = now.minus(90, ChronoUnit.DAYS)

  private val justOutsideWindow: Option[Instant] = Some(onWindowEdge.minusSeconds(1))

  private def firingUnder(criteria: RecruitmentCriteria, cache: PlayerRecruitmentCache): List[FigureCriterion] =
    FigureCriterion.values.toList.filter(_.rejectsFromCache(FigureCheck(cache, criteria, now)))

  private def cachedCriteriaTests =
    testEveryCriterionHasARow :: testLiveJudgementIgnoresRowAge :: testsTimeoutOnWindowEdgeFires ++
      testsRateFiresAtALimitOfZero ++ figureCriterionRows.flatMap(figureCriterionTests)

  private def testEveryCriterionHasARow = test("every figure criterion has a row") {
    assertTrue(FigureCriterion.values.toList.diff(figureCriterionRows.map(_.entry)).isEmpty)
  }

  private def testLiveJudgementIgnoresRowAge =
    test("a criterion judged live rejects a row past its staleness limit, which it would not reject from cache") {
      val check = FigureCheck(cacheAged(30).copy(clubCount = Some(51)), makeCriteria().copy(maxClubs = Some(50)), now)
      assertTrue(
        FigureCriterion.firstLive(FigureSource.Clubs, check).contains(RejectionReason.MaxClubs),
        FigureCriterion.firstFromCache(check).isEmpty
      )
    }

  private def testsTimeoutOnWindowEdgeFires = List(
    test("zero-tolerance daily timeout: a timeout exactly 90 days old fires") {
      val cache    = cacheAged(0).copy(lastDailyTimeoutAt = Some(onWindowEdge))
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
      assertTrue(FigureCriterion.ZeroToleranceDailyTimeout.rejectsFromCache(FigureCheck(cache, criteria, now)))
    },
    test("zero-tolerance team-match timeout: a timeout exactly 90 days old fires") {
      val cache    = cacheAged(0).copy(lastTmTimeoutAt = Some(onWindowEdge))
      val criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0))
      assertTrue(FigureCriterion.ZeroToleranceTmTimeout.rejectsFromCache(FigureCheck(cache, criteria, now)))
    }
  )

  private def testsRateFiresAtALimitOfZero = List(
    test("max daily timeout %: at a limit of 0, a rate above it fires alone once its timeout has left the window") {
      val cache    = cacheAged(0).copy(dailyTimeoutPct = Some(25.0), lastDailyTimeoutAt = justOutsideWindow)
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
      assertTrue(firingUnder(criteria, cache) == List(FigureCriterion.MaxDailyTimeoutPercent))
    },
    test(
      "max team-match timeout %: at a limit of 0, a rate above it fires alone once its timeout has left the window"
    ) {
      val cache    = cacheAged(0).copy(tmTimeoutPct90d = Some(25.0), lastTmTimeoutAt = justOutsideWindow)
      val criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0))
      assertTrue(firingUnder(criteria, cache) == List(FigureCriterion.MaxTmTimeoutPercent))
    }
  )

  // A row sets the field its reason enforces and no other. The two zero-tolerance checks share their field with a rate
  // check, so each must also record its own reason, which judges a timeout's date.
  private def recordsItsCriterion(row: FigureCriterionRow): Boolean =
    row.entry match {
      case FigureCriterion.ZeroToleranceDailyTimeout => row.entry.reason == RejectionReason.RecentDailyTimeout
      case FigureCriterion.ZeroToleranceTmTimeout    => row.entry.reason == RejectionReason.RecentTmTimeout
      case _ =>
        val base = makeCriteria()
        val setFields = row.criteria.productElementNames.zip(row.criteria.productIterator.zip(base.productIterator))
          .collect { case (name, (value, default)) if value != default => name }
          .toList
        setFields == fieldOf(row.entry.reason).toList
    }

  private def figureCriterionTests(row: FigureCriterionRow) = {
    def firing(cache: PlayerRecruitmentCache): List[FigureCriterion] = firingUnder(row.criteria, cache)
    List(
      test(
        s"${row.name}: a figure past the limit fires only this criterion until the row is ${row.stalenessDays} days old"
      ) {
        assertTrue(
          firing(row.pastLimit(cacheAged(0))) == List(row.entry),
          firing(row.pastLimit(cacheAged(row.stalenessDays - 1))) == List(row.entry),
          firing(row.pastLimit(cacheAged(row.stalenessDays))).isEmpty
        )
      },
      test(s"${row.name}: a figure not past the limit fires nothing") {
        assertTrue(firing(row.notPastLimit(cacheAged(0))).isEmpty)
      },
      test(s"${row.name}: an absent figure fires nothing") {
        assertTrue(firing(cacheAged(0)).isEmpty)
      },
      test(s"${row.name}: a rejection records the reason the criterion it reads would") {
        assertTrue(recordsItsCriterion(row))
      }
    )
  }

  private val figureCriterionRows: List[FigureCriterionRow] = List(
    FigureCriterionRow(
      name = "zero-tolerance daily timeout",
      entry = FigureCriterion.ZeroToleranceDailyTimeout,
      stalenessDays = 90,
      criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0)),
      pastLimit = cache => cache.copy(lastDailyTimeoutAt = duringRun(cache)),
      notPastLimit = _.copy(lastDailyTimeoutAt = justOutsideWindow)
    ),
    FigureCriterionRow(
      name = "zero-tolerance team-match timeout",
      entry = FigureCriterion.ZeroToleranceTmTimeout,
      stalenessDays = 90,
      criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(0.0)),
      pastLimit = cache => cache.copy(lastTmTimeoutAt = duringRun(cache)),
      notPastLimit = _.copy(lastTmTimeoutAt = justOutsideWindow)
    ),
    FigureCriterionRow(
      name = "max clubs",
      entry = FigureCriterion.MaxClubs,
      stalenessDays = 30,
      criteria = makeCriteria().copy(maxClubs = Some(50)),
      pastLimit = _.copy(clubCount = Some(51)),
      notPastLimit = _.copy(clubCount = Some(50))
    ),
    FigureCriterionRow(
      name = "min daily Elo",
      entry = FigureCriterion.MinDailyElo,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinElo = Some(Elo(1000))),
      pastLimit = _.copy(dailyElo = Some(Elo(999))),
      notPastLimit = _.copy(dailyElo = Some(Elo(1000)))
    ),
    FigureCriterionRow(
      name = "max daily Elo",
      entry = FigureCriterion.MaxDailyElo,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxElo = Some(Elo(2000))),
      pastLimit = _.copy(dailyElo = Some(Elo(2001))),
      notPastLimit = _.copy(dailyElo = Some(Elo(2000)))
    ),
    FigureCriterionRow(
      name = "min daily score rate",
      entry = FigureCriterion.MinDailyScoreRate,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinScoreRate = Some(0.5)),
      pastLimit = _.copy(dailyScoreRate = Some(0.49)),
      notPastLimit = _.copy(dailyScoreRate = Some(0.5))
    ),
    FigureCriterionRow(
      name = "max daily score rate",
      entry = FigureCriterion.MaxDailyScoreRate,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxScoreRate = Some(0.8)),
      pastLimit = _.copy(dailyScoreRate = Some(0.81)),
      notPastLimit = _.copy(dailyScoreRate = Some(0.8))
    ),
    FigureCriterionRow(
      name = "max daily timeout %",
      entry = FigureCriterion.MaxDailyTimeoutPercent,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0)),
      pastLimit = _.copy(dailyTimeoutPct = Some(10.5)),
      notPastLimit = _.copy(dailyTimeoutPct = Some(10.0))
    ),
    FigureCriterionRow(
      name = "min daily games finished",
      entry = FigureCriterion.MinDailyGamesFinished,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinGamesFinished = Some(50)),
      pastLimit = _.copy(dailyGamesFinished = Some(49)),
      notPastLimit = _.copy(dailyGamesFinished = Some(50))
    ),
    FigureCriterionRow(
      name = "min ongoing games",
      entry = FigureCriterion.MinOngoingGames,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinOngoingGames = Some(5)),
      pastLimit = _.copy(ongoingGames = Some(4)),
      notPastLimit = _.copy(ongoingGames = Some(5))
    ),
    FigureCriterionRow(
      name = "max ongoing games",
      entry = FigureCriterion.MaxOngoingGames,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxOngoingGames = Some(10)),
      pastLimit = _.copy(ongoingGames = Some(11)),
      notPastLimit = _.copy(ongoingGames = Some(10))
    ),
    FigureCriterionRow(
      name = "min ongoing team matches",
      entry = FigureCriterion.MinOngoingTeamMatches,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinOngoingTeamMatches = Some(2)),
      pastLimit = _.copy(ongoingTeamMatches = Some(1)),
      notPastLimit = _.copy(ongoingTeamMatches = Some(2))
    ),
    FigureCriterionRow(
      name = "min team-match games finished",
      entry = FigureCriterion.MinTmGamesFinished,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(5)),
      pastLimit = _.copy(tmGamesFinished90d = Some(4)),
      notPastLimit = _.copy(tmGamesFinished90d = Some(5))
    ),
    FigureCriterionRow(
      name = "max team-match timeout %",
      entry = FigureCriterion.MaxTmTimeoutPercent,
      stalenessDays = 30,
      criteria = makeCriteria().copy(dailyMaxTmTimeoutPercent = Some(10.0)),
      pastLimit = _.copy(tmTimeoutPct90d = Some(10.5)),
      notPastLimit = _.copy(tmTimeoutPct90d = Some(10.0))
    )
  )
}
