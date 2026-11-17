package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, Elo}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class RecruitmentCriteria(
  criteriaId: Long,
  minDaysSinceRegistration: Option[Int],
  daysSinceLastInvited: Option[Int],
  daysSinceRejected: Option[Int],
  nationalityExclude: Boolean,
  nationalityCountries: List[String],
  excludeClubs: List[ClubId],
  maxClubs: Option[Int],
  excludeSourceAdmins: Boolean,
  avoidAdminMinClubSize: Option[Int],
  excludeFormerMembers: Boolean,
  dailyMinElo: Option[Elo],
  dailyMaxElo: Option[Elo],
  dailyMinScoreRate: Option[Double],
  dailyMaxScoreRate: Option[Double],
  dailyMinGamesFinished: Option[Int],
  dailyMinTmGamesFinished: Option[Int],
  dailyMaxTimeoutPercent: Option[Double],
  dailyMaxTmTimeoutPercent: Option[Double],
  dailyMaxHoursPerMove: Option[Int],
  dailyMinOngoingGames: Option[Int],
  dailyMaxOngoingGames: Option[Int],
  dailyMinOngoingTeamMatches: Option[Int]
) derives DbCodec {
  def capped: RecruitmentCriteria = copy(
    daysSinceLastInvited = daysSinceLastInvited.map(_.min(RecruitmentCriteria.MaxDaysSinceLookback)),
    daysSinceRejected = daysSinceRejected.map(_.min(RecruitmentCriteria.MaxDaysSinceLookback))
  )

  /** What the [[RecruitmentCriteria.Rules]] for `setting` find wrong with it here, as one message: the CLI asks for that
    * setting again until there is none.
    */
  def problemsAt(setting: String): Option[String] =
    problems(RecruitmentCriteria.Rules.filter(_.setting == setting))

  /** Every one of the [[RecruitmentCriteria.Rules]] these settings break, as one message. ([[capped]] applies the
    * 180-day lookback cap, inside `insert`.) Surfaced as a `BadRequestException` by callers.
    */
  def validate: Either[String, Unit] =
    problems(RecruitmentCriteria.Rules).toLeft(())

  private def problems(rules: List[RecruitmentCriteria.Rule]): Option[String] =
    rules.flatMap(_.check(this)) match {
      case Nil   => None
      case found => Some(found.mkString("; "))
    }
}

object RecruitmentCriteria {
  val MaxDaysSinceLookback: Int = 180

  /** A rule the settings are held to, and the setting it is answered at, where a problem with it is reported. */
  final case class Rule(setting: String, check: RecruitmentCriteria => Option[String])

  /** Every rule, beyond what the settings' types already refuse: the one list, which the server saves by and the CLI
    * asks by. A min/max pair is answered at its max, which is asked after the min, so it is judged once both are in.
    */
  val Rules: List[Rule] = List(
    nonNeg("minDaysSinceRegistration", _.minDaysSinceRegistration),
    nonNeg("daysSinceLastInvited", _.daysSinceLastInvited),
    nonNeg("daysSinceRejected", _.daysSinceRejected),
    nonNeg("maxClubs", _.maxClubs),
    nonNeg("avoidAdminMinClubSize", _.avoidAdminMinClubSize),
    nonNeg("dailyMinGamesFinished", _.dailyMinGamesFinished),
    nonNeg("dailyMinTmGamesFinished", _.dailyMinTmGamesFinished),
    nonNeg("dailyMaxHoursPerMove", _.dailyMaxHoursPerMove),
    nonNeg("dailyMinOngoingGames", _.dailyMinOngoingGames),
    nonNeg("dailyMaxOngoingGames", _.dailyMaxOngoingGames),
    nonNeg("dailyMinOngoingTeamMatches", _.dailyMinOngoingTeamMatches),
    inUnit("dailyMinScoreRate", _.dailyMinScoreRate),
    inUnit("dailyMaxScoreRate", _.dailyMaxScoreRate),
    inPercent("dailyMaxTimeoutPercent", _.dailyMaxTimeoutPercent),
    inPercent("dailyMaxTmTimeoutPercent", _.dailyMaxTmTimeoutPercent),
    ordered("dailyMaxElo", "dailyElo", _.dailyMinElo, _.dailyMaxElo),
    ordered("dailyMaxScoreRate", "dailyScoreRate", _.dailyMinScoreRate, _.dailyMaxScoreRate),
    ordered("dailyMaxOngoingGames", "dailyOngoingGames", _.dailyMinOngoingGames, _.dailyMaxOngoingGames)
  )

  private def nonNeg(setting: String, value: RecruitmentCriteria => Option[Int]): Rule =
    Rule(setting, criteria => value(criteria).filter(_ < 0).map(x => s"$setting must be >= 0 (got $x)"))

  private def inUnit(setting: String, value: RecruitmentCriteria => Option[Double]): Rule =
    Rule(
      setting,
      criteria => value(criteria).filter(d => d < 0.0 || d > 1.0).map(x => s"$setting must be in [0, 1] (got $x)")
    )

  private def inPercent(setting: String, value: RecruitmentCriteria => Option[Double]): Rule =
    Rule(
      setting,
      criteria => value(criteria).filter(d => d < 0.0 || d > 100.0).map(x => s"$setting must be in [0, 100] (got $x)")
    )

  private def ordered[A](
    setting: String,
    pair: String,
    min: RecruitmentCriteria => Option[A],
    max: RecruitmentCriteria => Option[A]
  )(using ord: Ordering[A]): Rule =
    Rule(
      setting,
      criteria =>
        (min(criteria), max(criteria)) match {
          case (Some(lo), Some(hi)) if ord.gt(lo, hi) => Some(s"$pair min ($lo) must be <= max ($hi)")
          case _                                      => None
        }
    )

  private val selectCols = SqlLiteral(
    """criteria_id,
       min_days_since_registration, days_since_last_invited, days_since_rejected,
       nationality_exclude, nationality_countries,
       exclude_clubs, max_clubs, exclude_source_admins, avoid_admin_min_club_size, exclude_former_members,
       daily_min_elo, daily_max_elo, daily_min_score_rate, daily_max_score_rate,
       daily_min_games_finished, daily_min_tm_games_finished,
       daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
       daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches"""
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_criteria (
              criteria_id                    BIGSERIAL PRIMARY KEY,
              min_days_since_registration    INT,
              days_since_last_invited        INT,
              days_since_rejected            INT,
              nationality_exclude            BOOLEAN NOT NULL,
              nationality_countries          TEXT[] NOT NULL,
              exclude_clubs                  BIGINT[] NOT NULL,
              max_clubs                      INT,
              exclude_source_admins          BOOLEAN NOT NULL,
              avoid_admin_min_club_size      INT,
              exclude_former_members         BOOLEAN NOT NULL,
              daily_min_elo                  INT,
              daily_max_elo                  INT,
              daily_min_score_rate           DOUBLE PRECISION,
              daily_max_score_rate           DOUBLE PRECISION,
              daily_min_games_finished       INT,
              daily_min_tm_games_finished    INT,
              daily_max_timeout_percent      DOUBLE PRECISION,
              daily_max_tm_timeout_percent   DOUBLE PRECISION,
              daily_max_hours_per_move       INT,
              daily_min_ongoing_games        INT,
              daily_max_ongoing_games        INT,
              daily_min_ongoing_team_matches INT
            )""".update.run()
    }

  def selectId(criteriaId: Long): ZIO[PostgresClient, SQLException, Option[RecruitmentCriteria]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_criteria WHERE criteria_id = $criteriaId"
        .query[RecruitmentCriteria].run().headOption
    }

  def insert(item: RecruitmentCriteria): ZIO[PostgresClient, SQLException, Long] = {
    val criteria = item.capped
    connectZIO {
      sql"""INSERT INTO recruitment_criteria (
              min_days_since_registration, days_since_last_invited, days_since_rejected,
              nationality_exclude, nationality_countries,
              exclude_clubs, max_clubs, exclude_source_admins, avoid_admin_min_club_size, exclude_former_members,
              daily_min_elo, daily_max_elo, daily_min_score_rate, daily_max_score_rate,
              daily_min_games_finished, daily_min_tm_games_finished,
              daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
              daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
            ) VALUES (
              ${criteria.minDaysSinceRegistration}, ${criteria.daysSinceLastInvited}, ${criteria.daysSinceRejected},
              ${criteria.nationalityExclude}, ${criteria.nationalityCountries},
              ${criteria.excludeClubs}, ${criteria.maxClubs}, ${criteria.excludeSourceAdmins}, ${criteria.avoidAdminMinClubSize}, ${criteria.excludeFormerMembers},
              ${criteria.dailyMinElo}, ${criteria.dailyMaxElo}, ${criteria.dailyMinScoreRate}, ${criteria.dailyMaxScoreRate},
              ${criteria.dailyMinGamesFinished}, ${criteria.dailyMinTmGamesFinished},
              ${criteria.dailyMaxTimeoutPercent}, ${criteria.dailyMaxTmTimeoutPercent}, ${criteria.dailyMaxHoursPerMove},
              ${criteria.dailyMinOngoingGames}, ${criteria.dailyMaxOngoingGames}, ${criteria.dailyMinOngoingTeamMatches}
            ) RETURNING criteria_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))
  }

  def defaultDaily: RecruitmentCriteria =
    RecruitmentCriteria(
      criteriaId = 0,
      minDaysSinceRegistration = Some(90),
      daysSinceLastInvited = Some(180),
      daysSinceRejected = Some(30),
      nationalityExclude = false,
      nationalityCountries = Nil,
      excludeClubs = Nil,
      maxClubs = Some(40),
      excludeSourceAdmins = true,
      avoidAdminMinClubSize = None,
      excludeFormerMembers = true,
      dailyMinElo = Some(Elo(1000)),
      dailyMaxElo = None,
      dailyMinScoreRate = Some(3.0 / 8),
      dailyMaxScoreRate = Some(7.0 / 8),
      dailyMinGamesFinished = Some(20),
      dailyMinTmGamesFinished = Some(10),
      dailyMaxTimeoutPercent = Some(5.0),
      dailyMaxTmTimeoutPercent = Some(0.0),
      dailyMaxHoursPerMove = Some(12),
      dailyMinOngoingGames = None,
      dailyMaxOngoingGames = Some(60),
      dailyMinOngoingTeamMatches = None
    )
}
