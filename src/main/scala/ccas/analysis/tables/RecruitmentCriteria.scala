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

  /** Cross-field sanity checks beyond the per-field opaque-type validation. ([[capped]] applies the 180-day lookback
    * cap, inside `insert`.) Surfaced as a `BadRequestException` by callers.
    */
  def validate: Either[String, Unit] = {
    def nonNeg(label: String, v: Option[Int]): Option[String] =
      v.filter(_ < 0).map(x => s"$label must be >= 0 (got $x)")
    def inUnit(label: String, v: Option[Double]): Option[String] =
      v.filter(d => d < 0.0 || d > 1.0).map(x => s"$label must be in [0, 1] (got $x)")
    def inPercent(label: String, v: Option[Double]): Option[String] =
      v.filter(d => d < 0.0 || d > 100.0).map(x => s"$label must be in [0, 100] (got $x)")
    def ordered[A](label: String, lo: Option[A], hi: Option[A])(using ord: Ordering[A]): Option[String] =
      (lo, hi) match {
        case (Some(l), Some(h)) if ord.gt(l, h) => Some(s"$label min ($l) must be <= max ($h)")
        case _                                  => None
      }
    val errors = List(
      nonNeg("minDaysSinceRegistration", minDaysSinceRegistration),
      nonNeg("daysSinceLastInvited", daysSinceLastInvited),
      nonNeg("daysSinceRejected", daysSinceRejected),
      nonNeg("maxClubs", maxClubs),
      nonNeg("avoidAdminMinClubSize", avoidAdminMinClubSize),
      nonNeg("dailyMinGamesFinished", dailyMinGamesFinished),
      nonNeg("dailyMinTmGamesFinished", dailyMinTmGamesFinished),
      nonNeg("dailyMaxHoursPerMove", dailyMaxHoursPerMove),
      nonNeg("dailyMinOngoingGames", dailyMinOngoingGames),
      nonNeg("dailyMaxOngoingGames", dailyMaxOngoingGames),
      nonNeg("dailyMinOngoingTeamMatches", dailyMinOngoingTeamMatches),
      inUnit("dailyMinScoreRate", dailyMinScoreRate),
      inUnit("dailyMaxScoreRate", dailyMaxScoreRate),
      inPercent("dailyMaxTimeoutPercent", dailyMaxTimeoutPercent),
      inPercent("dailyMaxTmTimeoutPercent", dailyMaxTmTimeoutPercent),
      ordered("dailyElo", dailyMinElo, dailyMaxElo),
      ordered("dailyScoreRate", dailyMinScoreRate, dailyMaxScoreRate),
      ordered("dailyOngoingGames", dailyMinOngoingGames, dailyMaxOngoingGames)
    ).flatten
    if (errors.isEmpty) Right(()) else Left(errors.mkString("; "))
  }
}

object RecruitmentCriteria {
  val MaxDaysSinceLookback: Int = 180

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
