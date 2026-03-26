package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

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
  excludeFormerMembers: Boolean,
  dailyMinElo: Option[Int],
  dailyMaxElo: Option[Int],
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
}

object RecruitmentCriteria {
  val MaxDaysSinceLookback: Int = 180

  private val selectCols = SqlLiteral(
    """criteria_id,
       min_days_since_registration, days_since_last_invited, days_since_rejected,
       nationality_exclude, nationality_countries,
       exclude_clubs, max_clubs, exclude_source_admins, exclude_former_members,
       daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
       daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
       daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches"""
  )

  def createTable: ZIO[Transactor, SQLException, Int] =
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
              exclude_former_members         BOOLEAN NOT NULL,
              daily_min_elo                  INT,
              daily_max_elo                  INT,
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

  def selectId(criteriaId: Long): ZIO[Transactor, SQLException, Option[RecruitmentCriteria]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_criteria WHERE criteria_id = $criteriaId"
        .query[RecruitmentCriteria].run().headOption
    }

  def insert(item: RecruitmentCriteria): ZIO[Transactor, SQLException, Long] = {
    val criteria = item.capped
    connectZIO {
      sql"""INSERT INTO recruitment_criteria (
              min_days_since_registration, days_since_last_invited, days_since_rejected,
              nationality_exclude, nationality_countries,
              exclude_clubs, max_clubs, exclude_source_admins, exclude_former_members,
              daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
              daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
              daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
            ) VALUES (
              ${criteria.minDaysSinceRegistration}, ${criteria.daysSinceLastInvited}, ${criteria.daysSinceRejected},
              ${criteria.nationalityExclude}, ${criteria.nationalityCountries},
              ${criteria.excludeClubs}, ${criteria.maxClubs}, ${criteria.excludeSourceAdmins}, ${criteria.excludeFormerMembers},
              ${criteria.dailyMinElo}, ${criteria.dailyMaxElo}, ${criteria.dailyMinGamesFinished}, ${criteria.dailyMinTmGamesFinished},
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
      excludeFormerMembers = true,
      dailyMinElo = Some(1000),
      dailyMaxElo = None,
      dailyMinGamesFinished = Some(20),
      dailyMinTmGamesFinished = Some(10),
      dailyMaxTimeoutPercent = Some(5.0),
      dailyMaxTmTimeoutPercent = Some(0.0),
      dailyMaxHoursPerMove = Some(12),
      dailyMinOngoingGames = None,
      dailyMaxOngoingGames = Some(60),
      dailyMinOngoingTeamMatches = None
    )

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_criteria".update.run()
    }
}
