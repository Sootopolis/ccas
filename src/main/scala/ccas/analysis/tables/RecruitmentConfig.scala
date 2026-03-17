package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class RecruitmentConfig(
    clubId: ClubId,
    configName: String,
    minDaysSinceRegistration: Option[Int],
    daysSinceLastInvited: Option[Int],
    daysSinceRejected: Option[Int],
    nationalityExclude: Boolean,
    nationalityCountries: List[String],
    excludeClubs: List[String],
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
    dailyMinOngoingTeamMatches: Option[Int])
    derives DbCodec {
  def excludeClubNames: List[ClubUrlName] = excludeClubs.map(ClubUrlName.wrap)

  def capped: RecruitmentConfig = copy(
    daysSinceLastInvited = daysSinceLastInvited.map(_.min(RecruitmentConfig.MaxDaysSinceLookback)),
    daysSinceRejected = daysSinceRejected.map(_.min(RecruitmentConfig.MaxDaysSinceLookback))
  )
}

object RecruitmentConfig {
  val MaxDaysSinceLookback: Int = 180

  private val selectCols = SqlLiteral(
    """club_id, config_name,
       min_days_since_registration, days_since_last_invited, days_since_rejected,
       nationality_exclude, nationality_countries,
       exclude_clubs, max_clubs, exclude_source_admins, exclude_former_members,
       daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
       daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
       daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches"""
  )

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_config (
              club_id                        BIGINT NOT NULL,
              config_name                    VARCHAR NOT NULL,
              min_days_since_registration    INT,
              days_since_last_invited        INT,
              days_since_rejected            INT,
              nationality_exclude            BOOLEAN NOT NULL,
              nationality_countries          TEXT[] NOT NULL,
              exclude_clubs                  TEXT[] NOT NULL,
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
              daily_min_ongoing_team_matches INT,
              PRIMARY KEY (club_id, config_name),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
    }

  def select(clubId: ClubId, configName: String): ZIO[Transactor, SQLException, Option[RecruitmentConfig]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_config WHERE club_id = $clubId AND config_name = $configName"
        .query[RecruitmentConfig].run().headOption
    }

  def selectClub(clubId: ClubId): ZIO[Transactor, SQLException, List[RecruitmentConfig]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_config WHERE club_id = $clubId".query[RecruitmentConfig].run().toList
    }

  def upsert(item: RecruitmentConfig): ZIO[Transactor, SQLException, Int] = {
    val c = item.capped
    connectZIO {
      sql"""INSERT INTO recruitment_config (
              club_id, config_name,
              min_days_since_registration, days_since_last_invited, days_since_rejected,
              nationality_exclude, nationality_countries,
              exclude_clubs, max_clubs, exclude_source_admins, exclude_former_members,
              daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
              daily_max_timeout_percent, daily_max_tm_timeout_percent, daily_max_hours_per_move,
              daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches
            ) VALUES (
              ${c.clubId}, ${c.configName},
              ${c.minDaysSinceRegistration}, ${c.daysSinceLastInvited}, ${c.daysSinceRejected},
              ${c.nationalityExclude}, ${c.nationalityCountries},
              ${c.excludeClubs}, ${c.maxClubs}, ${c.excludeSourceAdmins}, ${c.excludeFormerMembers},
              ${c.dailyMinElo}, ${c.dailyMaxElo}, ${c.dailyMinGamesFinished}, ${c.dailyMinTmGamesFinished},
              ${c.dailyMaxTimeoutPercent}, ${c.dailyMaxTmTimeoutPercent}, ${c.dailyMaxHoursPerMove},
              ${c.dailyMinOngoingGames}, ${c.dailyMaxOngoingGames}, ${c.dailyMinOngoingTeamMatches}
            ) ON CONFLICT (club_id, config_name) DO UPDATE SET
              min_days_since_registration = EXCLUDED.min_days_since_registration,
              days_since_last_invited = EXCLUDED.days_since_last_invited,
              days_since_rejected = EXCLUDED.days_since_rejected,
              nationality_exclude = EXCLUDED.nationality_exclude,
              nationality_countries = EXCLUDED.nationality_countries,
              exclude_clubs = EXCLUDED.exclude_clubs,
              max_clubs = EXCLUDED.max_clubs,
              exclude_source_admins = EXCLUDED.exclude_source_admins,
              exclude_former_members = EXCLUDED.exclude_former_members,
              daily_min_elo = EXCLUDED.daily_min_elo,
              daily_max_elo = EXCLUDED.daily_max_elo,
              daily_min_games_finished = EXCLUDED.daily_min_games_finished,
              daily_min_tm_games_finished = EXCLUDED.daily_min_tm_games_finished,
              daily_max_timeout_percent = EXCLUDED.daily_max_timeout_percent,
              daily_max_tm_timeout_percent = EXCLUDED.daily_max_tm_timeout_percent,
              daily_max_hours_per_move = EXCLUDED.daily_max_hours_per_move,
              daily_min_ongoing_games = EXCLUDED.daily_min_ongoing_games,
              daily_max_ongoing_games = EXCLUDED.daily_max_ongoing_games,
              daily_min_ongoing_team_matches = EXCLUDED.daily_min_ongoing_team_matches""".update.run()
    }
  }

  def defaultDaily(clubId: ClubId): RecruitmentConfig =
    RecruitmentConfig(
      clubId                    = clubId,
      configName                = "daily",
      minDaysSinceRegistration  = Some(90),
      daysSinceLastInvited      = Some(180),
      daysSinceRejected         = Some(30),
      nationalityExclude        = false,
      nationalityCountries      = Nil,
      excludeClubs              = Nil,
      maxClubs                  = Some(40),
      excludeSourceAdmins       = true,
      excludeFormerMembers      = true,
      dailyMinElo               = Some(1000),
      dailyMaxElo               = None,
      dailyMinGamesFinished     = Some(20),
      dailyMinTmGamesFinished   = Some(10),
      dailyMaxTimeoutPercent    = Some(5.0),
      dailyMaxTmTimeoutPercent  = Some(0.0),
      dailyMaxHoursPerMove      = Some(12),
      dailyMinOngoingGames      = None,
      dailyMaxOngoingGames      = Some(60),
      dailyMinOngoingTeamMatches = None
    )

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_config".update.run()
    }
}
