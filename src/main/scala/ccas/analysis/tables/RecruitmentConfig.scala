package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.apps.recruitment.ExhaustionBehavior
import ccas.analysis.apps.recruitment.ExhaustionBehavior.given
import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class RecruitmentConfig(
    clubId: ClubId,
    configName: String,
    excludeClubs: List[String],
    onExhaustion: ExhaustionBehavior,
    nationalityMode: Option[String],
    nationalityCountries: List[String],
    maxClubs: Option[Int],
    dailyMaxTimeoutPercent: Option[Double],
    dailyMaxTmTimeoutPercent: Option[Double],
    dailyMinOngoingGames: Option[Int],
    dailyMaxOngoingGames: Option[Int],
    dailyMinOngoingTeamMatches: Option[Int],
    dailyMinElo: Option[Int],
    dailyMaxElo: Option[Int],
    dailyMinGamesFinished: Option[Int],
    dailyMinTmGamesFinished: Option[Int],
    minDaysSinceRegistration: Option[Int],
    daysSinceLastInvited: Option[Int],
    dailyMaxHoursPerMove: Option[Int],
    excludeSourceAdmins: Boolean,
    excludeFormerMembers: Boolean)
    derives DbCodec {
  def excludeClubNames: List[ClubUrlName] = excludeClubs.map(ClubUrlName.wrap)
}

object RecruitmentConfig {
  private val selectCols = SqlLiteral(
    """club_id, config_name, exclude_clubs, on_exhaustion,
       nationality_mode, nationality_countries, max_clubs,
       daily_max_timeout_percent, daily_max_tm_timeout_percent,
       daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches,
       daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
       min_days_since_registration, days_since_last_invited, daily_max_hours_per_move,
       exclude_source_admins, exclude_former_members"""
  )

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_config (
              club_id                        BIGINT NOT NULL,
              config_name                    VARCHAR NOT NULL,
              exclude_clubs                  TEXT[] NOT NULL,
              on_exhaustion                  VARCHAR NOT NULL CHECK (on_exhaustion IN ('Stop', 'Explore')),
              nationality_mode               VARCHAR,
              nationality_countries          TEXT[] NOT NULL,
              max_clubs                      INT,
              daily_max_timeout_percent      DOUBLE PRECISION,
              daily_max_tm_timeout_percent   DOUBLE PRECISION,
              daily_min_ongoing_games        INT,
              daily_max_ongoing_games        INT,
              daily_min_ongoing_team_matches INT,
              daily_min_elo                  INT,
              daily_max_elo                  INT,
              daily_min_games_finished       INT,
              daily_min_tm_games_finished    INT,
              min_days_since_registration    INT,
              days_since_last_invited        INT,
              daily_max_hours_per_move       INT,
              exclude_source_admins          BOOLEAN NOT NULL,
              exclude_former_members         BOOLEAN NOT NULL,
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

  def upsert(item: RecruitmentConfig): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_config (
              club_id, config_name, exclude_clubs, on_exhaustion,
              nationality_mode, nationality_countries, max_clubs,
              daily_max_timeout_percent, daily_max_tm_timeout_percent,
              daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches,
              daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
              min_days_since_registration, days_since_last_invited, daily_max_hours_per_move,
              exclude_source_admins, exclude_former_members
            ) VALUES (
              ${item.clubId}, ${item.configName},
              ${item.excludeClubs}, ${item.onExhaustion.toString},
              ${item.nationalityMode}, ${item.nationalityCountries}, ${item.maxClubs},
              ${item.dailyMaxTimeoutPercent}, ${item.dailyMaxTmTimeoutPercent},
              ${item.dailyMinOngoingGames}, ${item.dailyMaxOngoingGames}, ${item.dailyMinOngoingTeamMatches},
              ${item.dailyMinElo}, ${item.dailyMaxElo}, ${item.dailyMinGamesFinished}, ${item.dailyMinTmGamesFinished},
              ${item.minDaysSinceRegistration}, ${item.daysSinceLastInvited}, ${item.dailyMaxHoursPerMove},
              ${item.excludeSourceAdmins}, ${item.excludeFormerMembers}
            ) ON CONFLICT (club_id, config_name) DO UPDATE SET
              exclude_clubs = EXCLUDED.exclude_clubs,
              on_exhaustion = EXCLUDED.on_exhaustion,
              nationality_mode = EXCLUDED.nationality_mode,
              nationality_countries = EXCLUDED.nationality_countries,
              max_clubs = EXCLUDED.max_clubs,
              daily_max_timeout_percent = EXCLUDED.daily_max_timeout_percent,
              daily_max_tm_timeout_percent = EXCLUDED.daily_max_tm_timeout_percent,
              daily_min_ongoing_games = EXCLUDED.daily_min_ongoing_games,
              daily_max_ongoing_games = EXCLUDED.daily_max_ongoing_games,
              daily_min_ongoing_team_matches = EXCLUDED.daily_min_ongoing_team_matches,
              daily_min_elo = EXCLUDED.daily_min_elo,
              daily_max_elo = EXCLUDED.daily_max_elo,
              daily_min_games_finished = EXCLUDED.daily_min_games_finished,
              daily_min_tm_games_finished = EXCLUDED.daily_min_tm_games_finished,
              min_days_since_registration = EXCLUDED.min_days_since_registration,
              days_since_last_invited = EXCLUDED.days_since_last_invited,
              daily_max_hours_per_move = EXCLUDED.daily_max_hours_per_move,
              exclude_source_admins = EXCLUDED.exclude_source_admins,
              exclude_former_members = EXCLUDED.exclude_former_members""".update.run()
    }

  def defaultDaily(clubId: ClubId): RecruitmentConfig =
    RecruitmentConfig(
      clubId                    = clubId,
      configName                = "daily",
      excludeClubs              = Nil,
      onExhaustion              = ExhaustionBehavior.Explore,
      nationalityMode           = None,
      nationalityCountries      = Nil,
      maxClubs                  = Some(40),
      dailyMaxTimeoutPercent    = Some(5.0),
      dailyMaxTmTimeoutPercent  = Some(0.0),
      dailyMinOngoingGames      = None,
      dailyMaxOngoingGames      = Some(60),
      dailyMinOngoingTeamMatches = None,
      dailyMinElo               = Some(1000),
      dailyMaxElo               = None,
      dailyMinGamesFinished     = Some(20),
      dailyMinTmGamesFinished   = Some(10),
      minDaysSinceRegistration  = Some(90),
      daysSinceLastInvited      = Some(180),
      dailyMaxHoursPerMove      = Some(12),
      excludeSourceAdmins       = true,
      excludeFormerMembers      = true
    )

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_config".update.run()
    }
}
