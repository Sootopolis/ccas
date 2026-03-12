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
    maxCandidates: Int,
    sourceClubs: List[String],
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
    daysSinceLastInvited: Option[Int])
    derives DbCodec {
  def sourceClubNames: List[ClubUrlName]  = sourceClubs.map(ClubUrlName.wrap)
  def excludeClubNames: List[ClubUrlName] = excludeClubs.map(ClubUrlName.wrap)
}

object RecruitmentConfig {
  private val selectCols = SqlLiteral(
    """club_id, config_name, max_candidates, source_clubs, exclude_clubs, on_exhaustion,
       nationality_mode, nationality_countries, max_clubs,
       daily_max_timeout_percent, daily_max_tm_timeout_percent,
       daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches,
       daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
       min_days_since_registration, days_since_last_invited"""
  )

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_config (
              club_id                        BIGINT NOT NULL,
              config_name                    VARCHAR NOT NULL,
              max_candidates                 INT NOT NULL,
              source_clubs                   TEXT[] NOT NULL DEFAULT '{}',
              exclude_clubs                  TEXT[] NOT NULL DEFAULT '{}',
              on_exhaustion                  VARCHAR NOT NULL DEFAULT 'Stop',
              nationality_mode               VARCHAR,
              nationality_countries          TEXT[] NOT NULL DEFAULT '{}',
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
              PRIMARY KEY (club_id, config_name)
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
              club_id, config_name, max_candidates, source_clubs, exclude_clubs, on_exhaustion,
              nationality_mode, nationality_countries, max_clubs,
              daily_max_timeout_percent, daily_max_tm_timeout_percent,
              daily_min_ongoing_games, daily_max_ongoing_games, daily_min_ongoing_team_matches,
              daily_min_elo, daily_max_elo, daily_min_games_finished, daily_min_tm_games_finished,
              min_days_since_registration, days_since_last_invited
            ) VALUES (
              ${item.clubId}, ${item.configName}, ${item.maxCandidates},
              ${item.sourceClubs}, ${item.excludeClubs}, ${item.onExhaustion.toString},
              ${item.nationalityMode}, ${item.nationalityCountries}, ${item.maxClubs},
              ${item.dailyMaxTimeoutPercent}, ${item.dailyMaxTmTimeoutPercent},
              ${item.dailyMinOngoingGames}, ${item.dailyMaxOngoingGames}, ${item.dailyMinOngoingTeamMatches},
              ${item.dailyMinElo}, ${item.dailyMaxElo}, ${item.dailyMinGamesFinished}, ${item.dailyMinTmGamesFinished},
              ${item.minDaysSinceRegistration}, ${item.daysSinceLastInvited}
            ) ON CONFLICT (club_id, config_name) DO UPDATE SET
              max_candidates = EXCLUDED.max_candidates,
              source_clubs = EXCLUDED.source_clubs,
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
              days_since_last_invited = EXCLUDED.days_since_last_invited""".update.run()
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_config".update.run()
    }
}
