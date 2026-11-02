package ccas.api.misc.enums

import zio.json.{jsonHint, jsonHintNames, JsonDecoder, SnakeCase}

import ccas.utils.sql.EnumSql

@jsonHintNames(SnakeCase)
enum Colour derives JsonDecoder {
  case White
  case Black
}

// Not associated with the Chess.com public api.
enum GameResult(val score: Double) {
  case Win  extends GameResult(1.0)
  case Draw extends GameResult(0.5)
  case Loss extends GameResult(0.0)

  /** [[score]] doubled, so a match's points stay whole. */
  def scoreX2: Int = (score * 2).toInt
}

// Chess.com runs multi-word values together, so a multi-word case needs a hint.
@jsonHintNames(SnakeCase)
enum GameResultDetail(val category: GameResult) derives JsonDecoder {
  val score: Double = category.score

  case Win                                                  extends GameResultDetail(GameResult.Win)
  case Stalemate                                            extends GameResultDetail(GameResult.Draw)
  case Agreed                                               extends GameResultDetail(GameResult.Draw)
  case Repetition                                           extends GameResultDetail(GameResult.Draw)
  @jsonHint("50move") case FiftyMove                        extends GameResultDetail(GameResult.Draw)
  case Insufficient                                         extends GameResultDetail(GameResult.Draw)
  @jsonHint("timevsinsufficient") case TimeVsInsufficient   extends GameResultDetail(GameResult.Draw)
  case Checkmated                                           extends GameResultDetail(GameResult.Loss)
  case Resigned                                             extends GameResultDetail(GameResult.Loss)
  case Timeout                                              extends GameResultDetail(GameResult.Loss)
  case Lose                                                 extends GameResultDetail(GameResult.Loss)
  case Abandoned                                            extends GameResultDetail(GameResult.Loss)
  @jsonHint("kingofthehill") case KingOfTheHill             extends GameResultDetail(GameResult.Loss)
  @jsonHint("threecheck") case ThreeCheck                   extends GameResultDetail(GameResult.Loss)
  @jsonHint("bughousepartnerlose") case BughousePartnerLose extends GameResultDetail(GameResult.Loss)
}

object GameResultDetail extends EnumSql[GameResultDetail]

// Chess.com runs multi-word values together, so a multi-word case needs a hint.
@jsonHintNames(SnakeCase)
enum GameRule derives JsonDecoder {
  case Chess
  case Chess960
  case Bughouse
  @jsonHint("kingofthehill") case KingOfTheHill
  @jsonHint("threecheck") case ThreeCheck
  @jsonHint("crazyhouse") case CrazyHouse
  @jsonHint("oddschess") case OddsChess
}

// Capitalised on the wire, unlike most of Chess.com's enums: docs/chess-com-api.md, "Enum fields are not spelt alike".
enum League derives JsonDecoder {
  case Wood
  case Stone
  case Bronze
  case Silver
  case Crystal
  case Elite
  case Champion
  case Legend
}

@jsonHintNames(SnakeCase)
enum ClubMatchResult(val scorePerPlayer: Double) derives JsonDecoder {
  def totalScore(nPlayers: Int): Double = scorePerPlayer * nPlayers

  case Win  extends ClubMatchResult(5.0)
  case Draw extends ClubMatchResult(2.0)
  case Lose extends ClubMatchResult(0.0)
}

object ClubMatchResult extends EnumSql[ClubMatchResult]

@jsonHintNames(SnakeCase)
enum ClubMatchStatus derives JsonDecoder {
  case Finished
  case InProgress
  case Registration
  case Aborted
}

object ClubMatchStatus extends EnumSql[ClubMatchStatus]

enum PlayerStatusCategory {
  case Active
  case Closed
  case Fairplay
  case Abuse
  case Unknown
}

object PlayerStatusCategory extends EnumSql[PlayerStatusCategory]

@jsonHintNames(SnakeCase)
enum PlayerStatus(val category: PlayerStatusCategory) derives JsonDecoder {
  case Basic                                             extends PlayerStatus(PlayerStatusCategory.Active)
  case Premium                                           extends PlayerStatus(PlayerStatusCategory.Active)
  case Mod                                               extends PlayerStatus(PlayerStatusCategory.Active)
  case Staff                                             extends PlayerStatus(PlayerStatusCategory.Active)
  case Closed                                            extends PlayerStatus(PlayerStatusCategory.Closed)
  @jsonHint("closed:fair_play_violations") case Fairplay extends PlayerStatus(PlayerStatusCategory.Fairplay)
  @jsonHint("closed:abuse") case Abuse                   extends PlayerStatus(PlayerStatusCategory.Abuse)
}

@jsonHintNames(SnakeCase)
enum TimeClass(val isDaily: Boolean) derives JsonDecoder {
  case Daily     extends TimeClass(true)
  case Rapid     extends TimeClass(false)
  case Standard  extends TimeClass(false)
  case Blitz     extends TimeClass(false)
  case Lightning extends TimeClass(false)
  case Bullet    extends TimeClass(false)
}

object TimeClass extends EnumSql[TimeClass]

enum Title derives JsonDecoder {
  case GM
  case IM
  case FM
  case CM
  case NM
  case WGM
  case WIM
  case WFM
  case WCM
  case WNM
  case M
}

object Title extends EnumSql[Title]

@jsonHintNames(SnakeCase)
enum ClubVisibility derives JsonDecoder {
  case Public
  case Private
}

enum BoardGameWinner {
  case Team1, Team2, Draw

  /** The game's result for team 1. */
  def team1Result: GameResult = this match {
    case Team1 => GameResult.Win
    case Team2 => GameResult.Loss
    case Draw  => GameResult.Draw
  }
}

object BoardGameWinner extends EnumSql[BoardGameWinner]
