package ccas.api.misc.enums

import ccas.utils.json.EnumJson
import ccas.utils.sql.EnumSql

enum Colour {
  case White
  case Black
}

object Colour extends EnumJson[Colour]

// Not associated with the Chess.com public api.
enum GameResult(val score: Double) {
  case Win  extends GameResult(1.0)
  case Draw extends GameResult(0.5)
  case Loss extends GameResult(0.0)
}

object GameResult extends EnumJson[GameResult]

enum GameResultDetail(val category: GameResult) {
  val score: Double = category.score

  case Win                 extends GameResultDetail(GameResult.Win)
  case Stalemate           extends GameResultDetail(GameResult.Draw)
  case Agreed              extends GameResultDetail(GameResult.Draw)
  case Repetition          extends GameResultDetail(GameResult.Draw)
  case FiftyMove           extends GameResultDetail(GameResult.Draw)
  case Insufficient        extends GameResultDetail(GameResult.Draw)
  case TimeVsInsufficient  extends GameResultDetail(GameResult.Draw)
  case Checkmated          extends GameResultDetail(GameResult.Loss)
  case Resigned            extends GameResultDetail(GameResult.Loss)
  case Timeout             extends GameResultDetail(GameResult.Loss)
  case Lose                extends GameResultDetail(GameResult.Loss)
  case Abandoned           extends GameResultDetail(GameResult.Loss)
  case KingOfTheHill       extends GameResultDetail(GameResult.Loss)
  case ThreeCheck          extends GameResultDetail(GameResult.Loss)
  case BughousePartnerLose extends GameResultDetail(GameResult.Loss)
}

object GameResultDetail extends EnumJson[GameResultDetail] with EnumSql[GameResultDetail] {
  private val lookup = lookupJson(GameResultDetail.values.map { member =>
    val apiString = member match {
      case FiftyMove => "50move"
      case other     => other.toString.toLowerCase
    }
    apiString -> member
  }.toMap)

  override protected def jsonToEnum(string: String) = lookup(string)
}

enum GameRule {
  case Chess
  case Chess960
  case Bughouse
  case KingOfTheHill
  case ThreeCheck
  case CrazyHouse
  case OddsChess
}

object GameRule extends EnumJson[GameRule] {
  private val lookup = lookupJson(values.map(member => member.toString.toLowerCase -> member).toMap)

  override protected def jsonToEnum(string: String) = lookup(string)
}

enum League {
  case Wood
  case Stone
  case Bronze
  case Silver
  case Crystal
  case Elite
  case Champion
  case Legend
}

object League extends EnumJson[League]

enum ClubMatchResult(val scorePerPlayer: Double) {
  def totalScore(nPlayers: Int): Double = scorePerPlayer * nPlayers

  case Win  extends ClubMatchResult(5.0)
  case Draw extends ClubMatchResult(2.0)
  case Lose extends ClubMatchResult(0.0)
}

object ClubMatchResult extends EnumJson[ClubMatchResult] with EnumSql[ClubMatchResult]

enum ClubMatchStatus {
  case Finished
  case InProgress
  case Registration
  case Aborted
}

object ClubMatchStatus extends EnumJson[ClubMatchStatus] with EnumSql[ClubMatchStatus]

enum PlayerStatusCategory {
  case Active
  case Closed
  case Fairplay
  case Abuse
  case Unknown
}

object PlayerStatusCategory extends EnumSql[PlayerStatusCategory]

enum PlayerStatus(val category: PlayerStatusCategory) {
  case Basic    extends PlayerStatus(PlayerStatusCategory.Active)
  case Premium  extends PlayerStatus(PlayerStatusCategory.Active)
  case Mod      extends PlayerStatus(PlayerStatusCategory.Active)
  case Staff    extends PlayerStatus(PlayerStatusCategory.Active)
  case Closed   extends PlayerStatus(PlayerStatusCategory.Closed)
  case Fairplay extends PlayerStatus(PlayerStatusCategory.Fairplay)
  case Abuse    extends PlayerStatus(PlayerStatusCategory.Abuse)
}

object PlayerStatus extends EnumJson[PlayerStatus] {
  private val lookup = lookupJson(values.map { member =>
    val apiString = member match {
      case Fairplay => "closed:fair_play_violations"
      case Abuse    => "closed:abuse"
      case other    => other.toString.toLowerCase
    }
    apiString -> member
  }.toMap)

  override protected def jsonToEnum(string: String) =
    super.jsonToEnum(string).orElse(lookup(string))
}

enum TimeClass(val isDaily: Boolean) {
  case Daily     extends TimeClass(true)
  case Rapid     extends TimeClass(false)
  case Standard  extends TimeClass(false)
  case Blitz     extends TimeClass(false)
  case Lightning extends TimeClass(false)
  case Bullet    extends TimeClass(false)
}

object TimeClass extends EnumJson[TimeClass] with EnumSql[TimeClass]

enum Title {
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

object Title extends EnumJson[Title] with EnumSql[Title]

enum ClubVisibility {
  case Public
  case Private
}

object ClubVisibility extends EnumJson[ClubVisibility]

enum BoardGameWinner {
  case Team1, Team2, Draw
}

object BoardGameWinner extends EnumSql[BoardGameWinner]
