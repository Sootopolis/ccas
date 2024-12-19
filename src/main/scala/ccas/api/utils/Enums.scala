package ccas.api.utils

import zio.json.{DeriveJsonCodec, JsonCodec, PascalCase, SnakeCase}

object Enums {
  enum Colour {
    case White
    case Black
  }

  object Colour {
    given JsonCodec[Colour] = JsonCodec.string.transform(s => Colour.valueOf(s.capitalize), _.toString.toLowerCase)
  }

  // Not associated with the Chess.com public api.
  enum GameResult(val score: Double) {
    case Win extends GameResult(1.0)
    case Draw extends GameResult(0.5)
    case Loss extends GameResult(0.0)
  }

  object GameResult {
    given gameResultJsonCodec: JsonCodec[GameResult] = DeriveJsonCodec.gen
  }

  enum GameResultDetail(val category: GameResult) {
    val score: Double = category.score

    case Win extends GameResultDetail(GameResult.Win)
    case Stalemate extends GameResultDetail(GameResult.Draw)
    case Agreed extends GameResultDetail(GameResult.Draw)
    case Repetition extends GameResultDetail(GameResult.Draw)
    case `50Move` extends GameResultDetail(GameResult.Draw)
    case Insufficient extends GameResultDetail(GameResult.Draw)
    case TimeVsInsufficient extends GameResultDetail(GameResult.Draw)
    case Checkmated extends GameResultDetail(GameResult.Loss)
    case Resigned extends GameResultDetail(GameResult.Loss)
    case Timeout extends GameResultDetail(GameResult.Loss)
    case Lose extends GameResultDetail(GameResult.Loss)
    case Abandoned extends GameResultDetail(GameResult.Loss)
    case KingOfTheHill extends GameResultDetail(GameResult.Loss)
    case ThreeCheck extends GameResultDetail(GameResult.Loss)
    case BughousePartnerLose extends GameResultDetail(GameResult.Loss)
  }

  object GameResultDetail {
    private val lookup = GameResultDetail.values.map(result => result.toString.toLowerCase -> result).toMap

    given JsonCodec[GameResultDetail] = JsonCodec.string.transform(lookup.apply, _.toString.toLowerCase)
  }

  enum GameRule {
    case Chess
    case Chess960
    case Bughouse
    case KingOfTheHill
    case ThreeCheck
    case CrazyHouse
  }

  object GameRule {
    private val lookup = GameRule.values.map(result => result.toString.toLowerCase -> result).toMap

    given JsonCodec[GameRule] = JsonCodec.string.transform(lookup.apply, _.toString.toLowerCase)
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
  
  object League { 
    given leagueJsonCodec: JsonCodec[League] = DeriveJsonCodec.gen
  }

  enum ClubMatchResult(val scorePerPlayer: Double) {
    def totalScore(nPlayers: Int): Double = scorePerPlayer * nPlayers

    case Win extends ClubMatchResult(5.0)
    case Draw extends ClubMatchResult(2.0)
    case Lose extends ClubMatchResult(0.0)
  }

  enum ClubMatchStatus {
    case Finished
    case InProgress
    case Registration
  }

  object ClubMatchStatus {
    given JsonCodec[ClubMatchStatus] =
      JsonCodec.string.transform(s => ClubMatchStatus.valueOf(PascalCase(s)), e => SnakeCase(e.toString))
  }

  enum PlayerStatus {
    case basic
    case premium
    case mod
    case staff
    case closed
    case `closed:fair_play_violations`
    case `closed:abuse`
  }
  
  object PlayerStatus {
    given playerStatusJsonCodec: JsonCodec[PlayerStatus] = DeriveJsonCodec.gen
  }

  enum TimeClass {
    case daily
    case rapid
    case blitz
    case bullet
  }

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
  }
  
  object Title {
    given titleJsonCodec: JsonCodec[Title] = DeriveJsonCodec.gen
  }
}
