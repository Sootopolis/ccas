package ccas.api.utils

object Enums {
  enum Colour {
    case white
    case black
  }

  enum GameResult(val score: Double) {
    case Win extends GameResult(1.0)
    case Draw extends GameResult(0.5)
    case Loss extends GameResult(0.0)
  }

  enum GameResultDetail(val category: GameResult) {
    val score: Double = category.score

    case win extends GameResultDetail(GameResult.Win)
    case stalemate extends GameResultDetail(GameResult.Draw)
    case agreed extends GameResultDetail(GameResult.Draw)
    case repetition extends GameResultDetail(GameResult.Draw)
    case `50move` extends GameResultDetail(GameResult.Draw)
    case insufficient extends GameResultDetail(GameResult.Draw)
    case timevsinsufficient extends GameResultDetail(GameResult.Draw)
    case checkmated extends GameResultDetail(GameResult.Loss)
    case resigned extends GameResultDetail(GameResult.Loss)
    case timeout extends GameResultDetail(GameResult.Loss)
    case lose extends GameResultDetail(GameResult.Loss)
    case abandoned extends GameResultDetail(GameResult.Loss)
    case kingofthehill extends GameResultDetail(GameResult.Loss)
    case threecheck extends GameResultDetail(GameResult.Loss)
    case bughousepartnerlose extends GameResultDetail(GameResult.Loss)
  }

  enum GameRule {
    case chess
    case chess960
    case bughouse
    case kingofthehill
    case threecheck
    case crazyhouse
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

  enum ClubMatchResult(val scorePerPlayer: Double) {
    case win extends ClubMatchResult(5.0)
    case draw extends ClubMatchResult(2.0)
    case lose extends ClubMatchResult(0.0)
  }

  enum ClubMatchStatus {
    case finished
    case in_progress
    case registration
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
}
