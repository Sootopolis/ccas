package ccas.api.player

import ccas.api.utils.{GameRule, TimeClass}

trait ApiPlayerGame {
  val pgn: String
  val rules: GameRule
  val timeClass: TimeClass
}
