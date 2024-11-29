package ccas.api.player

import ccas.api.utils.Subtypes.Username
import ccas.utils.PrettyPrinting
import zio.Chunk
import zio.http.URL

case class ApiPlayerArchives(archives: Chunk[URL]) extends PrettyPrinting[ApiPlayerArchives]

object ApiPlayerArchives {
  def getUrl(username: Username): URL = ApiPlayerCurrentDailyGames.getUrl(username).addPath("archives")
}
