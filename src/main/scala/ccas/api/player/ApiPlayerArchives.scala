package ccas.api.player

import ccas.api.utils.Subtypes.Username
import zio.Chunk
import zio.http.URL
import zio.json.{SnakeCase, jsonMemberNames}

@jsonMemberNames(SnakeCase)
case class ApiPlayerArchives(archives: Chunk[URL])

object ApiPlayerArchives {
  def getUrl(username: Username): URL = ApiPlayerGamesCurrent.getUrl(username).addPath("archives")
}
