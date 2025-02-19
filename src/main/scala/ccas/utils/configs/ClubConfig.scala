package ccas.utils.configs

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.client.CcasClient
import ccas.utils.prettyprinting.PrettyPrinter
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.jdbczio.Quill.DataSource as QuillDataSource
import zio.config.magnolia.DeriveConfig
import zio.{Config, IO, Task, TaskLayer, ZIO}

import javax.sql.DataSource

case class ClubConfig(
  clubUrlName: ClubUrlName,
  clubId     : Option[ClubId] = None,
  recruitment: RecruitmentConfig = RecruitmentConfig.default
) derives PrettyPrinter {
  def dataSourceLayer: TaskLayer[DataSource] = {
    val config = new HikariConfig()
    config.setDriverClassName("org.sqlite.JDBC")
    config.setJdbcUrl(s"jdbc:sqlite:data/$clubUrlName.sqlite")
    QuillDataSource.fromDataSource(new HikariDataSource(config))
  }

  def checkClubId(client: CcasClient): Task[Unit] = {
    ApiClub.get(client, clubUrlName).map(_.clubId).filterOrFail(newId => clubId.forall(_ == newId)) {
      new Exception(
        s""""$clubUrlName" is no longer club "$clubId". Please update club url name in application.conf.
           |${ ApiClub.getUrl(clubUrlName) }""".stripMargin
      )
    }.unit
  }
}

object ClubConfig extends CcasConfig[ClubConfig] {
  override protected val derivedConfig = DeriveConfig.derived[ClubConfig].desc

  private def get(clubAlias: String) = derivedConfig.nested(AllConfigs.root, clubAlias)

  def load(clubAlias: String): IO[Config.Error, ClubConfig] = CcasConfig.load(get(clubAlias))
}
