package ccas.utils.configs

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubUrlName}
import ccas.utils.client.CcasClient
import ccas.utils.prettyprinting.PrettyPrinter
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.jdbczio.Quill.DataSource as QuillDataSource
import zio.config.magnolia.DeriveConfig
import zio.{Config, IO, Task, TaskLayer}

import javax.sql.DataSource

sealed trait BaseClubConfig extends Product with Serializable {
  self =>
  val clubUrlName: ClubUrlName
  val recruitment: RecruitmentConfig

  def dataSourceLayer: TaskLayer[DataSource] = {
    val config = new HikariConfig()
    config.setDriverClassName("org.sqlite.JDBC")
    config.setJdbcUrl(s"jdbc:sqlite:data/$clubUrlName.sqlite")
    QuillDataSource.fromDataSource(new HikariDataSource(config))
  }

  def idChecked(client: CcasClient): Task[BaseClubConfig.ClubConfig] = self match {
    case unknownId: BaseClubConfig.ClubConfigUnknownId =>
      ApiClub.get(client, clubUrlName).map(apiClub => unknownId.addClubId(apiClub.clubId))
    case clubConfig: BaseClubConfig.ClubConfig =>
      ApiClub.get(client, clubUrlName).map(_.clubId).filterOrFail(newId => clubConfig.clubId == newId) {
        new Exception(
          s""""$clubUrlName" is no longer club "${ clubConfig.clubId }".
             |Please update club url name in application.conf.
             |${ ApiClub.getUrl(clubUrlName) }""".stripMargin
        )
      }.as(clubConfig)
  }
}

object BaseClubConfig extends CcasConfig[BaseClubConfig] {
  case class ClubConfig(
    clubUrlName: ClubUrlName,
    clubId     : ClubId,
    recruitment: RecruitmentConfig = RecruitmentConfig.default
  ) extends BaseClubConfig derives PrettyPrinter

  object ClubConfig {
    private[configs] val derivedConfig = DeriveConfig.derived[ClubConfig].desc
  }

  private[configs] case class ClubConfigUnknownId(
    clubUrlName: ClubUrlName,
    recruitment: RecruitmentConfig = RecruitmentConfig.default
  ) extends BaseClubConfig derives PrettyPrinter {
    def addClubId(clubId: ClubId): ClubConfig = ClubConfig(clubUrlName, clubId, recruitment)
  }

  private[configs] object ClubConfigUnknownId {
    private[configs] val derivedConfig = DeriveConfig.derived[ClubConfigUnknownId].desc
  }

  override protected val derivedConfig: Config[BaseClubConfig] =
    ClubConfig.derivedConfig.orElse(ClubConfigUnknownId.derivedConfig)

  private def get(clubAlias: String) = derivedConfig.nested(AllConfigs.root, clubAlias)

  def load(clubAlias: String): IO[Config.Error, BaseClubConfig] = CcasConfig.load(get(clubAlias))
}
