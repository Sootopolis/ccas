package ccas.utils.configs

import ccas.api.utils.subtypes.ClubUrlName
import ccas.utils.prettyprinting.PrettyPrinter
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.jdbczio.Quill.DataSource as QuillDataSource
import zio.config.magnolia.DeriveConfig
import zio.{Config, IO, TaskLayer}

import javax.sql.DataSource

case class ClubConfig(clubUrlName: ClubUrlName, recruitment: RecruitmentConfig = RecruitmentConfig.default) {
  def dataSourceLayer: TaskLayer[DataSource] = {
    val config = new HikariConfig()
    config.setDriverClassName("org.sqlite.JDBC")
    config.setJdbcUrl(s"jdbc:sqlite:data/$clubUrlName.sqlite")
    QuillDataSource.fromDataSource(new HikariDataSource(config))
  }
}

object ClubConfig extends CcasConfig[ClubConfig] {
  override protected val derivedConfig = DeriveConfig.derived[ClubConfig].desc

  given prettyPrinter: PrettyPrinter[ClubConfig] =
    PrettyPrinter.derived(PrettyPrinter.Setting(ignoreDefault = true))

  private def get(clubAlias: String) = derivedConfig.nested(AllConfigs.root, clubAlias)

  def load(clubAlias: String): IO[Config.Error, ClubConfig] = CcasConfig.load(get(clubAlias))
}
