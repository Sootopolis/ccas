package ccas.utils.configs

import ccas.api.utils.subtypes.Elo
import ccas.utils.prettyprinting.PrettyPrinter
import zio.config.magnolia.DeriveConfig
import zio.{Chunk, Config, IO}

case class RecruitmentConfig(
  avoidAdmin         : Boolean = false,
  invitedExpiryDays  : Option[Int] = None,
  timeoutExpiryDays  : Option[Int] = None,
  checkedExpiryDays  : Option[Int] = None,
  minElo             : Option[Elo] = None,
  maxElo             : Option[Elo] = None,
  minScoreRate       : Option[Double] = None,
  maxScoreRate       : Option[Double] = None,
  maxTimeoutRate     : Option[Double] = None,
  maxMatchTimeoutRate: Option[Double] = None,
  minMatchesFinished : Option[Int] = None,
  minMatchesOngoing  : Option[Int] = None,
  maxGamesOngoing    : Option[Int] = None,
  maxClubs           : Option[Int] = None,
  maxHoursPerMove    : Option[Double] = None,
  maxHoursOffline    : Option[Double] = None,
  countries          : Chunk[String] = Chunk.empty,
) /*derives PrettyPrinter*/ {
  require(
    minScoreRate.forall(x => maxScoreRate.forall(x <= _)),
    s"minScoreRate ${ minScoreRate.get } is greater than maxScoreRate ${ maxScoreRate.get }."
  )
  require(
    minElo.forall(x => maxElo.forall(x <= _)),
    s"minElo ${ minElo.get } is greater than maxElo ${ maxElo.get }."
  )
  require(
    minMatchesOngoing.forall(x => maxGamesOngoing.forall(x <= _)),
    s"minMatchesOngoing ${ minMatchesOngoing.get } is greater than maxGamesOngoing ${ maxGamesOngoing.get }."
  )
}

object RecruitmentConfig extends CcasConfig[RecruitmentConfig] {
  val default = new RecruitmentConfig()

  given prettyPrinter: PrettyPrinter[RecruitmentConfig] =
    PrettyPrinter.derived(PrettyPrinter.Setting(ignoreDefault = true))

  override protected val derivedConfig: Config[RecruitmentConfig] = DeriveConfig.derived[RecruitmentConfig].desc

  private def get(clubAlias: String) = derivedConfig.nested(AllConfigs.root, clubAlias, "recruitment")

  def load(clubAlias: String): IO[Config.Error, RecruitmentConfig] = CcasConfig.load(get(clubAlias))
}
