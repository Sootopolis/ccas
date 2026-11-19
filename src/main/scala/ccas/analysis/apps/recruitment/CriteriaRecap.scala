package ccas.analysis.apps.recruitment

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import zio.{RIO, ZIO}

import ccas.analysis.apps.recruitment.RecruitmentStatsHelpers.StatsWindowDays
import ccas.analysis.tables.{Club, RecruitmentAlias, RecruitmentCriteria}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo}
import ccas.utils.sql.PostgresClient

/** Criteria as the operator reads them: named by club and name, dated by version, one setting per line, each with
  * what it holds a candidate to. A run's report file and the criteria route print it, and so does a run's job log,
  * unless the operator has just confirmed it at the terminal, when the log names it in one line instead (#485).
  */
object CriteriaRecap {

  /** A setting that is set. A list has no value column: its items follow the meaning, so they can't push it out. */
  private final case class Setting(name: String, valueOption: Option[String], meaning: String)

  /** The recap of `criteriaId`, as stored: criteria rows are only ever inserted, so an old run's recap is exactly
    * what it ran with (#481).
    */
  def ofId(criteriaId: Long): RIO[PostgresClient, List[String]] =
    RecruitmentCriteria.selectId(criteriaId)
      .someOrFail(IllegalStateException(s"Criteria $criteriaId not found"))
      .flatMap(of)

  /** [[ofId]] for a criteria row already read. */
  def of(criteria: RecruitmentCriteria): RIO[PostgresClient, List[String]] =
    named(criteria).map(names => lines(names.aliases, criteria, names.clubSlugs))

  /** The line a run logs in place of the recap when the operator has just read it and confirmed (#485). */
  def confirmed(criteria: RecruitmentCriteria): RIO[PostgresClient, String] =
    named(criteria).map(names => s"Criteria as confirmed: ${header(names.aliases, names.clubSlugs)}")

  /** The names that point at the criteria, and the slugs of the clubs they and the criteria name. */
  private final case class Names(aliases: List[RecruitmentAlias], clubSlugs: Map[ClubId, ClubSlug])

  private def named(criteria: RecruitmentCriteria): RIO[PostgresClient, Names] =
    for {
      aliases <- RecruitmentAlias.selectFirstPerAlias(criteria.criteriaId)
      clubs   <- ZIO.foreach((aliases.map(_.clubId) ++ criteria.excludeClubs).distinct)(Club.selectId)
    } yield Names(aliases, clubs.flatten.map(club => club.clubId -> club.slug).toMap)

  private[recruitment] def lines(
    aliases: List[RecruitmentAlias],
    criteria: RecruitmentCriteria,
    clubSlugs: Map[ClubId, ClubSlug]
  ): List[String] = {
    val title = header(aliases, clubSlugs)
    settingLines(criteria, clubSlugs) match {
      case Nil      => List(s"$title: none set")
      case settings => s"$title:" :: settings.map(setting => s"  $setting")
    }
  }

  /** Names the criteria as the operator does: by club and name, dated by the version, never by the criteria id. */
  private[recruitment] def header(aliases: List[RecruitmentAlias], clubSlugs: Map[ClubId, ClubSlug]): String =
    aliases.map { row =>
      s"${slugOf(row.clubId, clubSlugs)}'s criteria \"${row.alias}\", version of ${version(row.since)}"
    } match {
      case Nil   => "Criteria"
      case named => named.mkString("; ")
    }

  /** A version of criteria as the operator reads it: when it was set, to the minute, so two set on one day read apart. */
  def version(since: Instant): String = since.atOffset(ZoneOffset.UTC).format(VersionFormat)

  private val VersionFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm 'UTC'")

  private def slugOf(clubId: ClubId, clubSlugs: Map[ClubId, ClubSlug]): String =
    clubSlugs.get(clubId).fold(s"#$clubId")(ClubSlug.unwrap)

  /** The settings that are set, aligned in columns. An excluded club missing from `clubSlugs` reads as its id. */
  private[recruitment] def settingLines(
    criteria: RecruitmentCriteria,
    clubSlugs: Map[ClubId, ClubSlug]
  ): List[String] = {
    val set        = settings(criteria, clubSlugs)
    val nameWidth  = set.map(_.name.length).maxOption.getOrElse(0)
    val valueWidth = set.flatMap(_.valueOption).map(_.length).maxOption.getOrElse(0)
    set.map { setting =>
      val value = if (valueWidth == 0) "" else s"${setting.valueOption.getOrElse("").padTo(valueWidth, ' ')}  "
      s"${setting.name.padTo(nameWidth, ' ')}  $value${setting.meaning}"
    }
  }

  private def settings(criteria: RecruitmentCriteria, clubSlugs: Map[ClubId, ClubSlug]): List[Setting] = {
    def whole(name: String, value: Option[Int], meaning: String): Option[Setting] =
      value.map(v => Setting(name, Some(v.toString), meaning))
    def decimal(name: String, value: Option[Double], meaning: String): Option[Setting] =
      value.map(v => Setting(name, Some(BigDecimal(v).bigDecimal.stripTrailingZeros.toPlainString), meaning))
    def elo(name: String, value: Option[Elo], meaning: String): Option[Setting] =
      whole(name, value.map(Elo.unwrap(_).toInt), meaning)
    def flag(name: String, value: Boolean, meaning: String): Option[Setting] =
      Option.when(value)(Setting(name, Some("yes"), meaning))
    def list(name: String, items: List[String], meaning: String): Option[Setting] =
      Option.when(items.nonEmpty)(Setting(name, None, s"$meaning: ${items.mkString(", ")}"))
    val nationalitySense =
      if (criteria.nationalityExclude) "from none of these countries" else "from one of these countries"
    val excludedClubs = criteria.excludeClubs.map(slugOf(_, clubSlugs))
    val window        = s"in the last $StatsWindowDays days"
    // "Of any kind": casual, chess960 and unplayed games count, where Chess.com's rate leaves them out (#476).
    List(
      whole(
        "minDaysSinceRegistration",
        criteria.minDaysSinceRegistration,
        "joined Chess.com at least this many days ago"
      ),
      whole("daysSinceLastInvited", criteria.daysSinceLastInvited, "not invited to this club within this many days"),
      // The cooldown is kept per name, so it spans the edits a name has had (`RecruitmentExplore`).
      whole(
        "daysSinceRejected",
        criteria.daysSinceRejected,
        "not rejected by these criteria, or an earlier version of them, within this many days"
      ),
      list("nationalityCountries", criteria.nationalityCountries, nationalitySense),
      whole("maxClubs", criteria.maxClubs, "in at most this many clubs"),
      list("excludeClubs", excludedClubs, "in none of these clubs"),
      flag("excludeSourceAdmins", criteria.excludeSourceAdmins, "not an admin of a club being scouted"),
      whole(
        "avoidAdminMinClubSize",
        criteria.avoidAdminMinClubSize,
        "not an admin of an active club with at least this many members"
      ),
      flag("excludeFormerMembers", criteria.excludeFormerMembers, "not a former member of this club"),
      elo("dailyMinElo", criteria.dailyMinElo, "daily rating at least this"),
      elo("dailyMaxElo", criteria.dailyMaxElo, "daily rating at most this"),
      decimal("dailyMinScoreRate", criteria.dailyMinScoreRate, "daily score rate at least this, draws counting half"),
      decimal("dailyMaxScoreRate", criteria.dailyMaxScoreRate, "daily score rate at most this, draws counting half"),
      whole(
        "dailyMinGamesFinished",
        criteria.dailyMinGamesFinished,
        s"at least this many daily games of any kind ended $window"
      ),
      // At 0 the zero-tolerance check also applies, to a timeout in our own reading of the archives (`FigureCriterion`).
      decimal(
        "dailyMaxTimeoutPercent",
        criteria.dailyMaxTimeoutPercent,
        if (criteria.dailyMaxTimeoutPercent.contains(0.0)) {
          s"no daily game of any kind, team matches included, timed out $window"
        } else { "timed out of at most this % of daily games, by Chess.com's figure" }
      ),
      whole("dailyMaxHoursPerMove", criteria.dailyMaxHoursPerMove, "averages at most this many hours a daily move"),
      whole("dailyMinOngoingGames", criteria.dailyMinOngoingGames, "playing at least this many daily games now"),
      whole("dailyMaxOngoingGames", criteria.dailyMaxOngoingGames, "playing at most this many daily games now"),
      whole(
        "dailyMinTmGamesFinished",
        criteria.dailyMinTmGamesFinished,
        s"at least this many team-match games of any kind ended $window"
      ),
      decimal(
        "dailyMaxTmTimeoutPercent",
        criteria.dailyMaxTmTimeoutPercent,
        s"timed out of at most this % of team-match games of any kind $window"
      ),
      whole(
        "dailyMinOngoingTeamMatches",
        criteria.dailyMinOngoingTeamMatches,
        "playing at least this many team-match games now"
      )
    ).flatten
  }
}
