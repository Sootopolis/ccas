package ccas.analysis.apps.recruitment

import java.nio.file.{Files, Path}
import java.sql.SQLException
import java.time.Instant

import scala.annotation.tailrec

import zio.{Clock, IO, RIO, ZIO}
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder}

import ccas.analysis.apps.NamedClub
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo}
import ccas.utils.errors.{safeMessage, safeMessageNaming, BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

/** Sets, shows and lists a club's named recruitment criteria for [[ccas.server.routes.RecruitmentCriteriaRoutes]],
  * which `ccas recruitment-criteria` calls. A name is an alias row; "alias" stays the schema's word, not the operator's.
  *
  * A "set" is a versioned insert: criteria rows are immutable and `recruitment_alias` is keyed by
  * `(club_id, alias, since)`, so both first-set and later-change insert a fresh criteria row plus a new alias row
  * pointing at it. `RecruitmentApp` reads newest-wins via `RecruitmentAlias.selectLatest`, so there is no update or
  * delete path. Every operation takes a club already resolved, and so already ingested, which recruitment config
  * assumes anyway.
  */
object RecruitmentCriteriaApp {
  val MaxAliasLength: Int               = 64
  private val MaxJsonBytes: Long        = 1L * 1024 * 1024
  private val MaxSinceRetries: Int      = 5
  private val PgUniqueViolation: String = "23505"

  /** Refuses what [[set]] would, so a route can refuse it before resolving the club, which may ask Chess.com. */
  def validateSet(alias: String, criteria: RecruitmentCriteria): IO[BadRequestException, Unit] = {
    val a = alias.trim
    for {
      _ <- ZIO.whenDiscard(a.isEmpty)(ZIO.fail(BadRequestException("criteria name must not be empty")))
      _ <- ZIO.whenDiscard(a.length > MaxAliasLength) {
        ZIO.fail(BadRequestException(s"criteria name must be <= $MaxAliasLength chars (got ${a.length})"))
      }
      _ <- ZIO.fromEither(criteria.capped.validate).mapError(BadRequestException(_))
    } yield ()
  }

  def set(club: NamedClub, alias: String, criteria: RecruitmentCriteria): RIO[PostgresClient, Long] = {
    val a      = alias.trim
    val capped = criteria.capped
    for {
      _ <- validateSet(alias, criteria)
      _ <- ZIO.whenDiscard(capped != criteria) {
        ZIO.logInfo(s"Capped lookback fields to ${RecruitmentCriteria.MaxDaysSinceLookback} days for alias '$a'")
      }
      current <- latestCriteria(club.clubId, a)
      criteriaId <- current match {
        // Unchanged re-submit: reuse the existing version instead of growing the append-only tables.
        case Some((id, stored)) if stored.copy(criteriaId = 0) == capped.copy(criteriaId = 0) =>
          ZIO
            .logInfo(s"Criteria for alias '$a' of club ${club.slug} unchanged (criteria_id=$id); skipping insert")
            .as(id)
        case _ =>
          val changes = diffLines(current.map(_._2), capped, clubId => ClubId.unwrap(clubId).toString)
          for {
            now <- Clock.instant
            id  <- insertWithSinceRetry(club.clubId, a, capped, now, MaxSinceRetries)
            _ <- ZIO.logInfo(
              s"Set criteria (criteria_id=$id) for alias '$a' of club ${club.slug} | ${changes.mkString("; ")}"
            )
          } yield id
      }
    } yield criteriaId
  }

  /** Per-field "before → after" lines for a save, or `List("(new criteria)")` when there is no prior version. The save
    * logs them, and `ccas recruitment-criteria set` shows them before it saves, each excluded club as `clubName` gives it.
    */
  private[ccas] def diffLines(
    before: Option[RecruitmentCriteria],
    after: RecruitmentCriteria,
    clubName: ClubId => String
  ): List[String] = {
    @tailrec
    def disp(x: Any): String = x match {
      case None         => "none"
      case Some(v)      => disp(v)
      case lst: List[?] => if (lst.isEmpty) "[]" else lst.mkString("[", ",", "]")
      case other        => other.toString
    }
    def line[A](label: String, b: A, a: A): Option[String] =
      if (b == a) { None }
      else { Some(s"$label: ${disp(b)} → ${disp(a)}") }
    before match {
      case None => List("(new criteria)")
      case Some(b) =>
        List(
          line("minDaysSinceRegistration", b.minDaysSinceRegistration, after.minDaysSinceRegistration),
          line("daysSinceLastInvited", b.daysSinceLastInvited, after.daysSinceLastInvited),
          line("daysSinceRejected", b.daysSinceRejected, after.daysSinceRejected),
          line("nationalityExclude", b.nationalityExclude, after.nationalityExclude),
          line("nationalityCountries", b.nationalityCountries, after.nationalityCountries),
          line("excludeClubs", b.excludeClubs.map(clubName), after.excludeClubs.map(clubName)),
          line("maxClubs", b.maxClubs, after.maxClubs),
          line("excludeSourceAdmins", b.excludeSourceAdmins, after.excludeSourceAdmins),
          line("avoidAdminMinClubSize", b.avoidAdminMinClubSize, after.avoidAdminMinClubSize),
          line("excludeFormerMembers", b.excludeFormerMembers, after.excludeFormerMembers),
          line("dailyMinElo", b.dailyMinElo, after.dailyMinElo),
          line("dailyMaxElo", b.dailyMaxElo, after.dailyMaxElo),
          line("dailyMinScoreRate", b.dailyMinScoreRate, after.dailyMinScoreRate),
          line("dailyMaxScoreRate", b.dailyMaxScoreRate, after.dailyMaxScoreRate),
          line("dailyMinGamesFinished", b.dailyMinGamesFinished, after.dailyMinGamesFinished),
          line("dailyMinTmGamesFinished", b.dailyMinTmGamesFinished, after.dailyMinTmGamesFinished),
          line("dailyMaxTimeoutPercent", b.dailyMaxTimeoutPercent, after.dailyMaxTimeoutPercent),
          line("dailyMaxTmTimeoutPercent", b.dailyMaxTmTimeoutPercent, after.dailyMaxTmTimeoutPercent),
          line("dailyMaxHoursPerMove", b.dailyMaxHoursPerMove, after.dailyMaxHoursPerMove),
          line("dailyMinOngoingGames", b.dailyMinOngoingGames, after.dailyMinOngoingGames),
          line("dailyMaxOngoingGames", b.dailyMaxOngoingGames, after.dailyMaxOngoingGames),
          line("dailyMinOngoingTeamMatches", b.dailyMinOngoingTeamMatches, after.dailyMinOngoingTeamMatches)
        ).flatten
    }
  }

  /** The criteria currently bound to `(clubId, alias)`, with its id. `None` if no alias row, or if the referenced
    * criteria row is missing (a self-healing integrity gap: callers then write a fresh version).
    */
  private def latestCriteria(clubId: ClubId, alias: String): RIO[PostgresClient, Option[(Long, RecruitmentCriteria)]] =
    RecruitmentAlias.selectLatest(clubId, alias).flatMap {
      case Some(row) => RecruitmentCriteria.selectId(row.criteriaId).map(_.map((row.criteriaId, _)))
      case None      => ZIO.none
    }

  /** Single-shot atomic insert: criteria + alias rows in one transaction. On the (rare) same-microsecond collision
    * against the `recruitment_alias` composite PK `(club_id, alias, since)`, the transaction rolls back (so no orphan
    * criteria row is left behind) and we retry with `since + 1 µs`. `TIMESTAMPTZ` has microsecond precision, so 1 µs
    * is the smallest distinguishable bump.
    */
  private def insertWithSinceRetry(
    clubId: ClubId,
    alias: String,
    criteria: RecruitmentCriteria,
    since: Instant,
    attemptsLeft: Int
  ): RIO[PostgresClient, Long] =
    withTransaction {
      for {
        newId <- RecruitmentCriteria.insert(criteria)
        _     <- RecruitmentAlias.insert(RecruitmentAlias(clubId, alias, since, newId))
      } yield newId
    }.catchSome {
      case e: SQLException if e.getSQLState == PgUniqueViolation && attemptsLeft > 0 =>
        insertWithSinceRetry(clubId, alias, criteria, since.plusNanos(1000), attemptsLeft - 1)
    }

  def show(club: NamedClub, alias: String): RIO[PostgresClient, RecruitmentCriteria] =
    for {
      aliasRow <- RecruitmentAlias.selectLatest(club.clubId, alias)
        .someOrFail(NotFoundException(noSuchCriteria(club.slug, alias)))
      criteria <- RecruitmentCriteria.selectId(aliasRow.criteriaId)
        .someOrFail(
          new IllegalStateException(s"Criteria ${aliasRow.criteriaId} referenced by alias '$alias' not found")
        )
    } yield criteria

  /** What an operator who names criteria the club lacks is told, in their words: a name, not an alias row. */
  def noSuchCriteria(club: ClubSlug, alias: String): String = s"$club has no criteria named \"$alias\""

  def list(clubId: ClubId): RIO[PostgresClient, List[RecruitmentAlias]] =
    RecruitmentAlias.selectClub(clubId)

  // --- JSON input ---

  private[ccas] def loadFromJson(path: String): RIO[Any, RecruitmentCriteria] =
    for {
      file <- ZIO.attempt(Path.of(path)).mapError(e => BadRequestException(s"invalid path: ${e.safeMessage}"))
      size <- ZIO.attemptBlocking(Files.size(file))
        .mapError(e => BadRequestException(s"cannot read ${e.safeMessageNaming(file)}"))
      _ <- ZIO.whenDiscard(size > MaxJsonBytes) {
        ZIO.fail(BadRequestException(s"JSON file too large: $size bytes (max $MaxJsonBytes)"))
      }
      content <- ZIO.attemptBlocking(Files.readString(file))
        .mapError(e => BadRequestException(s"cannot read ${e.safeMessageNaming(file)}"))
      spec <- ZIO.fromEither(JsonDecoder[CriteriaSpec].decodeJson(content)).mapError(BadRequestException(_))
    } yield spec.toCriteria
}

/** Wire shape for `RecruitmentCriteria` minus the server-assigned `criteria_id` surrogate: the HTTP route body, and
  * the file `ccas recruitment-criteria show --json` prints and `set --file` reads.
  */
final case class CriteriaSpec(
  minDaysSinceRegistration: Option[Int],
  daysSinceLastInvited: Option[Int],
  daysSinceRejected: Option[Int],
  nationalityExclude: Boolean,
  nationalityCountries: List[String],
  excludeClubs: List[ClubId],
  maxClubs: Option[Int],
  excludeSourceAdmins: Boolean,
  avoidAdminMinClubSize: Option[Int],
  excludeFormerMembers: Boolean,
  dailyMinElo: Option[Elo],
  dailyMaxElo: Option[Elo],
  dailyMinScoreRate: Option[Double],
  dailyMaxScoreRate: Option[Double],
  dailyMinGamesFinished: Option[Int],
  dailyMinTmGamesFinished: Option[Int],
  dailyMaxTimeoutPercent: Option[Double],
  dailyMaxTmTimeoutPercent: Option[Double],
  dailyMaxHoursPerMove: Option[Int],
  dailyMinOngoingGames: Option[Int],
  dailyMaxOngoingGames: Option[Int],
  dailyMinOngoingTeamMatches: Option[Int]
) {
  def toCriteria: RecruitmentCriteria = RecruitmentCriteria(
    criteriaId = 0,
    minDaysSinceRegistration = minDaysSinceRegistration,
    daysSinceLastInvited = daysSinceLastInvited,
    daysSinceRejected = daysSinceRejected,
    nationalityExclude = nationalityExclude,
    nationalityCountries = nationalityCountries,
    excludeClubs = excludeClubs,
    maxClubs = maxClubs,
    excludeSourceAdmins = excludeSourceAdmins,
    avoidAdminMinClubSize = avoidAdminMinClubSize,
    excludeFormerMembers = excludeFormerMembers,
    dailyMinElo = dailyMinElo,
    dailyMaxElo = dailyMaxElo,
    dailyMinScoreRate = dailyMinScoreRate,
    dailyMaxScoreRate = dailyMaxScoreRate,
    dailyMinGamesFinished = dailyMinGamesFinished,
    dailyMinTmGamesFinished = dailyMinTmGamesFinished,
    dailyMaxTimeoutPercent = dailyMaxTimeoutPercent,
    dailyMaxTmTimeoutPercent = dailyMaxTmTimeoutPercent,
    dailyMaxHoursPerMove = dailyMaxHoursPerMove,
    dailyMinOngoingGames = dailyMinOngoingGames,
    dailyMaxOngoingGames = dailyMaxOngoingGames,
    dailyMinOngoingTeamMatches = dailyMinOngoingTeamMatches
  )
}

object CriteriaSpec {
  given JsonCodec[CriteriaSpec] = DeriveJsonCodec.gen

  def fromCriteria(c: RecruitmentCriteria): CriteriaSpec = CriteriaSpec(
    minDaysSinceRegistration = c.minDaysSinceRegistration,
    daysSinceLastInvited = c.daysSinceLastInvited,
    daysSinceRejected = c.daysSinceRejected,
    nationalityExclude = c.nationalityExclude,
    nationalityCountries = c.nationalityCountries,
    excludeClubs = c.excludeClubs,
    maxClubs = c.maxClubs,
    excludeSourceAdmins = c.excludeSourceAdmins,
    avoidAdminMinClubSize = c.avoidAdminMinClubSize,
    excludeFormerMembers = c.excludeFormerMembers,
    dailyMinElo = c.dailyMinElo,
    dailyMaxElo = c.dailyMaxElo,
    dailyMinScoreRate = c.dailyMinScoreRate,
    dailyMaxScoreRate = c.dailyMaxScoreRate,
    dailyMinGamesFinished = c.dailyMinGamesFinished,
    dailyMinTmGamesFinished = c.dailyMinTmGamesFinished,
    dailyMaxTimeoutPercent = c.dailyMaxTimeoutPercent,
    dailyMaxTmTimeoutPercent = c.dailyMaxTmTimeoutPercent,
    dailyMaxHoursPerMove = c.dailyMaxHoursPerMove,
    dailyMinOngoingGames = c.dailyMinOngoingGames,
    dailyMaxOngoingGames = c.dailyMaxOngoingGames,
    dailyMinOngoingTeamMatches = c.dailyMinOngoingTeamMatches
  )
}
