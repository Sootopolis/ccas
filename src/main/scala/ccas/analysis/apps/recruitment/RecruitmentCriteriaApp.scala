package ccas.analysis.apps.recruitment

import java.nio.file.{Files, Path}
import java.sql.SQLException
import java.time.Instant

import zio.{Clock, Console, RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.json.{DeriveJsonCodec, EncoderOps, JsonCodec, JsonDecoder}

import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo}
import ccas.utils.errors.{BadRequestException, NotFoundException}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

/** Sets recruitment criteria for a club's alias from CLI (interactive prompts or a JSON file) or — via
  * [[ccas.server.routes.RecruitmentCriteriaRoutes]] — from an HTTP request.
  *
  * A "set" is a versioned insert: criteria rows are immutable and `recruitment_alias` is keyed by
  * `(club_id, alias, since)`, so both first-set and later-change insert a fresh criteria row plus a new alias row
  * pointing at it. `RecruitmentApp` reads newest-wins via `RecruitmentAlias.selectLatest`, so there is no update or
  * delete path. The club must already exist locally (no network); recruitment config assumes the club has been
  * ingested.
  */
object RecruitmentCriteriaApp extends ZIOAppDefault {
  val MaxAliasLength: Int            = 64
  private val MaxJsonBytes: Long     = 1L * 1024 * 1024
  private val MaxSinceRetries: Int   = 5
  private val PgUniqueViolation: String = "23505"

  private val help =
    s"""Usage: RecruitmentCriteriaApp <command> [args]
       |
       |Commands:
       |  set <club-slug> <alias> [--json <file>]        Set criteria for an alias (interactive prompts, or load JSON)
       |  show <club-slug> <alias>                       Show the current criteria for an alias
       |  list <club-slug>                               List all aliases for a club
       |  sample                                         Print defaultDaily as JSON (template for --json)
       |
       |Alias must be 1-$MaxAliasLength characters after trimming.""".stripMargin

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      _ <- args.toList match {
        case "set" :: clubStr :: alias :: rest =>
          val clubSlug = ClubSlug.wrap(clubStr)
          for {
            criteriaOpt <- rest match {
              case "--json" :: path :: Nil => loadFromJson(path).map(Some(_))
              case Nil                     => promptCriteria(clubSlug, alias)
              case _                       => ZIO.fail(BadRequestException(help))
            }
            _ <- criteriaOpt match {
              case Some(c) => set(clubSlug, alias, c).flatMap(id => Console.printLine(s"criteria_id=$id"))
              case None    => Console.printLine("Aborted; no changes.")
            }
          } yield ()
        case "show" :: clubStr :: alias :: _ =>
          show(ClubSlug.wrap(clubStr), alias).flatMap(printCriteria)
        case "list" :: clubStr :: _ =>
          list(ClubSlug.wrap(clubStr)).flatMap(printAliases)
        case "sample" :: _ =>
          Console.printLine(CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily).toJsonPretty)
        case _ => ZIO.fail(BadRequestException(help))
      }
    } yield ()).provideSomeAuto(
      PostgresClient.live(onInit = Tables.ensureTablesOnInit)
    )

  // --- Core (reused by RecruitmentCriteriaRoutes) ---

  def set(clubSlug: ClubSlug, alias: String, criteria: RecruitmentCriteria): RIO[PostgresClient, Long] = {
    val a      = alias.trim
    val capped = criteria.capped
    for {
      _ <- ZIO.whenDiscard(a.isEmpty)(ZIO.fail(BadRequestException("alias must not be empty")))
      _ <- ZIO.whenDiscard(a.length > MaxAliasLength)(
        ZIO.fail(BadRequestException(s"alias must be <= $MaxAliasLength chars (got ${a.length})"))
      )
      _ <- ZIO.fromEither(validate(capped)).mapError(BadRequestException(_))
      _ <- ZIO.whenDiscard(capped != criteria)(
        ZIO.logInfo(s"Capped lookback fields to ${RecruitmentCriteria.MaxDaysSinceLookback} days for alias '$a'")
      )
      club    <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      current <- latestCriteria(club.clubId, a)
      criteriaId <- current match {
        // Unchanged re-submit: reuse the existing version instead of growing the append-only tables.
        case Some((id, stored)) if stored.copy(criteriaId = 0) == capped.copy(criteriaId = 0) =>
          ZIO.logInfo(s"Criteria for alias '$a' of club $clubSlug unchanged (criteria_id=$id); skipping insert").as(id)
        case _ =>
          val changes = diffLines(current.map(_._2), capped)
          for {
            now <- Clock.instant
            id  <- insertWithSinceRetry(club.clubId, a, capped, now, MaxSinceRetries)
            _ <- ZIO.logInfo(
              s"Set criteria (criteria_id=$id) for alias '$a' of club $clubSlug | ${changes.mkString("; ")}"
            )
          } yield id
      }
    } yield criteriaId
  }

  /** Per-field "before → after" lines for a save. Returns `List("(new alias)")` when there is no prior version.
    * Used by both the core save log and the interactive confirmation preview.
    */
  private[recruitment] def diffLines(before: Option[RecruitmentCriteria], after: RecruitmentCriteria): List[String] = {
    def disp(x: Any): String = x match {
      case None         => "none"
      case Some(v)      => disp(v)
      case lst: List[?] => if (lst.isEmpty) "[]" else lst.mkString("[", ",", "]")
      case other        => other.toString
    }
    def line[A](label: String, b: A, a: A): Option[String] =
      if (b == a) { None } else { Some(s"$label: ${disp(b)} → ${disp(a)}") }
    before match {
      case None => List("(new alias)")
      case Some(b) =>
        List(
          line("minDaysSinceRegistration", b.minDaysSinceRegistration, after.minDaysSinceRegistration),
          line("daysSinceLastInvited", b.daysSinceLastInvited, after.daysSinceLastInvited),
          line("daysSinceRejected", b.daysSinceRejected, after.daysSinceRejected),
          line("nationalityExclude", b.nationalityExclude, after.nationalityExclude),
          line("nationalityCountries", b.nationalityCountries, after.nationalityCountries),
          line("excludeClubs", b.excludeClubs, after.excludeClubs),
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

  def show(clubSlug: ClubSlug, alias: String): RIO[PostgresClient, RecruitmentCriteria] =
    for {
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      aliasRow <- RecruitmentAlias.selectLatest(club.clubId, alias)
        .someOrFail(NotFoundException(s"No recruitment alias '$alias' found for club '$clubSlug'"))
      criteria <- RecruitmentCriteria.selectId(aliasRow.criteriaId)
        .someOrFail(new IllegalStateException(s"Criteria ${aliasRow.criteriaId} referenced by alias '$alias' not found"))
    } yield criteria

  def list(clubSlug: ClubSlug): RIO[PostgresClient, List[RecruitmentAlias]] =
    for {
      club    <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      aliases <- RecruitmentAlias.selectClub(club.clubId)
    } yield aliases

  /** Cross-field sanity checks beyond the per-field opaque-type validation. (The 180-day lookback cap is applied by
    * `RecruitmentCriteria.capped` inside `insert`.) Surfaced as a `BadRequestException` by callers.
    */
  def validate(c: RecruitmentCriteria): Either[String, Unit] = {
    def nonNeg(label: String, v: Option[Int]): Option[String] =
      v.filter(_ < 0).map(x => s"$label must be >= 0 (got $x)")
    def inUnit(label: String, v: Option[Double]): Option[String] =
      v.filter(d => d < 0.0 || d > 1.0).map(x => s"$label must be in [0, 1] (got $x)")
    def inPercent(label: String, v: Option[Double]): Option[String] =
      v.filter(d => d < 0.0 || d > 100.0).map(x => s"$label must be in [0, 100] (got $x)")
    def ordered[A](label: String, lo: Option[A], hi: Option[A])(using ord: Ordering[A]): Option[String] =
      (lo, hi) match {
        case (Some(l), Some(h)) if ord.gt(l, h) => Some(s"$label min ($l) must be <= max ($h)")
        case _                                  => None
      }
    val errors = List(
      nonNeg("minDaysSinceRegistration", c.minDaysSinceRegistration),
      nonNeg("daysSinceLastInvited", c.daysSinceLastInvited),
      nonNeg("daysSinceRejected", c.daysSinceRejected),
      nonNeg("maxClubs", c.maxClubs),
      nonNeg("avoidAdminMinClubSize", c.avoidAdminMinClubSize),
      nonNeg("dailyMinGamesFinished", c.dailyMinGamesFinished),
      nonNeg("dailyMinTmGamesFinished", c.dailyMinTmGamesFinished),
      nonNeg("dailyMaxHoursPerMove", c.dailyMaxHoursPerMove),
      nonNeg("dailyMinOngoingGames", c.dailyMinOngoingGames),
      nonNeg("dailyMaxOngoingGames", c.dailyMaxOngoingGames),
      nonNeg("dailyMinOngoingTeamMatches", c.dailyMinOngoingTeamMatches),
      inUnit("dailyMinScoreRate", c.dailyMinScoreRate),
      inUnit("dailyMaxScoreRate", c.dailyMaxScoreRate),
      inPercent("dailyMaxTimeoutPercent", c.dailyMaxTimeoutPercent),
      inPercent("dailyMaxTmTimeoutPercent", c.dailyMaxTmTimeoutPercent),
      ordered("dailyElo", c.dailyMinElo, c.dailyMaxElo),
      ordered("dailyScoreRate", c.dailyMinScoreRate, c.dailyMaxScoreRate),
      ordered("dailyOngoingGames", c.dailyMinOngoingGames, c.dailyMaxOngoingGames)
    ).flatten
    if (errors.isEmpty) Right(()) else Left(errors.mkString("; "))
  }

  // --- JSON input ---

  private def loadFromJson(path: String): RIO[Any, RecruitmentCriteria] =
    for {
      size <- ZIO.attemptBlocking(Files.size(Path.of(path)))
        .mapError(e => BadRequestException(s"cannot read $path: ${e.getMessage}"))
      _ <- ZIO.whenDiscard(size > MaxJsonBytes)(
        ZIO.fail(BadRequestException(s"JSON file too large: $size bytes (max $MaxJsonBytes)"))
      )
      content <- ZIO.attemptBlocking(Files.readString(Path.of(path)))
        .mapError(e => BadRequestException(s"cannot read $path: ${e.getMessage}"))
      spec <- ZIO.fromEither(JsonDecoder[CriteriaSpec].decodeJson(content)).mapError(BadRequestException(_))
    } yield spec.toCriteria

  // --- Output ---

  private def printCriteria(c: RecruitmentCriteria): RIO[Any, Unit] =
    Console.printLine(CriteriaSpec.fromCriteria(c).toJsonPretty)

  private def printAliases(aliases: List[RecruitmentAlias]): RIO[Any, Unit] =
    if (aliases.isEmpty) { Console.printLine("No aliases set.") }
    else {
      ZIO.foreachDiscard(aliases)(a =>
        Console.printLine(s"  ${a.alias}  (since ${a.since}, criteria_id=${a.criteriaId})")
      )
    }

  // --- Interactive prompts ---

  private def prompt(label: String): RIO[Any, String] =
    (Console.print(label) *> Console.readLine).map(_.trim)

  private def promptOpt[A](
    label: String,
    current: Option[A],
    show: A => String,
    parse: String => Either[String, A]
  ): RIO[Any, Option[A]] = {
    val shown = current.fold("none")(show)
    prompt(s"$label [$shown] (Enter=keep, - =none): ").flatMap {
      case ""  => ZIO.succeed(current)
      case "-" => ZIO.succeed(None)
      case s =>
        parse(s) match {
          case Right(v)  => ZIO.succeed(Some(v))
          case Left(err) => Console.printLine(s"  $err") *> promptOpt(label, current, show, parse)
        }
    }
  }

  private def promptOptInt(label: String, current: Option[Int]): RIO[Any, Option[Int]] =
    promptOpt(label, current, _.toString, s => s.toIntOption.toRight(s"'$s' is not an integer"))

  private def promptOptDouble(label: String, current: Option[Double]): RIO[Any, Option[Double]] =
    promptOpt(label, current, _.toString, s => s.toDoubleOption.toRight(s"'$s' is not a number"))

  private def promptOptElo(label: String, current: Option[Elo]): RIO[Any, Option[Elo]] =
    promptOpt(
      label,
      current,
      e => Elo.unwrap(e).toString,
      s =>
        s.toIntOption.toRight(s"'$s' is not an integer").flatMap { i =>
          if (i < 0 || i > Short.MaxValue) Left(s"must be in [0, ${Short.MaxValue}] (got $i)")
          else Right(Elo(i.toShort))
        }
    )

  private def promptBool(label: String, current: Boolean): RIO[Any, Boolean] =
    prompt(s"$label [$current] (y/n, Enter=keep): ").flatMap { s =>
      s.toLowerCase match {
        case ""                                 => ZIO.succeed(current)
        case "y" | "yes" | "true" | "t"         => ZIO.succeed(true)
        case "n" | "no" | "false" | "f"         => ZIO.succeed(false)
        case _ => Console.printLine("  enter y or n") *> promptBool(label, current)
      }
    }

  private def promptStrList(label: String, current: List[String]): RIO[Any, List[String]] =
    prompt(s"$label [${current.mkString(",")}] (comma-separated, Enter=keep, - =empty): ").map {
      case ""  => current
      case "-" => Nil
      case s   => s.split(',').map(_.trim).filter(_.nonEmpty).toList
    }

  private def promptClubIdList(label: String, current: List[ClubId]): RIO[Any, List[ClubId]] = {
    val shown = current.map(ClubId.unwrap).mkString(",")
    prompt(s"$label [$shown] (comma-separated ids, Enter=keep, - =empty): ").flatMap {
      case ""  => ZIO.succeed(current)
      case "-" => ZIO.succeed(Nil)
      case s =>
        val parsed = s.split(',').map(_.trim).filter(_.nonEmpty).toList.map { tok =>
          tok.toLongOption.toRight(s"'$tok' is not a valid id")
            .flatMap(l => if (l < 0) Left(s"club id must be >= 0 (got $l)") else Right(ClubId(l)))
        }
        parsed.partitionMap(identity) match {
          case (Nil, ids) => ZIO.succeed(ids)
          case (errs, _)  => Console.printLine(s"  ${errs.mkString("; ")}") *> promptClubIdList(label, current)
        }
    }
  }

  private def promptCriteria(clubSlug: ClubSlug, alias: String): RIO[PostgresClient, Option[RecruitmentCriteria]] = {
    val a = alias.trim
    for {
      club     <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      existing <- latestCriteria(club.clubId, a).map(_.map(_._2))
      base = existing.getOrElse(RecruitmentCriteria.defaultDaily)
      _ <- Console.printLine(
        if (existing.isDefined) { s"Editing criteria for alias '$a' (Enter keeps the current value)." }
        else { s"New alias '$a' — defaults shown from defaultDaily (Enter keeps)." }
      )
      minDaysSinceRegistration   <- promptOptInt("minDaysSinceRegistration", base.minDaysSinceRegistration)
      daysSinceLastInvited       <- promptOptInt("daysSinceLastInvited", base.daysSinceLastInvited)
      daysSinceRejected          <- promptOptInt("daysSinceRejected", base.daysSinceRejected)
      nationalityExclude         <- promptBool("nationalityExclude", base.nationalityExclude)
      nationalityCountries       <- promptStrList("nationalityCountries", base.nationalityCountries)
      excludeClubs               <- promptClubIdList("excludeClubs", base.excludeClubs)
      maxClubs                   <- promptOptInt("maxClubs", base.maxClubs)
      excludeSourceAdmins        <- promptBool("excludeSourceAdmins", base.excludeSourceAdmins)
      avoidAdminMinClubSize      <- promptOptInt("avoidAdminMinClubSize", base.avoidAdminMinClubSize)
      excludeFormerMembers       <- promptBool("excludeFormerMembers", base.excludeFormerMembers)
      dailyMinElo                <- promptOptElo("dailyMinElo", base.dailyMinElo)
      dailyMaxElo                <- promptOptElo("dailyMaxElo", base.dailyMaxElo)
      dailyMinScoreRate          <- promptOptDouble("dailyMinScoreRate", base.dailyMinScoreRate)
      dailyMaxScoreRate          <- promptOptDouble("dailyMaxScoreRate", base.dailyMaxScoreRate)
      dailyMinGamesFinished      <- promptOptInt("dailyMinGamesFinished", base.dailyMinGamesFinished)
      dailyMinTmGamesFinished    <- promptOptInt("dailyMinTmGamesFinished", base.dailyMinTmGamesFinished)
      dailyMaxTimeoutPercent     <- promptOptDouble("dailyMaxTimeoutPercent", base.dailyMaxTimeoutPercent)
      dailyMaxTmTimeoutPercent   <- promptOptDouble("dailyMaxTmTimeoutPercent", base.dailyMaxTmTimeoutPercent)
      dailyMaxHoursPerMove       <- promptOptInt("dailyMaxHoursPerMove", base.dailyMaxHoursPerMove)
      dailyMinOngoingGames       <- promptOptInt("dailyMinOngoingGames", base.dailyMinOngoingGames)
      dailyMaxOngoingGames       <- promptOptInt("dailyMaxOngoingGames", base.dailyMaxOngoingGames)
      dailyMinOngoingTeamMatches <- promptOptInt("dailyMinOngoingTeamMatches", base.dailyMinOngoingTeamMatches)
      candidate = RecruitmentCriteria(
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
      ds = diffLines(existing, candidate.capped)
      _  <- Console.printLine("")
      _  <- Console.printLine("--- Changes ---")
      _ <- {
        if (ds.isEmpty) { Console.printLine("  (no changes)") }
        else { ZIO.foreachDiscard(ds)(d => Console.printLine(s"  $d")) }
      }
      confirm <- prompt("Save? [Y/n]: ").map(_.toLowerCase).map(s => s.isEmpty || s == "y" || s == "yes")
    } yield if (confirm) { Some(candidate) } else { None }
  }
}

/** Wire shape for `RecruitmentCriteria` minus the server-assigned `criteria_id` surrogate. Shared by the CLI
  * `--json <file>` path and the HTTP route body.
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
