package ccas.cli

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant

import scala.util.Try

import zio.{Console, IO, Task, ZIO}
import zio.json.EncoderOps

import ccas.analysis.apps.{ClubQuery, NamedClub}
import ccas.analysis.apps.recruitment.{CriteriaRecap, CriteriaSpec, RecruitmentCriteriaApp}
import ccas.analysis.tables.{RecruitmentAlias, RecruitmentCriteria}
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.routes.{ClubRequest, ClubResult}
import ccas.server.routes.RecruitmentCriteriaRoutes.{
  AliasSummary,
  CriteriaView,
  SetCriteriaRequest,
  SetCriteriaResponse
}
import ccas.utils.YesNo

/** `ccas recruitment-criteria show | set | list`: a club's named criteria, through the server's criteria routes. Each
  * settles its club as every club command does; `set` then names it by id.
  */
private[cli] object CriteriaCommand {

  def show(
    wiring: CliWiring,
    api: CcasApiClient,
    currentClubOption: Option[String],
    show: CliCommand.CriteriaShow
  ): Task[Int] =
    for {
      name   <- criteriaName(show.names)
      target <- ClubResolver.single(show.club, show.clubIdOption, currentClubOption)
      view <- Dispatcher.actOnClub(wiring, currentClubOption, target) { chosen =>
        api.getJson[ClubResult[CriteriaView]](viewPath(name, chosen.query))
      }.map(_._2)
      _ <-
        if (show.json) { Console.printLine(view.criteria.toJsonPretty) }
        else { ZIO.foreachDiscard(view.recap)(Console.printLine(_)) }
    } yield 0

  def list(
    wiring: CliWiring,
    api: CcasApiClient,
    currentClubOption: Option[String],
    nonDefaultServerOption: Option[String],
    list: CliCommand.CriteriaList
  ): Task[Int] =
    for {
      target        <- ClubResolver.single(list.club, list.clubIdOption, currentClubOption)
      (club, names) <- listNames(wiring, api, currentClubOption, target)
      lines = listLines(nonDefaultServerOption, club.slug, names)
      _ <- ZIO.foreachDiscard(lines)(Console.printLine(_))
    } yield 0

  /** What `list` prints: each name with its version, or how to set the first. */
  private def listLines(
    nonDefaultServerOption: Option[String],
    club: ClubSlug,
    names: List[AliasSummary]
  ): List[String] = {
    val slug  = ClubSlug.unwrap(club)
    val width = names.map(quoted(_).length).maxOption.getOrElse(0)
    if (names.isEmpty) {
      List(
        s"$slug has no criteria yet. Set some with: " +
          setCommand(nonDefaultServerOption, club, RecruitmentAlias.Default)
      )
    } else {
      s"$slug's criteria:" :: names.map(n => s"  ${quoted(n).padTo(width, ' ')}  version of ${version(n.since)}")
    }
  }

  private def quoted(name: AliasSummary): String = s"\"${name.alias}\""

  // The route sends an instant's own text; one it can't parse is shown as sent rather than failing the list.
  private def version(since: String): String =
    Try(Instant.parse(since)).toOption.fold(since)(CriteriaRecap.version)

  /** Asks for each setting, or reads `--file`, then saves what changed. A save makes a new version, which every later
    * run under the name uses, scheduled ones included, so a save at a terminal says so and asks first, from a file too.
    */
  def set(
    wiring: CliWiring,
    api: CcasApiClient,
    currentClubOption: Option[String],
    set: CliCommand.CriteriaSet,
    interactive: Boolean
  ): Task[Int] =
    for {
      name          <- criteriaName(set.names)
      fileCriteria  <- ZIO.foreach(set.fileOption)(RecruitmentCriteriaApp.loadFromJson)
      _             <- ZIO.whenDiscard(fileCriteria.isEmpty && !interactive)(ZIO.fail(CliError(NeedsTerminal, 2)))
      target        <- ClubResolver.single(set.club, set.clubIdOption, currentClubOption)
      (club, names) <- listNames(wiring, api, currentClubOption, target)
      existing <- ZIO.foreach(names.find(_.alias == name)) { _ =>
        clubAnswer(api.getJson[ClubResult[CriteriaView]](viewPath(name, ClubQuery.ById(club.clubId))))
          .map(_.criteria.toCriteria)
      }
      candidate <- fileCriteria match {
        case Some(criteria) => ZIO.succeed(criteria)
        case None => CriteriaPrompts.criteria(heading(club.slug, name, existing.isDefined), existingOr(existing))
      }
      code <- saveChanges(api, club, name, existing, candidate, confirm = interactive)
    } yield code

  private val NeedsTerminal = "set asks for each setting, so it needs a terminal; or pass --file <criteria.json>"

  private def existingOr(existing: Option[RecruitmentCriteria]): RecruitmentCriteria =
    existing.getOrElse(RecruitmentCriteria.defaultDaily)

  private def heading(club: ClubSlug, name: String, exists: Boolean): String =
    if (exists) { s"Editing ${ClubSlug.unwrap(club)}'s criteria \"$name\" (Enter keeps the current value)." }
    else { s"New criteria \"$name\" for ${ClubSlug.unwrap(club)}, starting from the defaults (Enter keeps)." }

  private def saveChanges(
    api: CcasApiClient,
    club: NamedClub,
    name: String,
    existing: Option[RecruitmentCriteria],
    candidate: RecruitmentCriteria,
    confirm: Boolean
  ): Task[Int] =
    for {
      // Refused here as the server would refuse it, before the operator is asked to save what can't be saved.
      _ <- ZIO.fromEither(candidate.capped.validate).mapError(problem => CliError(s"not saved: $problem", 1))
      _ <- capNote(candidate)
      _ <- changeLines(existing, candidate.capped) match {
        case Nil     => Console.printLine("No changes; nothing saved.")
        case changes => saveWhatChanged(api, club, name, candidate, changes, confirm)
      }
    } yield 0

  // A new name has no version to compare with, so what is about to be saved is shown against the defaults instead.
  private def changeLines(existing: Option[RecruitmentCriteria], candidate: RecruitmentCriteria): List[String] =
    existing match {
      case Some(_) => RecruitmentCriteriaApp.diffLines(existing, candidate)
      case None =>
        RecruitmentCriteriaApp.diffLines(Some(RecruitmentCriteria.defaultDaily), candidate) match {
          case Nil          => List("(new criteria, the same as the defaults)")
          case fromDefaults => "(new criteria; what differs from the defaults)" :: fromDefaults
        }
    }

  private def saveWhatChanged(
    api: CcasApiClient,
    club: NamedClub,
    name: String,
    candidate: RecruitmentCriteria,
    changes: List[String],
    confirm: Boolean
  ): Task[Unit] =
    for {
      _ <- Console.printLine("")
      _ <- Console.printLine("--- Changes ---")
      _ <- ZIO.foreachDiscard(changes)(change => Console.printLine(s"  $change"))
      saved <-
        if (confirm) { askToSave(club.slug, name) }
        else { ZIO.succeed(true) }
      _ <-
        if (saved) { save(api, club, name, candidate) }
        else { Console.printLine("Not saved.") }
    } yield ()

  // The server saves a lookback past the cap as the cap, which can leave nothing to save, so say so before the diff.
  private def capNote(candidate: RecruitmentCriteria): Task[Unit] =
    ZIO.foreachDiscard(RecruitmentCriteriaApp.diffLines(Some(candidate), candidate.capped)) { capped =>
      Console.printLine(s"Capped to the ${RecruitmentCriteria.MaxDaysSinceLookback}-day lookback: $capped")
    }

  private def askToSave(club: ClubSlug, name: String): Task[Boolean] =
    for {
      _ <- Console.printLine(
        s"Saving makes a new version: every later run with ${ClubSlug.unwrap(club)}'s criteria \"$name\" uses it, " +
          "scheduled runs included."
      )
      yes <- YesNo.ask("Save?", default = true)
    } yield yes

  // Prints the recap as the server stored it, so its version, and any lookback the server capped, are what's shown.
  private def save(api: CcasApiClient, club: NamedClub, name: String, criteria: RecruitmentCriteria): Task[Unit] = {
    val byId = ClubQuery.ById(club.clubId)
    for {
      saved <- clubAnswer(
        api.postJson[SetCriteriaRequest, ClubResult[SetCriteriaResponse]](
          path = "/api/recruitment-criteria",
          body = SetCriteriaRequest(byId, name, CriteriaSpec.fromCriteria(criteria))
        )
      )
      view <- clubAnswer(api.getJson[ClubResult[CriteriaView]](viewPath(name, byId)))
      _ <- Console.printLine(
        // Another save in between would make the recap a later version than this one.
        if (view.criteriaIdOption.forall(_ == saved.criteriaId)) { "Saved." }
        else { "Saved, and changed again since; the current version:" }
      )
      _ <- ZIO.foreachDiscard(view.recap)(Console.printLine(_))
    } yield ()
  }

  private def listNames(
    wiring: CliWiring,
    api: CcasApiClient,
    currentClubOption: Option[String],
    target: ClubTarget
  ): Task[(NamedClub, List[AliasSummary])] =
    Dispatcher.actOnClub(wiring, currentClubOption, target) { chosen =>
      api.getJson[ClubResult[List[AliasSummary]]](s"/api/recruitment-criteria?${ClubRequest.queryString(chosen.query)}")
    }

  // A request by id, once the club is settled: the answer is the club's, or why it no longer has one.
  private def clubAnswer[A](request: Task[ClubResult[A]]): Task[A] =
    for {
      result      <- request
      (_, answer) <- ZIO.fromEither(result.toEither).mapError(CliError(_, 1))
    } yield answer

  /** The one criteria name a command names, `"default"` when it names none. A second is refused rather than ignored. */
  private[cli] def criteriaName(names: List[String]): IO[CliError, String] =
    names.map(_.trim) match {
      case Nil => ZIO.succeed(RecruitmentAlias.Default)
      case name :: Nil =>
        val max = RecruitmentCriteriaApp.MaxAliasLength
        if (name.isEmpty) { ZIO.fail(CliError("criteria name must not be blank", 2)) }
        else if (name.length > max) {
          ZIO.fail(CliError(s"criteria name must be <= $max chars (got ${name.length})", 2))
        } else { ZIO.succeed(name) }
      case name :: rest => ZIO.fail(CliError(tooManyNames(name, rest), 2))
    }

  // zio-cli has already taken the options before the name, so a rebuilt command would drop them (`--server` too); the
  // hint names only what to move.
  private def tooManyNames(name: String, rest: List[String]): String = {
    val misplaced = rest.dropWhile(!_.startsWith("-"))
    val hint =
      if (misplaced.isEmpty) { "" }
      else { s"; options must come before the name: move '${misplaced.mkString(" ")}' before '$name'" }
    s"expected at most one criteria name, got ${rest.size + 1} (${(name :: rest).mkString(", ")})$hint"
  }

  /** The command that edits `name`, for an operator to paste: with `--server` when the command printing it was given a
    * server other than the default, so the paste reaches the same one.
    */
  private[cli] def setCommand(nonDefaultServerOption: Option[String], club: ClubSlug, name: String): String = {
    val server = nonDefaultServerOption.fold("")(url => s" --server ${shellWord(url)}")
    s"ccas recruitment-criteria set$server --club ${ClubSlug.unwrap(club)} ${shellWord(name)}"
  }

  // A name is free text, so one a shell would split or expand is single-quoted, as POSIX shells read it.
  private def shellWord(value: String): String =
    if (value.matches("[A-Za-z0-9._:/-]+")) { value }
    else { "'" + value.replace("'", "'\\''") + "'" }

  /** The route that reads `name`'s criteria for `query`'s club. */
  private[cli] def viewPath(name: String, query: ClubQuery): String =
    s"/api/recruitment-criteria/${pathSegment(name)}?${ClubRequest.queryString(query)}"

  // A criteria name is free text, so it is percent-encoded as a path segment: `URLEncoder` writes a space as `+`,
  // which a path keeps literally.
  private def pathSegment(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
}
