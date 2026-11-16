package ccas.cli

import zio.{Console, Task, UIO, ZIO}

import ccas.analysis.tables.RecruitmentCriteria
import ccas.analysis.tables.RecruitmentCriteria.Check
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo}
import ccas.utils.YesNo

/** Asks for each recruitment criteria setting in turn, offering the current value, for
  * `ccas recruitment-criteria set`. Each answer is checked as it is given, by the rules the server saves by, and one it
  * would refuse is asked again rather than discovered after the last setting.
  */
private[cli] object CriteriaPrompts {

  /** How excluded clubs are named for the operator, and found from what the operator types: the server's answers.
    *
    * @param name the club's slug, or `#<id>` for one the server can't name
    * @param find the club a slug reaches, or why it reaches none
    * @param note tells the operator what names reached, once a line of them is taken
    */
  final case class ClubNames(
    name: ClubId => Task[String],
    find: ClubSlug => Task[Either[String, Reached]],
    note: List[String] => UIO[Unit]
  )

  /** The club a typed name excludes, and a note to show when that is not simply the club holding the name now. */
  final case class Reached(clubId: ClubId, noteOption: Option[String])

  /** How many club lookups go to the server at once. */
  val LookupParallelism = 8

  /** Prints `heading`, then asks for every setting, starting from `base`. */
  def criteria(heading: String, base: RecruitmentCriteria, clubs: ClubNames): Task[RecruitmentCriteria] =
    for {
      _                        <- Console.printLine(heading)
      minDaysSinceRegistration <- promptCount("minDaysSinceRegistration", base.minDaysSinceRegistration)
      daysSinceLastInvited     <- promptCount("daysSinceLastInvited", base.daysSinceLastInvited)
      daysSinceRejected        <- promptCount("daysSinceRejected", base.daysSinceRejected)
      nationalityExclude       <- promptBool("nationalityExclude", base.nationalityExclude)
      nationalityCountries     <- promptStrList("nationalityCountries", base.nationalityCountries)
      excludeClubs             <- promptClubs("excludeClubs", base.excludeClubs, clubs)
      maxClubs                 <- promptCount("maxClubs", base.maxClubs)
      excludeSourceAdmins      <- promptBool("excludeSourceAdmins", base.excludeSourceAdmins)
      avoidAdminMinClubSize    <- promptCount("avoidAdminMinClubSize", base.avoidAdminMinClubSize)
      excludeFormerMembers     <- promptBool("excludeFormerMembers", base.excludeFormerMembers)
      dailyMinElo              <- promptOptElo("dailyMinElo", base.dailyMinElo, _ => None)
      dailyMaxElo       <- promptOptElo("dailyMaxElo", base.dailyMaxElo, Check.ordered("dailyElo", dailyMinElo, _))
      dailyMinScoreRate <- promptRate("dailyMinScoreRate", base.dailyMinScoreRate, Check.inUnit, _ => None)
      dailyMaxScoreRate <- promptRate(
        label = "dailyMaxScoreRate",
        current = base.dailyMaxScoreRate,
        range = Check.inUnit,
        alsoCheck = Check.ordered("dailyScoreRate", dailyMinScoreRate, _)
      )
      dailyMinGamesFinished   <- promptCount("dailyMinGamesFinished", base.dailyMinGamesFinished)
      dailyMinTmGamesFinished <- promptCount("dailyMinTmGamesFinished", base.dailyMinTmGamesFinished)
      dailyMaxTimeoutPercent <-
        promptRate("dailyMaxTimeoutPercent", base.dailyMaxTimeoutPercent, Check.inPercent, _ => None)
      dailyMaxTmTimeoutPercent <-
        promptRate("dailyMaxTmTimeoutPercent", base.dailyMaxTmTimeoutPercent, Check.inPercent, _ => None)
      dailyMaxHoursPerMove <- promptCount("dailyMaxHoursPerMove", base.dailyMaxHoursPerMove)
      dailyMinOngoingGames <- promptCount("dailyMinOngoingGames", base.dailyMinOngoingGames)
      dailyMaxOngoingGames <- promptCountAnd(
        label = "dailyMaxOngoingGames",
        current = base.dailyMaxOngoingGames,
        alsoCheck = Check.ordered("dailyOngoingGames", dailyMinOngoingGames, _)
      )
      dailyMinOngoingTeamMatches <- promptCount("dailyMinOngoingTeamMatches", base.dailyMinOngoingTeamMatches)
    } yield RecruitmentCriteria(
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

  private def prompt(label: String): Task[String] =
    CliPrompt.line(label, ended = "input ended before every setting was answered")

  /** Asks until the answer parses and `check` finds nothing wrong with the setting it gives, a kept one included: the
    * second of a min/max pair is checked against the first, which may have changed.
    */
  private def promptOpt[A](
    label: String,
    current: Option[A],
    show: A => String,
    parse: String => Either[String, A],
    check: Option[A] => Option[String]
  ): Task[Option[A]] = {
    val shown = current.fold("none")(show)
    prompt(s"$label [$shown] (Enter=keep, - =none): ").flatMap { typed =>
      val answer = typed match {
        case ""  => Right(current)
        case "-" => Right(None)
        case s   => parse(s).map(Some(_))
      }
      answer.flatMap(value => check(value).toLeft(value)) match {
        case Right(value)  => ZIO.succeed(value)
        case Left(problem) => Console.printLine(s"  $problem") *> promptOpt(label, current, show, parse, check)
      }
    }
  }

  /** A day or count setting: a whole number, never below 0. */
  private def promptCount(label: String, current: Option[Int]): Task[Option[Int]] =
    promptCountAnd(label, current, _ => None)

  private def promptCountAnd(
    label: String,
    current: Option[Int],
    alsoCheck: Option[Int] => Option[String]
  ): Task[Option[Int]] =
    promptOpt(
      label = label,
      current = current,
      show = _.toString,
      parse = s => s.toIntOption.toRight(s"'$s' is not an integer"),
      check = value => Check.nonNeg(label, value).orElse(alsoCheck(value))
    )

  /** A rate or a percentage, held to `range`. */
  private def promptRate(
    label: String,
    current: Option[Double],
    range: (String, Option[Double]) => Option[String],
    alsoCheck: Option[Double] => Option[String]
  ): Task[Option[Double]] =
    promptOpt(
      label = label,
      current = current,
      show = _.toString,
      parse = s => s.toDoubleOption.toRight(s"'$s' is not a number"),
      check = value => range(label, value).orElse(alsoCheck(value))
    )

  private def promptOptElo(
    label: String,
    current: Option[Elo],
    check: Option[Elo] => Option[String]
  ): Task[Option[Elo]] =
    promptOpt(
      label = label,
      current = current,
      show = e => Elo.unwrap(e).toString,
      parse = s =>
        s.toIntOption.toRight(s"'$s' is not an integer").flatMap { i =>
          if (i < 0 || i > Short.MaxValue) Left(s"must be in [0, ${Short.MaxValue}] (got $i)")
          else Right(Elo(i.toShort))
        },
      check = check
    )

  // Shown as yes or no, as the recap shows a flag, since that is what it is answered with.
  private def promptBool(label: String, current: Boolean): Task[Boolean] = {
    val shown = if (current) { "yes" }
    else { "no" }
    prompt(s"$label [$shown] (y/n, Enter=keep): ").flatMap { typed =>
      YesNo.read(typed, default = current) match {
        case Some(answer) => ZIO.succeed(answer)
        case None         => Console.printLine(YesNo.AskAgain) *> promptBool(label, current)
      }
    }
  }

  private def promptStrList(label: String, current: List[String]): Task[List[String]] =
    prompt(s"$label [${current.mkString(",")}] (comma-separated, Enter=keep, - =empty): ").map {
      case ""  => current
      case "-" => Nil
      case s   => s.split(',').map(_.trim).filter(_.nonEmpty).toList
    }

  /** Clubs shown and taken by slug, or as `#<id>`, the form a club the server can't name is shown in. */
  private def promptClubs(label: String, current: List[ClubId], clubs: ClubNames): Task[List[ClubId]] =
    ZIO.foreachPar(current)(clubs.name).withParallelism(LookupParallelism).flatMap { shown =>
      askClubs(
        s"$label [${shown.mkString(",")}] (comma-separated slugs or #ids, Enter=keep, - =empty): ",
        current,
        clubs
      )
    }

  // A line is taken whole or asked again whole, so a name's note is shown only once every name on it is found.
  private def askClubs(question: String, current: List[ClubId], clubs: ClubNames): Task[List[ClubId]] =
    prompt(question).flatMap {
      case ""  => ZIO.succeed(current)
      case "-" => ZIO.succeed(Nil)
      case typed =>
        val names = typed.split(',').map(_.trim).filter(_.nonEmpty).toList
        ZIO.foreachPar(names)(club(_, clubs)).withParallelism(LookupParallelism).flatMap { found =>
          found.partitionMap(identity) match {
            case (Nil, reached) =>
              clubs.note(reached.flatMap(_.noteOption)).as(reached.map(_.clubId).distinct)
            case (problems, _) =>
              Console.printLine(s"  ${problems.mkString("; ")}") *> askClubs(question, current, clubs)
          }
        }
    }

  // `#<id>` is the club's identity and needs no server. Anything else is a slug, found through it, digits included,
  // since a slug can be all digits.
  private def club(typed: String, clubs: ClubNames): Task[Either[String, Reached]] =
    if (typed.startsWith("#")) {
      ZIO.succeed(
        typed.drop(1).toLongOption.filter(_ > 0).map(id => Reached(ClubId(id), None)).toRight(
          s"'$typed' is not a club id"
        )
      )
    } else {
      clubs.find(ClubSlug(typed))
    }
}
