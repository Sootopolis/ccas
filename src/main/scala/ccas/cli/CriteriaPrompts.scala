package ccas.cli

import zio.{Console, Task, UIO, ZIO}

import ccas.analysis.tables.RecruitmentCriteria
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
    Console.printLine(heading) *>
      ZIO.foldLeft(steps(clubs))(base.copy(criteriaId = 0))((answered, step) => step(answered))

  // One per setting, in the order the criteria hold them: each offers the setting from the answers so far and writes
  // the operator's answer back into them, where the rules can see it beside every earlier one.
  private def steps(clubs: ClubNames): List[RecruitmentCriteria => Task[RecruitmentCriteria]] = List(
    c => promptCount("minDaysSinceRegistration", c.minDaysSinceRegistration)(v => c.copy(minDaysSinceRegistration = v)),
    c => promptCount("daysSinceLastInvited", c.daysSinceLastInvited)(v => c.copy(daysSinceLastInvited = v)),
    c => promptCount("daysSinceRejected", c.daysSinceRejected)(v => c.copy(daysSinceRejected = v)),
    c => promptBool("nationalityExclude", c.nationalityExclude).map(v => c.copy(nationalityExclude = v)),
    c => promptStrList("nationalityCountries", c.nationalityCountries).map(v => c.copy(nationalityCountries = v)),
    c => promptClubs("excludeClubs", c.excludeClubs, clubs).map(v => c.copy(excludeClubs = v)),
    c => promptCount("maxClubs", c.maxClubs)(v => c.copy(maxClubs = v)),
    c => promptBool("excludeSourceAdmins", c.excludeSourceAdmins).map(v => c.copy(excludeSourceAdmins = v)),
    c => promptCount("avoidAdminMinClubSize", c.avoidAdminMinClubSize)(v => c.copy(avoidAdminMinClubSize = v)),
    c => promptBool("excludeFormerMembers", c.excludeFormerMembers).map(v => c.copy(excludeFormerMembers = v)),
    c => promptElo("dailyMinElo", c.dailyMinElo)(v => c.copy(dailyMinElo = v)),
    c => promptElo("dailyMaxElo", c.dailyMaxElo)(v => c.copy(dailyMaxElo = v)),
    c => promptRate("dailyMinScoreRate", c.dailyMinScoreRate)(v => c.copy(dailyMinScoreRate = v)),
    c => promptRate("dailyMaxScoreRate", c.dailyMaxScoreRate)(v => c.copy(dailyMaxScoreRate = v)),
    c => promptCount("dailyMinGamesFinished", c.dailyMinGamesFinished)(v => c.copy(dailyMinGamesFinished = v)),
    c => promptCount("dailyMinTmGamesFinished", c.dailyMinTmGamesFinished)(v => c.copy(dailyMinTmGamesFinished = v)),
    c => promptRate("dailyMaxTimeoutPercent", c.dailyMaxTimeoutPercent)(v => c.copy(dailyMaxTimeoutPercent = v)),
    c => promptRate("dailyMaxTmTimeoutPercent", c.dailyMaxTmTimeoutPercent)(v => c.copy(dailyMaxTmTimeoutPercent = v)),
    c => promptCount("dailyMaxHoursPerMove", c.dailyMaxHoursPerMove)(v => c.copy(dailyMaxHoursPerMove = v)),
    c => promptCount("dailyMinOngoingGames", c.dailyMinOngoingGames)(v => c.copy(dailyMinOngoingGames = v)),
    c => promptCount("dailyMaxOngoingGames", c.dailyMaxOngoingGames)(v => c.copy(dailyMaxOngoingGames = v)),
    c =>
      promptCount("dailyMinOngoingTeamMatches", c.dailyMinOngoingTeamMatches) { v =>
        c.copy(dailyMinOngoingTeamMatches = v)
      }
  )

  private val Ended = "input ended before every setting was answered"

  private def prompt(label: String): Task[String] =
    CliPrompt.line(label, Ended)

  /** Asks until the answer parses and, written into the criteria by `fill`, breaks none of the
    * [[RecruitmentCriteria.Rules]] for `label`, a kept answer included: the max of a min/max pair is judged against the
    * min, which may have changed. The only prompt that checks them: a rule for a setting asked any other way fails
    * `TestRecruitmentCriteriaCommand`.
    */
  private def promptOpt[A](
    label: String,
    current: Option[A],
    show: A => String,
    parse: String => Either[String, A],
    fill: Option[A] => RecruitmentCriteria
  ): Task[RecruitmentCriteria] = {
    val shown = current.fold("none")(show)
    prompt(s"$label [$shown] (Enter=keep, - =none): ").flatMap { typed =>
      val answer = typed match {
        case ""  => Right(current)
        case "-" => Right(None)
        case s   => parse(s).map(Some(_))
      }
      answer.map(fill).flatMap(answered => answered.problemsAt(label).toLeft(answered)) match {
        case Right(answered) => ZIO.succeed(answered)
        case Left(problem)   => Console.printLine(s"  $problem") *> promptOpt(label, current, show, parse, fill)
      }
    }
  }

  /** A day or count setting: a whole number. */
  private def promptCount(label: String, current: Option[Int])(
    fill: Option[Int] => RecruitmentCriteria
  ): Task[RecruitmentCriteria] =
    promptOpt(label, current, _.toString, s => s.toIntOption.toRight(s"'$s' is not an integer"), fill)

  /** A rate or a percentage. */
  private def promptRate(label: String, current: Option[Double])(
    fill: Option[Double] => RecruitmentCriteria
  ): Task[RecruitmentCriteria] =
    promptOpt(label, current, _.toString, s => s.toDoubleOption.toRight(s"'$s' is not a number"), fill)

  private def promptElo(label: String, current: Option[Elo])(
    fill: Option[Elo] => RecruitmentCriteria
  ): Task[RecruitmentCriteria] =
    promptOpt(
      label = label,
      current = current,
      show = e => Elo.unwrap(e).toString,
      parse = s =>
        s.toIntOption.toRight(s"'$s' is not an integer").flatMap { i =>
          if (i < 0 || i > Short.MaxValue) Left(s"must be in [0, ${Short.MaxValue}] (got $i)")
          else Right(Elo(i.toShort))
        },
      fill = fill
    )

  // Shown as yes or no, as the recap shows a flag, since that is what it is answered with.
  private def promptBool(label: String, current: Boolean): Task[Boolean] = {
    val shown = if (current) { "yes" }
    else { "no" }
    val question = s"$label [$shown] (y/n, Enter=keep): "
    YesNo.askWith(question, current, CliPrompt.readLine, Console.print(_), ZIO.fail(CliError(Ended, 1)))
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
