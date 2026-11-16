package ccas.cli

import zio.{Console, Task, ZIO}

import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.{ClubId, Elo}

/** Asks for each recruitment criteria setting in turn, offering the current value, for
  * `ccas recruitment-criteria set`.
  */
private[cli] object CriteriaPrompts {

  /** Prints `heading`, then asks for every setting, starting from `base`. */
  def criteria(heading: String, base: RecruitmentCriteria): Task[RecruitmentCriteria] =
    for {
      _                          <- Console.printLine(heading)
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

  private def promptOpt[A](
    label: String,
    current: Option[A],
    show: A => String,
    parse: String => Either[String, A]
  ): Task[Option[A]] = {
    val shown = current.fold("none")(show)
    prompt(s"$label [$shown] (Enter=keep, - =none): ").flatMap {
      case ""  => ZIO.succeed(current)
      case "-" => ZIO.none
      case s =>
        parse(s) match {
          case Right(v)  => ZIO.some(v)
          case Left(err) => Console.printLine(s"  $err") *> promptOpt(label, current, show, parse)
        }
    }
  }

  private def promptOptInt(label: String, current: Option[Int]): Task[Option[Int]] =
    promptOpt(label, current, _.toString, s => s.toIntOption.toRight(s"'$s' is not an integer"))

  private def promptOptDouble(label: String, current: Option[Double]): Task[Option[Double]] =
    promptOpt(label, current, _.toString, s => s.toDoubleOption.toRight(s"'$s' is not a number"))

  private def promptOptElo(label: String, current: Option[Elo]): Task[Option[Elo]] =
    promptOpt(
      label = label,
      current = current,
      show = e => Elo.unwrap(e).toString,
      parse = s =>
        s.toIntOption.toRight(s"'$s' is not an integer").flatMap { i =>
          if (i < 0 || i > Short.MaxValue) Left(s"must be in [0, ${Short.MaxValue}] (got $i)")
          else Right(Elo(i.toShort))
        }
    )

  private def promptBool(label: String, current: Boolean): Task[Boolean] =
    prompt(s"$label [$current] (y/n, Enter=keep): ").flatMap { s =>
      s.toLowerCase match {
        case ""                         => ZIO.succeed(current)
        case "y" | "yes" | "true" | "t" => ZIO.succeed(true)
        case "n" | "no" | "false" | "f" => ZIO.succeed(false)
        case _                          => Console.printLine("  enter y or n") *> promptBool(label, current)
      }
    }

  private def promptStrList(label: String, current: List[String]): Task[List[String]] =
    prompt(s"$label [${current.mkString(",")}] (comma-separated, Enter=keep, - =empty): ").map {
      case ""  => current
      case "-" => Nil
      case s   => s.split(',').map(_.trim).filter(_.nonEmpty).toList
    }

  private def promptClubIdList(label: String, current: List[ClubId]): Task[List[ClubId]] = {
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
}
