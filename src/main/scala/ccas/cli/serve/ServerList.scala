package ccas.cli.serve

import java.nio.file.Path

import zio.{Console, ExitCode, UIO, ZIO}

import ccas.cli.serve.HealthProbe.Readiness
import ccas.utils.errors.safeMessage

/** `ccas server list`: a row for each port with a pid file in the state directory, plus this shell's port, so that a
  * foreground server there shows too. Every port is probed as `status` probes one, all at once, since a cold database
  * can take the whole readiness timeout. It only reads: a stale pid file is listed, not removed. The rows are laid out
  * by the pure [[render]]; [[run]] only wires the effects, as in [[ServerStatus]].
  */
object ServerList {

  /** One port's row: its pid file, and what [[HealthProbe.probe]] got. */
  final case class Row(port: Int, pidFile: PidFile.State, readiness: Readiness, up: Boolean)

  private final case class Line(marked: Boolean, port: String, pid: String, state: String, database: String)

  /** One line per row, in columns: the port, its pid file, what the probes got, and the database the server names.
    * `shellPort`'s row is marked `*`. Pure.
    */
  def render(rows: List[Row], shellPort: Int): List[String] = {
    val lines = rows.map { row =>
      Line(
        marked = row.port == shellPort,
        port = row.port.toString,
        pid = pidCell(row.pidFile),
        state = stateCell(row.readiness, row.up),
        database = databaseCell(row.readiness)
      )
    }
    val portWidth  = widest(lines.map(_.port))
    val pidWidth   = widest(lines.map(_.pid))
    val stateWidth = widest(lines.map(_.state))
    lines.map { line =>
      val marker = if (line.marked) { "*" }
      else { " " }
      val port    = " " * (portWidth - line.port.length) + line.port
      val columns = List(port, line.pid.padTo(pidWidth, ' '), line.state.padTo(stateWidth, ' '), line.database)
      s"$marker ${columns.mkString("  ")}".stripTrailing
    }
  }

  private def widest(cells: List[String]): Int = cells.map(_.length).maxOption.getOrElse(0)

  private def pidCell(pidFile: PidFile.State): String = pidFile match {
    case PidFile.State.Live(pid)  => s"pid $pid"
    case PidFile.State.Dead(pid)  => s"pid $pid dead"
    case PidFile.State.Corrupt    => "corrupt pid file"
    case PidFile.State.Unreadable => "unreadable pid file"
    case PidFile.State.Absent     => "no pid file"
  }

  /** The probes' answer alone. What `status` folds in from the pid file ("starting or unhealthy", "stale pid file") is
    * the pid column's to say.
    */
  private def stateCell(readiness: Readiness, up: Boolean): String = readiness match {
    case Readiness.Ready(_)   => "ready"
    case Readiness.Unready(_) => "db unavailable"
    case Readiness.NoAnswer =>
      if (up) { "db unavailable" }
      else { "not answering" }
  }

  private def databaseCell(readiness: Readiness): String = readiness match {
    case Readiness.NoAnswer => ""
    case answered           => HealthProbe.databaseLabel(answered.databaseOption)
  }

  def run(stateDir: Path, shellPort: Int): UIO[ExitCode] =
    run(stateDir = stateDir, shellPort = shellPort, probe = HealthProbe.probe)

  /** @param probe what [[HealthProbe.probe]] answers for a port; injected so tests need no network */
  private[serve] def run(stateDir: Path, shellPort: Int, probe: Int => UIO[(Readiness, Boolean)]): UIO[ExitCode] = {
    val program =
      for {
        ports <- ZIO.attemptBlocking(PidFile.ports(stateDir))
        rows  <- ZIO.foreachPar((shellPort :: ports).distinct.sorted)(port => row(stateDir, port, probe))
        _     <- ZIO.foreachDiscard(render(rows, shellPort))(Console.printLine(_))
      } yield ExitCode.success
    program.catchAllCause(c => Console.printLineError(s"error: ${c.squash.safeMessage}").orDie.as(ExitCode(1)))
  }

  private def row(stateDir: Path, port: Int, probe: Int => UIO[(Readiness, Boolean)]): UIO[Row] =
    ZIO.succeedBlocking(PidFile.state(PidFile.path(stateDir, port))).zipPar(probe(port)).map {
      case (pidFile, (readiness, up)) => Row(port = port, pidFile = pidFile, readiness = readiness, up = up)
    }
}
