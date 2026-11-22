package ccas.cli

/** The desktop session the CLI runs in, which decides how it reaches the clipboard and shows a notification. */
private[cli] enum Desktop {
  case Mac, Wayland, X11, Headless
}

private[cli] object Desktop {

  def current: Desktop =
    if (sys.props.getOrElse("os.name", "").toLowerCase.contains("mac")) { Mac }
    else if (envSet("WAYLAND_DISPLAY")) { Wayland }
    else if (envSet("DISPLAY")) { X11 }
    else { Headless }

  private def envSet(name: String): Boolean = Option(System.getenv(name)).exists(_.nonEmpty)
}
