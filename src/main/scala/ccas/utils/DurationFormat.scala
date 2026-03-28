package ccas.utils

import java.time.{Duration as JDuration}

extension (d: JDuration) def display: String = s"${d.toMinutes}m ${d.toSecondsPart}s"
