import Versions.*

ThisBuild / version      := vCcas
ThisBuild / scalaVersion := vScala
ThisBuild / sbtVersion   := vSbt

// modules
libraryDependencies ++= Seq(
  // zio
  "dev.zio" %% "zio"               % vZio,
  "dev.zio" %% "zio-test"          % vZio % Test,
  "dev.zio" %% "zio-test-sbt"      % vZio % Test,
  "dev.zio" %% "zio-test-magnolia" % vZio % Test,

  // zio-json
  "dev.zio" %% "zio-json" % vZioJson,

  // zio-config
  "dev.zio" %% "zio-config"          % vZioConfig,
  "dev.zio" %% "zio-config-magnolia" % vZioConfig,
  "dev.zio" %% "zio-config-typesafe" % vZioConfig,

  // zio-http
  "dev.zio" %% "zio-http" % vZioHttp,

  // magnum
  "com.augustnagro" %% "magnum" % vMagnum,

  // postgresql
  "org.postgresql" % "postgresql" % vPostgresql,

  // connection pool
  "com.zaxxer" % "HikariCP" % vHikari,

  // ulid
  "com.github.f4b6a3" % "ulid-creator" % vUlidCreator,

  // zio-cli (CLI parsing — ZIO-native, also generates shell completions)
  "dev.zio" %% "zio-cli" % vZioCli
)

scalacOptions ++= Seq(
  "--deprecation",
  "--feature",
  "-Wunused:all",
  "-Wshadow:all",
  "-Wvalue-discard",
  "-Xmax-inlines:64"
)

Test / parallelExecution := true

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging)
  .settings(
    name := "ccas",
    // `sbt stage` emits two launchers: `bin/ccas` (primary, the CLI) and a forwarder
    // `bin/ccas-server` for the deployable server entry. Pin discoveredMainClasses so
    // native-packager only forwards CcasServer, not every ZIOAppDefault app.
    Compile / mainClass             := Some("ccas.cli.Main"),
    executableScriptName            := "ccas",
    Compile / discoveredMainClasses := Seq("ccas.cli.Main", "ccas.server.CcasServer"),
    buildInfoKeys                   := Seq(name, version, scalaVersion, sbtVersion),
    buildInfoPackage                := "ccas.info",
    // Silence the sun.misc.Unsafe deprecation warning (scala-library's LazyVals) that the JVM
    // prints on JDK 24+. The `--sun-misc-unsafe-memory-access` flag only exists on JDK 23+, so
    // probe the runtime version in the launcher and add it conditionally (older JDKs don't warn).
    bashScriptExtraDefines ++= Seq(
      """java_major=$("${java_cmd:-java}" -version 2>&1 | head -n1 | sed -E 's/.*version "?([0-9]+).*/\1/')""",
      """if [ "${java_major:-0}" -ge 23 ] 2>/dev/null; then addJava "--sun-misc-unsafe-memory-access=allow"; fi"""
    )
  )
