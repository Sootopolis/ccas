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
  "com.github.f4b6a3" % "ulid-creator" % vUlidCreator
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
    buildInfoPackage                := "ccas.info"
  )
