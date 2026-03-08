import Versions.*

ThisBuild / version      := vCcas
ThisBuild / scalaVersion := vScala
ThisBuild / sbtVersion   := vSbt

// modules
libraryDependencies ++= Seq(
  // zio
  "dev.zio"                      %% "zio"                   % vZio,
  "dev.zio"                      %% "zio-test"              % vZio % Test,
  "dev.zio"                      %% "zio-test-sbt"          % vZio % Test,
  "dev.zio"                      %% "zio-test-magnolia"     % vZio % Test,

  // zio-json
  "dev.zio"                      %% "zio-json"              % vZioJson,

  // zio-config
  "dev.zio"                      %% "zio-config"            % vZioConfig,
  "dev.zio"                      %% "zio-config-magnolia"   % vZioConfig,
  "dev.zio"                      %% "zio-config-typesafe"   % vZioConfig,

  // zio-http
  "dev.zio"                      %% "zio-http"              % vZioHttp,

  // magnum
  "com.augustnagro"              %% "magnum"                % vMagnum,

  // zio-schema
//  "dev.zio"                      %% "zio-schema"            % vZioSchema,
//  "dev.zio"                      %% "zio-schema-json"       % vZioSchema,
//  "dev.zio"                      %% "zio-schema-zio-test"   % vZioSchema,
//  "dev.zio"                      %% "zio-schema-derivation" % vZioSchema,
  //  "org.scala-lang"               %  "scala-reflect"         % scalaVersion.value % "provided",

  // magnolia
  "com.softwaremill.magnolia1_3" %% "magnolia"              % vMagnolia,

  // postgresql
  "org.postgresql"               %  "postgresql"            % vPostgresql,

)

scalacOptions ++= Seq(
  "--deprecation",
  "--feature",
  "-Wunused:all",
  "-Wshadow:all",
  "-Wvalue-discard",
  "-Xmax-inlines:64",
)

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "ccas",
    buildInfoKeys := Seq(name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "ccas.info"
  )
