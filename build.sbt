val versionCCAS  = "0.1.0-SNAPSHOT"
val versionScala = "3.6.2"

ThisBuild / version      := versionCCAS
ThisBuild / scalaVersion := versionScala

// versions

val versionZio           = "2.1.14"
val versionZioHttp       = "3.0.1"
val versionZioJson       = "0.7.3"
val versionZioProtoQuill = "4.8.6"
val versionMagnolia      = "1.3.8"
val versionPostgres      = "42.7.4"

// modules

val zio             = "dev.zio"                      %% "zio"               % versionZio
val zioTest         = "dev.zio"                      %% "zio-test"          % versionZio % Test
val zioTestSbt      = "dev.zio"                      %% "zio-test-sbt"      % versionZio % Test
val zioTestMagnolia = "dev.zio"                      %% "zio-test-magnolia" % versionZio % Test
val zioHttp         = "dev.zio"                      %% "zio-http"          % versionZioHttp
val zioJson         = "dev.zio"                      %% "zio-json"          % versionZioJson
val zioProtoQuill   = "io.getquill"                  %% "quill-jdbc-zio"    % versionZioProtoQuill
val magnolia        = "com.softwaremill.magnolia1_3" %% "magnolia"          % versionMagnolia
val postgresql      = "org.postgresql"               %  "postgresql"        % versionPostgres

libraryDependencies ++= Seq(
  zio,
  zioTest,
  zioTestSbt,
  zioTestMagnolia,
  zioHttp,
  zioJson,
  zioProtoQuill,
  magnolia,
  postgresql,
)

scalacOptions ++= Seq(
  "--deprecation",
  "--feature",
  "-Wunused:all",
  "-Wshadow:all",
  "-Wvalue-discard",
  "-Xmax-inlines:256",
)

lazy val root = (project in file(".")).settings(
  name := "ccas",
)
