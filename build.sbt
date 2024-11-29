ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.5.2"

val zioVersion = "2.1.13"
val zioHttpVersion = "3.0.1"
val refinedVersion = "0.11.2"

libraryDependencies ++= Seq(
  "dev.zio"    %% "zio"               % zioVersion,
  "dev.zio"    %% "zio-test"          % zioVersion % Test,
  "dev.zio"    %% "zio-test-sbt"      % zioVersion % Test,
  "dev.zio"    %% "zio-test-magnolia" % zioVersion % Test,
  "dev.zio"    %% "zio-http"          % zioHttpVersion,
)

scalacOptions ++= Seq(
  "--deprecation",
  "--feature",
  "-Wunused:all",
  "-Wshadow:all",
  "-Wvalue-discard",
  "-Xmax-inlines:16384",
)

lazy val root = (project in file("."))
  .settings(
    name := "ccas",
  )
