ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.2"

val zioVersion = "2.1.13"
val zioHttpVersion = "3.0.1"
val zioJsonVersion = "0.7.3"
val magnoliaVersion = "1.3.8"

libraryDependencies ++= Seq(
  "dev.zio"                      %% "zio"               % zioVersion,
  "dev.zio"                      %% "zio-test"          % zioVersion % Test,
  "dev.zio"                      %% "zio-test-sbt"      % zioVersion % Test,
  "dev.zio"                      %% "zio-test-magnolia" % zioVersion % Test,
  "dev.zio"                      %% "zio-http"          % zioHttpVersion,
  "dev.zio"                      %% "zio-json"          % zioJsonVersion,
  "com.softwaremill.magnolia1_3" %% "magnolia"          % magnoliaVersion
)

scalacOptions ++= Seq(
  "--deprecation",
  "--feature",
  "-Wunused:all",
  "-Wshadow:all",
  "-Wvalue-discard",
  "-Xmax-inlines:256",
)

lazy val root = (project in file("."))
  .settings(
    name := "ccas",
  )
