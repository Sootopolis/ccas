ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.5.2"

libraryDependencies += "dev.zio" %% "zio" % "2.1.13"
libraryDependencies += "dev.zio" %% "zio-http" % "3.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "ccas"
  )
