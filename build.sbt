val vCcas  = "0.1.0-SNAPSHOT"
val vScala = "3.6.2"
val vSbt   = "1.10.7"

ThisBuild / version      := vCcas
ThisBuild / scalaVersion := vScala
ThisBuild / sbtVersion   := vSbt

// versions

val vZio           = "2.1.14"
val vZioConfig     = "4.0.3"
val vZioHttp       = "3.0.1"
val vZioJson       = "0.7.3"
val vZioProtoQuill = "4.8.6"
val vMagnolia      = "1.3.8"
val vSqliteJdbc    = "3.47.2.0"

// modules
libraryDependencies ++= Seq(
  "dev.zio"                      %% "zio"                 % vZio,
  "dev.zio"                      %% "zio-test"            % vZio % Test,
  "dev.zio"                      %% "zio-test-sbt"        % vZio % Test,
  "dev.zio"                      %% "zio-test-magnolia"   % vZio % Test,
  "dev.zio"                      %% "zio-config"          % vZioConfig,
  "dev.zio"                      %% "zio-config-magnolia" % vZioConfig,
  "dev.zio"                      %% "zio-config-typesafe" % vZioConfig,
  "dev.zio"                      %% "zio-http"            % vZioHttp,
  "dev.zio"                      %% "zio-json"            % vZioJson,
  "io.getquill"                  %% "quill-jdbc-zio"      % vZioProtoQuill,
  "com.softwaremill.magnolia1_3" %% "magnolia"            % vMagnolia,
  "org.xerial"                   %  "sqlite-jdbc"         % vSqliteJdbc,
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
