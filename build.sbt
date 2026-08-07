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
  "dev.zio" %% "zio-cli" % vZioCli,

  // cron4s — wall-clock CRON schedule triggers (6-field; java.time next/prev via cron4s.lib.javatime)
  "com.github.alonsodomin.cron4s" %% "cron4s-core" % vCron4s,

  // AWS S3 SDK v2 — response-body blob store off metered Postgres (BodyStore → Cloudflare R2, #191). Sync client
  // wrapped in attemptBlockingInterrupt (matches the JDBC idiom); url-connection-client is the JDK-HttpURLConnection
  // transport, avoiding the heavier netty-nio async client. Endpoint override points the S3 API at R2/B2/MinIO.
  "software.amazon.awssdk" % "s3"                    % vAwsSdk,
  "software.amazon.awssdk" % "url-connection-client" % vAwsSdk,

  // No-op SLF4J binding: we log via ZIO, not SLF4J. Without a binding, transitive SLF4J users (netty, etc.) print
  // "No SLF4J providers were found" on every CLI invocation; the NOP binding silences that library noise.
  "org.slf4j" % "slf4j-nop" % vSlf4j
)

scalacOptions ++= Seq(
  "--deprecation",
  "--feature",
  "-Wunused:all",
  "-Wshadow:all",
  "-Wvalue-discard",
  "-Xmax-inlines:64"
)

// Suites run one-at-a-time. The test suite shares three process-global resources that concurrent suites race on:
// the OS process table / memory (the `<shell> -n` completion checks spawn subprocesses that fail to fork under
// parallel-suite memory pressure), the JVM's `System.out` (TestProgressBar swaps it process-wide to capture output),
// and — historically — DB rows (now isolated per-suite via FreshSchemaLayer). Disabling cross-suite parallelism
// removes the contention at its root rather than masking it with `@@ TestAspect.flaky` retries. Cost is small: the
// suite is DB-I/O-bound, so serialising adds ~11s to the test phase (~17s -> ~28s), not a multiple. Tracked: #65.
Test / parallelExecution := false

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
    // native-packager's stage pulls packagedArtifacts, which builds the javadoc + sources jars
    // only for universalDepMappings to discard them — and scaladoc now fails outright reading
    // cron4s-core's TASTy (its JVM jar bakes in scalajs-stubs annotations like JSExportTopLevel
    // that don't resolve on a JVM classpath). Nothing consumes these jars; skip them.
    Compile / packageDoc / publishArtifact := false,
    Compile / packageSrc / publishArtifact := false,
    buildInfoKeys                   := Seq(name, version, scalaVersion, sbtVersion),
    buildInfoPackage                := "ccas.info",
    // Silence the sun.misc.Unsafe deprecation warning (scala-library's LazyVals) that the JVM
    // prints on JDK 24+. The `--sun-misc-unsafe-memory-access` flag only exists on JDK 23+, so
    // probe the runtime version in the launcher and add it conditionally (older JDKs don't warn).
    bashScriptExtraDefines ++= Seq(
      // Grant native access to unnamed-module code (netty's loadLibrary) so the JVM doesn't print "restricted method"
      // warnings on JDK 24+. Valid since JDK 16 and harmless on older JDKs, so add it unconditionally.
      "addJava \"--enable-native-access=ALL-UNNAMED\"",
      """java_major=$("${java_cmd:-java}" -version 2>&1 | head -n1 | sed -E 's/.*version "?([0-9]+).*/\1/')""",
      """if [ "${java_major:-0}" -ge 23 ] 2>/dev/null; then addJava "--sun-misc-unsafe-memory-access=allow"; fi"""
    )
  )
