# Every JVM this project starts carries the same two flags

**Status:** Accepted, 2026-09-07 (#231).

## Context

Two flags are not optional for this stack. `--sun-misc-unsafe-memory-access=allow` covers scala-library's `LazyVals`, which reads `sun.misc.Unsafe`; `--enable-native-access=ALL-UNNAMED` covers netty's `loadLibrary`. JDK 24 warns on the first, and JDK 26 denies it — the denial lands in class-init (`LazyVals` → `izumi.reflect` → `ZLayer`), so a JVM without the flag cannot reach `main()`.

Both were added only by native-packager's `bashScriptExtraDefines`, which reaches exactly one of the ways this project starts a JVM:

1. the **packaged launcher** — `bin/ccas`, `bin/ccas-server`;
2. whatever the **`sbt` script** starts — `sbt test`, `run`, `console`, and an IDE's sbt shell;
3. a **direct `java` invocation**: `.bsp/sbt.json` launches the BSP server as `java -cp sbt-launch.jar … xsbt.boot.Boot -bsp`, which is how Metals and IntelliJ-over-BSP run the suite. Nothing set `fork`, so that JVM was also the one the tests ran in.

On JDK 26 the packaged launcher would therefore keep working while the whole build died — "the build is broken but the app is fine", which is a confusing place to start debugging.

The gap was partly masked locally. Recent sbt launcher scripts add both flags themselves when the runtime is JDK 25 or newer (`addJdkWorkaround`, defeated by `--no-hide-jdk-warnings`) — present in 1.11.7 and 2.0.8, absent from 1.11.5 and 1.10.11. So a terminal `sbt test` was covered by which launcher Coursier happened to install, on a JDK new enough to trip that gate: not by anything this repository controls, and nothing at all on JDK 24, which warns.

## Decision

**One home per launch path, and the flags are identical in all three.**

| Path | Home | Conditional? |
| --- | --- | --- |
| Packaged launcher | `bashScriptExtraDefines` in `build.sbt` | yes — probes the runtime version |
| Anything the `sbt` script starts | `.jvmopts` | no |
| The test JVM, whatever started its parent | `Test / fork` + `Test / javaOptions` | no |

**The test JVM is forked** because it is the only way to reach path 3: `.bsp/sbt.json` is generated, so it is not a file to edit, and it reads neither `.jvmopts` nor the launcher's own workaround. `javaOptions` without `fork` is not an alternative: sbt drops it with `[warn] javaOptions will be ignored, fork is set to false`.

**Only the launcher probes the JDK.** `--sun-misc-unsafe-memory-access` does not exist before JDK 23, and an unrecognised flag aborts startup, so the other two homes take a documented floor instead: JDK 23+, met by the pin in `.sdkmanrc` and by `java-version` in `.github/workflows/ci.yml`. A packaged binary can be run on a JDK neither of those pins, which is why it keeps the probe.

**`run` stays unforked.** `ConfigCommand`, `RecruitmentApp` and `Dispatcher` prompt on stdin, which a fork drops unless `run / connectInput` is set. Its flags come from `.jvmopts`.

## Consequences

- The forked test JVM does not inherit sbt's `-Xss4M` / `-Xmx1024m`. The suite does not need them: 1250 tests pass in ~27 s, against the ~28 s recorded for the in-process run.
- Forking also ended the pgjdbc deregistration the README used to document under troubleshooting (`Failed to get driver instance for ...ccas_test` **or `No suitable driver`**, from a long-lived, repeatedly-reloaded sbt JVM) — a fresh JVM per run cannot accumulate it, so that entry is gone. Not to be confused with the same driver-level message raised by a libpq-form URL, which is [0014](0014-accept-both-database-url-forms.md) and still live.
- The forked JVM inherits the environment, so CI's step-level variables still reach tests and an exported `DATABASE_URL` still redirects the whole suite, exactly as before. sbt still writes `target/test-reports/TEST-*.xml`, so the CI report step is unaffected.
- Bloop picks the flags up only after a `bloopInstall` re-export, since `platform.jvm.options` comes from that export and `.bloop/` is gitignored.
- A bare IDE *Application* run configuration launches a main class through none of the three homes and still gets nothing. That is a per-user setting with no repository-side fix.
- Adding or changing a flag is now three edits. `TestJvmFlags` pins two of the three: the flags the test JVM was started with, and the contents of `.jvmopts`.
- Below JDK 23 every path except the packaged launcher fails at JVM startup rather than at runtime. That is the intended trade for not duplicating the version probe in two more places.
