# Dependency decisions — 2026-08-24

From the first full version review in a while. Records only the calls that needed weighing; the
routine bumps are legible from `project/Versions.scala` and aren't repeated here.

## zio-http, zio-json and zio-cli move as one

Not three bumps — one. zio-cli 0.8.2 requires zio-json 0.10.0, and zio-http 3.11.4 pulls it
transitively via zio-schema-json 1.8.6. Bumping any one alone fails in `update` with an early-semver
eviction error before anything compiles.

The workarounds are both traps: `evictionErrorLevel := Level.Warn` puts 0.10.0 on the classpath while
`vZioJson` still reads 0.9.2 — a lie in the pin file — and `libraryDependencySchemes` disables the
binary-compat check for *every* ZIO artifact to route around a conflict a routine bump dissolves.
Move all three together.

## sbt 1.13.0 rather than 1.12.15

1.12.15 fixes an RCE in the sbt server's JSON-RPC; 1.13.0 adds a second via BSP. Both need
`serverConnectionType = Tcp`, which appears nowhere here — but the log4j and Ivy CVEs in the
1.12.10–1.12.14 stream are unconditional, so there was no reason to stop short. It is also the floor
for any later Scala 3.9 move (3.9 went to JLine 4).

Two things to know. Run `clean` when changing the sbt version — a warm `target/` produced a one-off
stale-TASTy error. And the version used to be pinned twice: `project/build.properties` (which the
launcher reads) plus a `vSbt` mirror feeding `ThisBuild / sbtVersion`, with nothing keeping them in
agreement. The mirror is gone; `BuildInfo.sbtVersion` now reports whichever sbt actually ran, so the
two can no longer disagree.

## HikariCP 7.0.0 — taken despite being a major

The major is nominal. The public `HikariConfig` diff is four added methods and zero removals, minimum
JDK is unchanged at 11, and `HikariPool.createTimeoutException` is untouched — so `PostgresClient`'s
`SQLTransientConnectionException` fail-fast branch and its literal-message test still hold.

Watch item rather than blocker: #2265 added an unconditional `!Thread.interrupted()` in
`shouldContinueCreating()`. It only runs on `addConnectionExecutor`, which we never interrupt. If the
pool is ever seen sitting below `minimumIdle`, look there first.

## Excluding the unused AWS transports

`awssdk:s3` pulls `netty-nio-client` and `apache-client` transitively even though only
`url-connection-client` is used. netty-nio-client drags Netty **4.1** alongside the **4.2** zio-http
selects — and 4.2 split `netty-codec` into `netty-codec-base` + `netty-codec-compression`, so the 4.1
jar put **192 duplicate fully-qualified classes** on the classpath, including `ByteToMessageDecoder`
and every gzip decompressor the `Decompression.NonStrict` path depends on. Which copy loaded was
decided by jar ordering alone. 4.2 happened to win in both the runtime and staged classpaths, so this
was benign — but incidentally so, which is why it was worth fixing rather than noting.

Both transports are now excluded in `build.sbt`, and Netty is now uniformly 4.2.17. The jar saving is
minor and easy to overstate — 103 → 100 attributable to the exclusions themselves, measured at AWS
2.54.2; the 115 the tree started at was pre-exclusion *and* pre-bump. The version skew is the reason
to keep them, not the jar count.

Consequence to keep in mind: since SDK 2.41.11, if Apache5 is on the classpath and no transport is
configured, the SDK silently *prefers* it rather than throwing. That makes `S3BodyStore`'s explicit
`.httpClientBuilder` load-bearing in a way it wasn't before — dropping it would swap transport and
discard the `S3Timeouts` values. The exclusions restore the loud failure.

## Held

**Magnum 1.3.1.** Newest stable. Maven's `<release>` pointer and GitHub both mislabel `2.0.0-M3` as a
release (M2 and M3 carry `prerelease=false`), so checkers will report it as latest — it is a
milestone, and the 2.0 line has sat in milestone 19 months. Worth knowing what 2.0 would buy: it
deletes both documented SQL gotchas — the null-`DbCodec` guard (fixed in M1) and the bare-enum-case
empty-placeholder bug (fixed in M2). Migration is ~7 sites across 4 files, mostly one word each, plus
a small `PostgresClient` change since `Transactor` is no longer a case class. The risky part is the
new `readSingleOption`: a careless `Some(readSingle(...))` turns every NULL into `Some(null)` rather
than failing loudly.

**Scala 3.9** is still RC. Do not take 3.3.8 LTS instead — that is the JDK-8-era line and a two-year
step back.

**JDK 26** would hard-fail this stack: it flips `--sun-misc-unsafe-memory-access` from `warn` to
`deny`, and `scala.runtime.LazyVals$.getOffsetStatic` → `izumi.reflect` → `ZLayer` dies at class-init
before reaching `main()`. 25.0.4-tem is also the newest 25.x SDKMAN offers for darwinarm64. Related
latent gap: those JVM flags are added *only* by native-packager's `bashScriptExtraDefines`, so on
JDK 26 `sbt test`/`run` and IDE runs would break while the packaged launcher kept working.

**sbt 2.x** — see [sbt 2 evaluation](sbt-2-evaluation.md).

## Accepted without verification

Shipped on a green suite (1223 tests) without two checks that source reading can't substitute for:

- **One live Chess.com crawl.** zio-http 3.11.0 replaced sequential connect with Happy Eyeballs —
  IPv6-first, 250 ms stagger, raced parallel connects. api.chess.com returns 5 A + 5 AAAA, so the
  racing path engages on every pool miss and first preference flips to IPv6. For a client whose whole
  design is per-IP rate-limit management against Cloudflare, that wants watching:
  `client_stats.latency_*` and the 429 / CF-403 counters.
- **One real R2 round trip** — a >1 MiB put and read-back. `TestBodyStore` passes and the emitted
  request bytes were verified byte-equivalent across SDK versions, but R2's own behaviour was never
  exercised.
