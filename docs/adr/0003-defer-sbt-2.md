# sbt 2 — evaluated and deferred (2026-08-24)

**Status:** Deferred, 2026-08-25. Revisit on the triggers at the end.

**Decision: stay on sbt 1.13.0.** Not blocked — the payoff doesn't cover the cost. Revisit on the
triggers at the end.

## Problem

sbt 2.0.0 shipped 2026-06-14 (2.0.7 current). CCAS is a single-module, Scala-3-only, JVM-only build
with three plugins and no custom tasks — close to the easiest possible migration. Worth asking
whether to take it.

## What sbt 2 actually buys us

| | sbt 1.13 | sbt 2.0.7 |
|---|---|---|
| `clean; compile`, nothing changed | ~36 s | ~2 s (machine-global CAS restore) |
| compile in a second worktree, identical sources | ~36 s | ~12 s |
| CI cross-run caching | none | free via `sbt/setup-sbt@v1` |
| full test phase | ~26 s | ~25 s |
| cold compile, empty cache | ~33 s | ~41 s |

Everything else sbt 2 adds — `projectMatrix`, platform-aware `%%`, `exportJars := true`, the
`target/out/jvm/scala-<v>/<proj>/` layout, common-settings-replacing-`ThisBuild`, the Scala 3
metabuild — targets multi-module or cross-building. One module, one Scala version, JVM-only: those
are migration cost here, not benefit.

Three things that sound like sbt 2 features and aren't:

- **Fast startup** is sbtn, and sbt 1.13 already ships it. Measured here: `sbt -batch` warm 2.41 s,
  `sbt --client` warm 0.12 s. The published "41% faster" figure is against sbt 1.10.2.
- **Remote caching** existed in sbt 1.4+ over a plain Maven repo. sbt 2 replaced it with a
  Bazel-protocol gRPC CAS needing bazel-remote/BuildBuddy — less accessible, not more.
- **Zinc incremental compile, Coursier, BSP** are all sbt 1 features.

The headline "5×–20×" is scoped by sbt's own docs to builds taking 10+ minutes to test. Ours takes 26 s.

## Why not, concretely

**The one feature with real payoff has to be switched off.** sbt 2 renames the test tasks: `test`
becomes sbt 1's `testQuick`, and `testFull` is the old `test`. Verified on a scratch copy that had
*never run its tests*:

```
$ sbt test
[info] No tests to run for Test / testQuick
[success] elapsed time: 1 s, cache 100%, 30 disk cache hits
```

Zero tests, exit 0, inheriting "already passed" from another directory via the machine-global cache.
`.githooks/pre-push` and `.github/workflows/ci.yml` both run `sbt test`, so the push gate would go
green having executed nothing — silently, with nothing to notice.

`testFull` is the fix and it removes the benefit. We could not keep incremental testing regardless:
56 of 94 suites hit live Postgres, and `createTable` is the schema of record applied by hand via
psql, so the cache key sees code only and would report "passed" across a schema change it cannot see.

**Everything else is cheap.** All three plugins cross-publish `_sbt2_3` at the versions already
pinned, so `plugins.sbt` needs no edit. sbt-buildinfo 0.13.1 generates correctly under 2.0.7 despite
being built against 2.0.0-RC2 — the RC3 ecosystem cutoff governs publication targeting, not runtime
loading. `testFull` ran 1223/1223 in 25 s, four consecutive times in one warm server, with no
`Test / fork`. Remaining cost is `project/metals.sbt` (pins sbt-bloop 2.0.9; the sbt2 line starts at
2.0.18 — now untracked, see below), the two `sbt test` → `testFull` call sites, the stage paths in
`scripts/install-cli.sh` and README, and ~10 cosmetic `lintUnused` warnings from native-packager.

**No maintenance pressure to move.** sbt 1.13.0 shipped a *new feature* (Scala 3.9 REPL support)
65 minutes before 2.0.7, and both received the same GHSA patch the same day. Plugins are
cross-building, not stranding sbt 1.

## Done now instead (sbt-1-safe, keeps the migration available)

1. **`ci.yml` test-report glob** → `'**/test-reports/TEST-*.xml'`. The `target/`-anchored form works
   today (94 matches) but yields 0 under sbt 2's layout; the unanchored form matches under both. The
   reporter runs `if: always()`, so a non-matching glob reports an empty run rather than failing.
2. **`project/metals.sbt` untracked and gitignored.** Metals-generated, marked `DO NOT EDIT`, and it
   pinned its own ageing sbt-bloop independently of anything the build declares.
3. **Completion recipe now uses `sbt --server`.** `-batch` alone does *not* suppress the thin client:
   with `SBT_NATIVE_CLIENT=true`, `sbt -batch -error ...` writes `[info] entering thin client - BEEP
   WHIRR`, a `[success] elapsed time` line and a raw ESC `\u001b[0J` into **stdout**, corrupting the
   redirect into `completions/ccas.bash` and failing `TestCcasCompletion` on a control byte the repo
   forbids. `--server` emits the payload alone either way (verified byte-identical to the committed
   file with the thin client both on and off); `--no-server` does not.

`sbt --client` is worth using interactively — 2.41 s → 0.12 s per invocation — now that the recipe
is protected against it.

## Triggers to revisit

- **sbt/sbt#9321** lands near-match Analysis restore. The cache is exact-match today, so every real
  commit is a full miss; this is the blocker on both the CI and cross-worktree cases.
- **sbt-native-packager publishes an sbt2 build against a GA baseline.** Currently RC6, `main` still
  declares `scriptedSbt := "2.0.0-RC6"`, and it is the plugin that ships `bin/ccas`.
- **sbt/sbt#9587** gives the `test`/`testFull` split a proper opt-out, so a non-hermetic suite stops
  depending on remembering one word.
- **A second module, cross-build, or Scala.js target appears** — then `projectMatrix`, `exportJars`,
  the target layout and common settings all start paying at once.

## Not verified

- One trial reported 32 deterministic `No suitable driver` failures under sbt 2 (pgjdbc's global
  `DriverManager` registration vs. `closeClassLoaders`, default true since 2.0.5). It did not
  reproduce across four consecutive `testFull` runs here. Treat as unconfirmed, not as a known
  blocker — but re-check before any migration, since the failure mode is plausible.
- Whether plugins built against 2.0.0-RC2 / RC6 keep working past 2.0.7. sbt's MiMa forward-compat
  baseline on `develop` is `Seq("2.0.4")`, so nothing upstream re-verifies them. They work on 2.0.7;
  each patch bump re-rolls that.
