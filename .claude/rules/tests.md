---
paths:
  - "src/test/**"
  - "build.sbt"
---

**Tests.** `Test/parallelExecution := false` is deliberate — several suites touch process-global state or a shared schema. Within a suite ZIO Test still runs tests in parallel, so a suite sharing mutable schema or filesystem state needs `@@ TestAspect.sequential`. Fix flakes at the root; `@@ TestAspect.flaky` is not an acceptable remedy. A test of a fiber's cleanup under interruption asserts `exit.isInterruptedOnly`: `isInterrupted` also passes when the cleanup crashed.
