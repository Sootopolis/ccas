---
paths:
  - "src/main/scala/ccas/cli/**"
  - "src/test/scala/ccas/cli/**"
  - "completions/**"
---

**Shell completions.** `completions/ccas.bash` is committed and asserted byte-equal to `CompletionEmitter.bash` by `TestCcasCompletion`. Regenerate after any CLI tree change with `sbt --server -batch -error 'runMain ccas.cli.Main completion bash' > completions/ccas.bash`. `--server` is load-bearing: with `SBT_NATIVE_CLIENT=true`, `-batch` alone still routes through the thin client, which writes banner lines and a raw ESC into **stdout**, corrupting the redirect.

**zio-cli argument parsing.** zio-cli swallows an option written *after* a positional as another positional value, and `Args.atMost(n)` silently *truncates* extra values rather than rejecting them. Capture every positional with `.repeat` and validate arity by hand (see `UseClub`) wherever a dropped argument would change what the command does.
