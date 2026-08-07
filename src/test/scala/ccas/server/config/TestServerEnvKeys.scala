package ccas.server.config

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ServerEnvKey.Domain.*

/** Tests the [[ServerEnvKeys]] registry: the essential bootstrap set, the per-key display domains, the secret flags
  * that drive redaction, and the `byName` / `redact` / `grouped` helpers. Pure, no IO.
  */
object TestServerEnvKeys extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestServerEnvKeys")(
    test("essential is exactly the contact email plus the 8-key DB bootstrap set") {
      // `essential` is otherwise only read by this test — `init` and `Main.missingServeEnv` hardcode the same names —
      // so pin the full set: a mis-flagged `essential =` boolean (extra or dropped key) would otherwise go uncaught.
      assertTrue(
        ServerEnvKeys.essential.map(_.name).toSet ==
          Set("CCAS_CONTACT_EMAIL", "DATABASE_URL", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SCHEMA")
      )
    },
    test("domain maps each key, including the non-DB_-prefixed cases") {
      assertTrue(
        ServerEnvKeys.byName("CCAS_CONTACT_EMAIL").map(_.domain) == Some(Contact),
        ServerEnvKeys.byName("DATABASE_URL").map(_.domain) == Some(Database),
        ServerEnvKeys.byName("SERVER_PORT").map(_.domain) == Some(Server),
        ServerEnvKeys.byName("JOB_LOGS_DIR").map(_.domain) == Some(Server),
        ServerEnvKeys.byName("SCHEDULER_POLL_MINUTES").map(_.domain) == Some(Scheduler),
        ServerEnvKeys.byName("CHESS_COM_API_COOLDOWN_SECONDS").map(_.domain) == Some(ChessComClient),
        ServerEnvKeys.byName("CCAS_R2_ENDPOINT").map(_.domain) == Some(BodyStore)
      )
    },
    test("grouped lists domains in enum order, registry order within a domain") {
      assertTrue(
        ServerEnvKeys.grouped.map(_._1) == List(Contact, Database, Server, Scheduler, ChessComClient, BodyStore),
        ServerEnvKeys.grouped.find(_._1 == Database).map(_._2.head.name) == Some("DATABASE_URL")
      )
    },
    test("DATABASE_URL and DB_PASSWORD are secret; CCAS_CONTACT_EMAIL is not") {
      assertTrue(
        ServerEnvKeys.isSecret("DATABASE_URL"),
        ServerEnvKeys.isSecret("DB_PASSWORD"),
        !ServerEnvKeys.isSecret("CCAS_CONTACT_EMAIL")
      )
    },
    test("R2 credential keys are secret (redacted); the R2 endpoint/bucket are not") {
      assertTrue(
        ServerEnvKeys.isSecret("CCAS_R2_SECRET_KEY"),
        ServerEnvKeys.isSecret("CCAS_R2_ACCESS_KEY"),
        !ServerEnvKeys.isSecret("CCAS_R2_ENDPOINT"),
        !ServerEnvKeys.isSecret("CCAS_R2_BUCKET")
      )
    },
    test("redact masks secret values, passes non-secrets and blanks through") {
      assertTrue(
        ServerEnvKeys.redact("DB_PASSWORD", "hunter2") == "****",
        ServerEnvKeys.redact("DB_HOST", "localhost") == "localhost",
        ServerEnvKeys.redact("DB_PASSWORD", "") == "",
        ServerEnvKeys.redact("UNKNOWN_KEY", "v") == "v"
      )
    },
    test("byName resolves a known key and rejects an unknown one") {
      assertTrue(ServerEnvKeys.byName("SERVER_PORT").exists(_.name == "SERVER_PORT"), ServerEnvKeys.byName("NOPE").isEmpty)
    }
  )
}
