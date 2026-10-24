package ccas.server.config

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ServerEnvKey.Domain.*

/** Tests the [[ServerEnvKeys]] registry: the per-key display domains, the secret keys that drive redaction, and the
  * `byName` / `redact` / `grouped` helpers. Pure, no IO.
  */
object TestServerEnvKeys extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestServerEnvKeys")(
    test("domain maps each key, including the non-DB_-prefixed cases") {
      assertTrue(
        ServerEnvKeys.byName("CCAS_CONTACT_EMAIL").map(_.domain) == Some(Contact),
        ServerEnvKeys.byName("DATABASE_URL").map(_.domain) == Some(Database),
        ServerEnvKeys.byName("SERVER_PORT").map(_.domain) == Some(Server),
        ServerEnvKeys.byName("JOB_LOGS_DIR").map(_.domain) == Some(Server),
        ServerEnvKeys.byName("CCAS_OUTPUT_DIR").map(_.domain) == Some(Server),
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
    test("exactly DATABASE_URL, DB_PASSWORD and the two R2 credentials are secret") {
      assertTrue(
        ServerEnvKeys.all.filter(key => ServerEnvKeys.isSecret(key.name)).map(_.name).toSet ==
          Set("DATABASE_URL", "DB_PASSWORD", "CCAS_R2_ACCESS_KEY", "CCAS_R2_SECRET_KEY")
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
