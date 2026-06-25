package ccas.server.config

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

/** Tests the [[ServerEnvKeys]] registry: the essential bootstrap set, the secret flags that drive redaction, and the
  * `byName` / `redact` helpers. Pure, no IO.
  */
object TestServerEnvKeys extends ZIOSpecDefault {

  override def spec: Spec[Any, Any] = suite("TestServerEnvKeys")(
    test("essential keys include the contact email and the DB bootstrap set") {
      val names = ServerEnvKeys.essential.map(_.name).toSet
      assertTrue(names.contains("CCAS_CONTACT_EMAIL"), names.contains("DATABASE_URL"), names.contains("DB_PASSWORD"))
    },
    test("DATABASE_URL and DB_PASSWORD are secret; CCAS_CONTACT_EMAIL is not") {
      assertTrue(
        ServerEnvKeys.isSecret("DATABASE_URL"),
        ServerEnvKeys.isSecret("DB_PASSWORD"),
        !ServerEnvKeys.isSecret("CCAS_CONTACT_EMAIL")
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
