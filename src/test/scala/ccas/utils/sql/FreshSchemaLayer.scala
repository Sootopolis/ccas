package ccas.utils.sql

import java.nio.file.Paths

import zio.{RIO, TaskLayer, ULayer, ZIO, ZLayer}

import ccas.utils.client.{BodyStore, FsBodyStore}

object FreshSchemaLayer {
  private val schemaNamePattern = "^[a-z_][a-z0-9_]*$".r

  private[sql] def isTestDatabase(catalog: String): Boolean = catalog.endsWith("_test")

  // Names the database but never the URL: in the `url` config branch the JDBC URL is the one thing that may have
  // carried credentials.
  private def refusal(schema: String, catalog: String): String =
    s"Refusing to reset schema '$schema': connected to database '$catalog', which is not a test database. " +
      "Is DATABASE_URL exported in this shell?"

  /** Fresh-schema test layer. Outputs `PostgresClient & BodyStore` so specs can both drive the DB and exercise the
    * body store (which since #191 backs `api_response_body`). The store is a local-filesystem [[FsBodyStore]] rooted
    * under the suite's schema name — per-suite isolation mirroring the DB schema reset, so content-addressed objects
    * written by one suite can't be pruned out from under another (a global root would couple suites through shared
    * SHA-256 keys). The same store backs both the `onInit` hook (which runs with only `PostgresClient` in scope, so
    * `Tables.ensureTables`'s `BodyStore` requirement is self-provided) and the layer output.
    */
  def apply(
    schema: String,
    onInit: RIO[PostgresClient & BodyStore, Unit] = ZIO.unit
  ): TaskLayer[PostgresClient & BodyStore] = {
    val bodyStore: ULayer[BodyStore] =
      ZLayer.succeed[BodyStore](new FsBodyStore(Paths.get("target", "test-bodies", schema)))
    val resetSchema: RIO[PostgresClient, Unit] = ZIO.serviceWithZIO[PostgresClient] { pgClient =>
      ZIO.attempt(require(schemaNamePattern.matches(schema), s"Invalid schema name: $schema")) *>
        ZIO.scoped {
          for {
            conn    <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(pgClient.transactor.dataSource.getConnection))
            catalog <- ZIO.attemptBlocking(conn.getCatalog)
            // Refuse to DROP SCHEMA anywhere but a test database. The test config nulls `database.url` so an exported
            // DATABASE_URL cannot redirect the suite; this is the second line, checked against the live connection so
            // that no other config route, present or future, gets around it.
            _    <- ZIO.attempt(require(isTestDatabase(catalog), refusal(schema, catalog)))
            stmt <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(conn.createStatement()))
            _ <- ZIO.attemptBlocking {
              stmt.execute(s"DROP SCHEMA IF EXISTS $schema CASCADE"): Unit
              stmt.execute(s"CREATE SCHEMA $schema"): Unit
            }
          } yield ()
        }
    }
    PostgresClient.live(
      schema = Some(schema),
      onInit = (resetSchema *> onInit).provideSomeLayer[PostgresClient](bodyStore)
    ) ++ bodyStore
  }
}
