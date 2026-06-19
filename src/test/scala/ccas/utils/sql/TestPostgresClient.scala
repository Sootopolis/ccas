package ccas.utils.sql

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.HikariDataSource
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

object TestPostgresClient extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestPostgresClient")(
    suite("DB-backed")(
      testIsHikariDataSource,
      testPoolConfigApplied,
      testConnectionFunctional,
      testSchemaOverride,
      testPoolClosesOnScopeExit,
      testInvalidSchemaRejected,
      testLiveBuildsWithoutSchema
    ).provideShared(
      FreshSchemaLayer("test_dsl")
    ),
    testResolveSchema
  ) @@ TestAspect.sequential

  // Pure resolveSchema cases — no DB / PostgresClient service needed. Builds inline dataSource configs to cover the
  // #92 regression: an absent `currentSchema` key must yield None (no ConfigException), not crash.
  private def testResolveSchema = suite("resolveSchema")(
    test("present key resolves") {
      val cfg = ConfigFactory.parseString("currentSchema = public")
      assertTrue(PostgresClient.resolveSchema(None, cfg).contains("public"))
    },
    test("absent key resolves to None without throwing") {
      val cfg = ConfigFactory.parseString("user = ccas")
      assertTrue(PostgresClient.resolveSchema(None, cfg).isEmpty)
    },
    test("empty value resolves to None") {
      val cfg = ConfigFactory.parseString("currentSchema = \"\"")
      assertTrue(PostgresClient.resolveSchema(None, cfg).isEmpty)
    },
    test("whitespace value resolves to None") {
      val cfg = ConfigFactory.parseString("currentSchema = \"   \"")
      assertTrue(PostgresClient.resolveSchema(None, cfg).isEmpty)
    },
    test("explicit schema wins over absent key") {
      val cfg = ConfigFactory.parseString("user = ccas")
      assertTrue(PostgresClient.resolveSchema(Some("override"), cfg).contains("override"))
    },
    test("explicit schema wins over present key") {
      val cfg = ConfigFactory.parseString("currentSchema = public")
      assertTrue(PostgresClient.resolveSchema(Some("override"), cfg).contains("override"))
    }
  )

  private def testIsHikariDataSource = test("underlying DataSource is HikariDataSource") {
    for {
      pgClient <- ZIO.service[PostgresClient]
    } yield assertTrue(pgClient.transactor.dataSource.isInstanceOf[HikariDataSource])
  }

  private def testPoolConfigApplied = test("pool config is applied from application.conf") {
    for {
      pgClient <- ZIO.service[PostgresClient]
      hikariDs = pgClient.transactor.dataSource.asInstanceOf[HikariDataSource]
    } yield assertTrue(
      hikariDs.getMaximumPoolSize == 3,
      hikariDs.getMinimumIdle == 1,
      hikariDs.getConnectionTimeout == 30000L
    )
  }

  private def testConnectionFunctional = test("connections are functional") {
    for {
      pgClient <- ZIO.service[PostgresClient]
      result <- ZIO.scoped {
        for {
          conn <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(pgClient.transactor.dataSource.getConnection))
          stmt <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(conn.createStatement()))
          value <- ZIO.attemptBlocking {
            val rs = stmt.executeQuery("SELECT 1")
            rs.next()
            rs.getInt(1)
          }
        } yield value
      }
    } yield assertTrue(result == 1)
  }

  private def testSchemaOverride = test("FreshSchemaLayer creates schema") {
    for {
      pgClient <- ZIO.service[PostgresClient]
      exists <- ZIO.scoped {
        for {
          conn <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(pgClient.transactor.dataSource.getConnection))
          stmt <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(conn.createStatement()))
          found <- ZIO.attemptBlocking {
            val rs = stmt.executeQuery(
              "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'test_dsl'"
            )
            rs.next()
          }
        } yield found
      }
    } yield assertTrue(exists)
  }

  private def testPoolClosesOnScopeExit = test("pool closes on scope exit") {
    for {
      hikariDs <- ZIO.scoped {
        for {
          xa <- PostgresClient.live(schema = Some("test_dsl_close")).build
          ds = xa.get[PostgresClient].transactor.dataSource.asInstanceOf[HikariDataSource]
        } yield ds
      }
    } yield assertTrue(hikariDs.isClosed)
  }

  // #92 e2e: the prod DB_* path with DB_SCHEMA unset — `databaseNoSchema` has no `currentSchema` key — must build the
  // pool instead of throwing ConfigException$Missing. With no schema set, Hikari leaves getSchema null (PG falls back
  // to the connection's default search_path). Builds its own layer, so it doesn't use the shared FreshSchemaLayer.
  private def testLiveBuildsWithoutSchema = test("live builds pool when currentSchema is absent") {
    ZIO.scoped {
      for {
        xa <- PostgresClient.live(prefix = "databaseNoSchema").build
        hikariDs = xa.get[PostgresClient].transactor.dataSource.asInstanceOf[HikariDataSource]
      } yield assertTrue(hikariDs.getSchema == null)
    }
  }

  private def testInvalidSchemaRejected = test("invalid schema name is rejected") {
    for {
      result <- ZIO.scoped {
        FreshSchemaLayer("DROP TABLE").build
      }.either
    } yield assertTrue(
      result.left.exists(_.isInstanceOf[IllegalArgumentException])
    )
  }
}
