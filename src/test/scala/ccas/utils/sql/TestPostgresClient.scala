package ccas.utils.sql

import com.zaxxer.hikari.HikariDataSource
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

object TestPostgresClient extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestPostgresClient")(
    testIsHikariDataSource,
    testPoolConfigApplied,
    testConnectionFunctional,
    testSchemaOverride,
    testPoolClosesOnScopeExit,
    testInvalidSchemaRejected
  ).provideShared(
    FreshSchemaLayer("test_dsl")
  ) @@ TestAspect.sequential

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
      result <- ZIO.attemptBlocking {
        val conn = pgClient.transactor.dataSource.getConnection
        try {
          val stmt = conn.createStatement()
          val rs   = stmt.executeQuery("SELECT 1")
          rs.next()
          val value = rs.getInt(1)
          rs.close()
          stmt.close()
          value
        } finally conn.close()
      }
    } yield assertTrue(result == 1)
  }

  private def testSchemaOverride = test("FreshSchemaLayer creates schema") {
    for {
      pgClient <- ZIO.service[PostgresClient]
      exists <- ZIO.attemptBlocking {
        val conn = pgClient.transactor.dataSource.getConnection
        try {
          val stmt = conn.createStatement()
          val rs = stmt.executeQuery(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'test_dsl'"
          )
          val found = rs.next()
          rs.close()
          stmt.close()
          found
        } finally conn.close()
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
