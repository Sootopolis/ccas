package ccas.utils.sql

import java.sql.SQLException

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

object TestIsTransient extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("PostgresClient.isTransient")(
    test("SQLState 08000 (connection_exception) is transient") {
      assertTrue(PostgresClient.isTransient(sqlEx("08000", "connection_exception")))
    },
    test("SQLState 08003 (connection_does_not_exist) is transient") {
      assertTrue(PostgresClient.isTransient(sqlEx("08003", "connection_does_not_exist")))
    },
    test("SQLState 08006 (connection_failure) is transient") {
      assertTrue(PostgresClient.isTransient(sqlEx("08006", "connection_failure")))
    },
    test("message containing 'terminating connection' is transient") {
      assertTrue(PostgresClient.isTransient(sqlEx(null, "terminating connection due to administrator command")))
    },
    test("message containing 'Connection is closed' is transient") {
      assertTrue(PostgresClient.isTransient(sqlEx(null, "Connection is closed")))
    },
    test("message containing 'This connection has been closed' is transient") {
      assertTrue(PostgresClient.isTransient(sqlEx(null, "This connection has been closed")))
    },
    test("SQLState 23505 (unique_violation) is not transient") {
      assertTrue(!PostgresClient.isTransient(sqlEx("23505", "duplicate key value violates unique constraint")))
    },
    test("SQLState 42P01 (undefined_table) is not transient") {
      assertTrue(!PostgresClient.isTransient(sqlEx("42P01", "relation does not exist")))
    },
    test("generic RuntimeException is not transient") {
      assertTrue(!PostgresClient.isTransient(new RuntimeException("something went wrong")))
    },
    test("SQLException with null state and unrelated message is not transient") {
      assertTrue(!PostgresClient.isTransient(sqlEx(null, "syntax error at position 42")))
    },
    test("SQLException with null state and null message is not transient") {
      assertTrue(!PostgresClient.isTransient(new SQLException()))
    }
  )

  private def sqlEx(state: String, message: String): SQLException =
    new SQLException(message, state)
}
