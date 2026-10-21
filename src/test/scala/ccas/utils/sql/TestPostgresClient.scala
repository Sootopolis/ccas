package ccas.utils.sql

import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
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
      testLiveBuildsWithoutSchema,
      testConfiguredAddressIsLiveAddress
    ).provideShared(
      FreshSchemaLayer("test_dsl")
    ),
    testResolveSchema,
    testNormalizeJdbcUrl,
    testDatabaseAddress,
    testExportedDatabaseUrlIgnored
  ) @@ TestAspect.sequential

  // A shell that exports DATABASE_URL — the `wip` worktree's direnv does — must not redirect the suite. Merges the
  // classpath `application.conf` files the way `ConfigFactory.load()` does, with a stand-in for the exported variable.
  private def testExportedDatabaseUrlIgnored = test("an exported DATABASE_URL does not reach the test config") {
    val exported = ConfigFactory.parseString("""DATABASE_URL = "jdbc:postgresql://elsewhere:5432/production"""")
    val resolved = ConfigFactory
      .parseResources("application.conf")
      .withFallback(exported)
      .resolve(ConfigResolveOptions.defaults().setUseSystemEnvironment(false))
    assertTrue(!resolved.hasPath("database.url"), resolved.getString("database.dataSource.databaseName") == "ccas_test")
  }

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

  // Pure normalizeJdbcUrl cases — no DB needed. The libpq URI is what every managed provider hands out and pgjdbc
  // rejects it outright (`acceptsURL` is false for the scheme, and userinfo credentials parse to null even with a
  // `jdbc:` prefix), so the conversion is the difference between a working paste and Hikari's opaque
  // "Failed to get driver instance".
  private def testNormalizeJdbcUrl = suite("normalizeJdbcUrl")(
    test("libpq URI converts, lifting userinfo credentials out of the URL") {
      val resolved =
        PostgresClient.normalizeJdbcUrl("postgresql://ccas_owner:s3cret@ep-x.neon.tech/ccas?sslmode=require")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:postgresql://ep-x.neon.tech/ccas?sslmode=require"),
        resolved.flatMap(_.user.toRight("")).contains("ccas_owner"),
        resolved.flatMap(_.password.toRight("")).contains("s3cret")
      )
    },
    test("postgres:// scheme is accepted as well as postgresql://") {
      val resolved = PostgresClient.normalizeJdbcUrl("postgres://u:p@host:5433/db")
      assertTrue(resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host:5433/db"))
    },
    test("libpq-only parameters are dropped, others preserved in order") {
      val resolved = PostgresClient.normalizeJdbcUrl(
        "postgresql://u:p@host/db?sslmode=require&channel_binding=require&ApplicationName=ccas"
      )
      assertTrue(resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host/db?sslmode=require&ApplicationName=ccas"))
    },
    test("query credentials are lifted out of an existing JDBC URL") {
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql://host/db?user=u&password=p&sslmode=require")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host/db?sslmode=require"),
        resolved.flatMap(_.user.toRight("")).contains("u"),
        resolved.flatMap(_.password.toRight("")).contains("p")
      )
    },
    test("a JDBC URL with no query survives unchanged") {
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql://host:5432/db")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host:5432/db"),
        resolved.map(_.user).contains(None),
        resolved.map(_.password).contains(None)
      )
    },
    test("userinfo credentials in a jdbc: URL are lifted rather than mis-parsed as a port") {
      // pgjdbc's own parseURL returns null for this shape ("invalid port number: p@host"), so accepting the
      // half-converted paste is strictly better than passing it through.
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql://u:p@host/db")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host/db"),
        resolved.flatMap(_.user.toRight("")).contains("u")
      )
    },
    test("an explicit query credential wins over userinfo") {
      val resolved = PostgresClient.normalizeJdbcUrl("postgresql://ignored:ignored@host/db?user=real&password=realpw")
      assertTrue(
        resolved.flatMap(_.user.toRight("")).contains("real"),
        resolved.flatMap(_.password.toRight("")).contains("realpw")
      )
    },
    test("userinfo percent-escapes decode, and '+' stays a literal plus") {
      val resolved = PostgresClient.normalizeJdbcUrl("postgresql://u:p%40ss%2Fw+d@host/db")
      assertTrue(resolved.flatMap(_.password.toRight("")).contains("p@ss/w+d"))
    },
    test("userinfo decodes multi-byte UTF-8 escapes as one character") {
      val resolved = PostgresClient.normalizeJdbcUrl("postgresql://u:caf%C3%A9@host/db")
      assertTrue(resolved.flatMap(_.password.toRight("")).contains("café"))
    },
    test("query credentials decode with pgjdbc's rules, where '+' is a space") {
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql://host/db?user=u&password=a+b%40c")
      assertTrue(resolved.flatMap(_.password.toRight("")).contains("a b@c"))
    },
    test("an unescaped '@' in the password splits at the last one") {
      val resolved = PostgresClient.normalizeJdbcUrl("postgresql://u:p@ss@host/db")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host/db"),
        resolved.flatMap(_.password.toRight("")).contains("p@ss")
      )
    },
    test("an '@' inside a query value does not confuse the host split") {
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql://host/db?user=u&password=a@b")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host/db"),
        resolved.flatMap(_.password.toRight("")).contains("a@b")
      )
    },
    test("a non-Postgres jdbc: URL passes through untouched") {
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:h2:mem:test")
      assertTrue(
        resolved.map(_.jdbcUrl).contains("jdbc:h2:mem:test"),
        resolved.map(_.user).contains(None)
      )
    },
    test("a blank URL is rejected with the expected shapes named") {
      val err = PostgresClient.normalizeJdbcUrl("   ").left.toOption.getOrElse("")
      assertTrue(err.contains("blank"), err.contains("jdbc:postgresql://"), err.contains("postgresql://"))
    },
    test("an unrecognised scheme is rejected without echoing the URL") {
      val err = PostgresClient.normalizeJdbcUrl("mysql://u:hunter2@host/db").left.toOption.getOrElse("")
      assertTrue(err.contains("unrecognised scheme"), !err.contains("hunter2"))
    },
    test("a host-less libpq URI is rejected, naming the local-socket case") {
      val err = PostgresClient.normalizeJdbcUrl("postgresql:///db").left.toOption.getOrElse("")
      assertTrue(err.contains("no host"), err.contains("local-socket"))
    },
    test("a host-less JDBC URL passes through, since pgjdbc accepts it") {
      // pgjdbc parses `jdbc:postgresql:///ccas` (PGHOST empty, port and database from its defaults), so rejecting it
      // would break an existing working configuration that this change never needed to touch.
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql:///ccas?user=u")
      assertTrue(resolved.map(_.jdbcUrl).contains("jdbc:postgresql:///ccas?user=u"))
    },
    test("a repeated credential parameter takes the last, as pgjdbc does") {
      val resolved = PostgresClient.normalizeJdbcUrl("jdbc:postgresql://host/db?user=first&user=second")
      assertTrue(resolved.flatMap(_.user.toRight("")).contains("second"))
    },
    test("scheme matching is case-insensitive") {
      val resolved = PostgresClient.normalizeJdbcUrl("PostgreSQL://u:p@Host/db?sslmode=require")
      assertTrue(resolved.map(_.jdbcUrl).contains("jdbc:postgresql://Host/db?sslmode=require"))
    },
    test("further libpq-only parameters are dropped") {
      val resolved = PostgresClient.normalizeJdbcUrl(
        "postgresql://u:p@host/db?connect_timeout=10&target_session_attrs=read-write&sslmode=require"
      )
      assertTrue(resolved.map(_.jdbcUrl).contains("jdbc:postgresql://host/db?sslmode=require"))
    }
  )

  // Through `normalizeJdbcUrl` first, as `live` does: the address is read from the URL Hikari is handed, never the raw
  // config value. A rendered address is printed and logged, so no credential may survive into it.
  private def addressOf(raw: String): Option[DatabaseAddress] =
    PostgresClient.normalizeJdbcUrl(raw).toOption.flatMap(resolved => PostgresClient.databaseAddress(resolved.jdbcUrl))

  private def testDatabaseAddress = suite("databaseAddress")(
    test("a libpq URI's address is host, port and database, without its userinfo credentials") {
      val rendered = addressOf("postgresql://alice:hunter2@ep-x.neon.tech:5432/neondb?sslmode=require").map(_.render)
      assertTrue(
        rendered.contains("ep-x.neon.tech:5432/neondb"),
        !rendered.exists(_.contains("alice")),
        !rendered.exists(_.contains("hunter2"))
      )
    },
    test("a JDBC URL's address carries none of its query parameters, credentials included") {
      val rendered =
        addressOf("jdbc:postgresql://localhost:5432/ccas?user=alice&password=hunter2&sslmode=disable").map(_.render)
      assertTrue(rendered.contains("localhost:5432/ccas"))
    },
    test("the address shows a port only where the URL gives one") {
      assertTrue(addressOf("postgresql://u:p@ep-x.neon.tech/neondb").map(_.render).contains("ep-x.neon.tech/neondb"))
    },
    test("a host-less JDBC URL has no address, even with credentials in its userinfo") {
      // The one form `normalizeJdbcUrl` passes through untouched, so its userinfo is still in the URL Hikari gets. A
      // password of `12/34` would otherwise read as host `alice`, port 12.
      assertTrue(
        addressOf("jdbc:postgresql:///ccas").isEmpty,
        addressOf("jdbc:postgresql://alice:hunter2@/ccas").isEmpty,
        addressOf("jdbc:postgresql://alice:12/34@/ccas").isEmpty
      )
    },
    test("a non-Postgres JDBC URL has no address") {
      assertTrue(addressOf("jdbc:h2:mem:test").isEmpty, addressOf("jdbc:mysql://host:3306/db").isEmpty)
    },
    test("a URL whose host part is not a host has no address, rather than a fragment of a password") {
      // An unescaped '?' in a password ends the authority early, leaving `alice:hun` where the host should be.
      assertTrue(addressOf("postgresql://alice:hun?ter2@host/db").isEmpty)
    },
    test("localhost, 127.x and ::1 are loopback; a remote or multi-host address is not") {
      def loopback(hostPort: String) = DatabaseAddress(hostPort, "/ccas").isLoopback
      assertTrue(
        loopback("localhost:5432"),
        loopback("LOCALHOST"),
        loopback("127.0.0.1"),
        loopback("127.1.2.3:5433"),
        loopback("[::1]:5432"),
        loopback("[0:0:0:0:0:0:0:1]"),
        !loopback("ep-x.neon.tech"),
        !loopback("127.example.com"),
        !loopback("192.168.1.10:5432"),
        !loopback("localhost:5432,ep-x.neon.tech:5432")
      )
    },
    // `server down` decides whether to ask from what the server reports, so the two must agree on "loopback".
    test("a rendered address reads back as the address that rendered it") {
      val addresses = List(
        DatabaseAddress("ep-x.neon.tech:5432", "/neondb"),
        DatabaseAddress("[::1]:5432", "/ccas"),
        DatabaseAddress("localhost", "")
      )
      assertTrue(
        addresses.forall(address => DatabaseAddress.fromRendered(address.render).contains(address)),
        DatabaseAddress.fromRendered("").isEmpty,
        DatabaseAddress.fromRendered("/ccas").isEmpty
      )
    }
  )

  // The premise of the boot prompt: what it resolves before the pool exists is where the pool then connects.
  private def testConfiguredAddressIsLiveAddress = test("the configured address is the one a live client connects to") {
    for {
      configured <- PostgresClient.configuredAddress
      live       <- ZIO.serviceWith[PostgresClient](_.addressOption)
    } yield assertTrue(configured.isDefined, configured == live, configured.exists(_.render.endsWith("/ccas_test")))
  }

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
