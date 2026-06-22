package ccas.analysis.tables

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.utils.sql.FreshSchemaLayer

object TestAppSettingSql extends ZIOSpecDefault {

  // Order-coupled, shared schema: testSeedOnFreshDb reads the freshly-seeded 36500 and MUST run before
  // testDbWinsOverSeed mutates cache_retention_days. @@ TestAspect.sequential preserves this listed order.
  override def spec: Spec[Any, Throwable] = suite("TestAppSettingSql")(
    testSeedOnFreshDb,
    testSelectMissingReturnsNone,
    testInsertIfAbsentInserts,
    testInsertIfAbsentNoopWhenPresent,
    testUpsertOverwrites,
    testSelectAllContainsRows,
    testDbWinsOverSeed
  ).provideShared(
    FreshSchemaLayer("test_app_setting", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  // A throwaway key for CRUD tests, distinct from cache_retention_days seeded by ensureTables.
  private val k = "test_crud_key"

  private def testSeedOnFreshDb = test("ensureTables seeds cache_retention_days from the HOCON default (test = 36500)") {
    for {
      stored <- AppSetting.select(AppSetting.CacheRetentionDays)
    } yield assertTrue(stored.contains("36500"))
  }

  private def testSelectMissingReturnsNone = test("select returns None for an absent key") {
    for {
      v <- AppSetting.select("no_such_key")
    } yield assertTrue(v.isEmpty)
  }

  private def testInsertIfAbsentInserts = test("insertIfAbsent inserts a new key") {
    for {
      rows <- AppSetting.insertIfAbsent(k, "v1")
      v <- AppSetting.select(k)
    } yield assertTrue(rows == 1, v.contains("v1"))
  }

  private def testInsertIfAbsentNoopWhenPresent = test("insertIfAbsent is a no-op when the key exists") {
    for {
      rows <- AppSetting.insertIfAbsent(k, "v2")
      v <- AppSetting.select(k)
    } yield assertTrue(rows == 0, v.contains("v1"))
  }

  private def testUpsertOverwrites = test("upsert overwrites the stored value") {
    for {
      rows <- AppSetting.upsert(k, "v3")
      v <- AppSetting.select(k)
    } yield assertTrue(rows == 1, v.contains("v3"))
  }

  private def testSelectAllContainsRows = test("selectAll returns all stored settings") {
    for {
      all <- AppSetting.selectAll
    } yield assertTrue(
      all.contains(AppSetting(k, "v3")),
      all.exists(_.key == AppSetting.CacheRetentionDays)
    )
  }

  private def testDbWinsOverSeed = test("a re-run of ensureTables does not clobber an existing DB value") {
    for {
      _ <- AppSetting.upsert(AppSetting.CacheRetentionDays, "5")
      _ <- Tables.ensureTables
      stored <- AppSetting.select(AppSetting.CacheRetentionDays)
    } yield assertTrue(stored.contains("5"))
  }
}
