package ccas.analysis.tables

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestAppSettingSql extends ZIOSpecDefault {

  import AppSettings.CacheRetentionDays

  // Order-coupled, shared schema (sequential): testGetMissingReturnsDefault reads the empty table and MUST run
  // before any test writes a row.
  override def spec: Spec[Any, Throwable] = suite("TestAppSettingSql")(
    testGetMissingReturnsDefault,
    testSetThenGetRoundTrips,
    testSetOverridesDefault,
    testGetUnparseableFallsBackToDefault,
    testSelectAllReflectsWrites
  ).provideShared(
    FreshSchemaLayer("test_app_setting", onInit = AppSetting.createTable.unit)
  ) @@ TestAspect.sequential

  private def testGetMissingReturnsDefault = test("get returns the compiled default when the row is absent") {
    for {
      v <- AppSetting.get(CacheRetentionDays)
    } yield assertTrue(v == CacheRetentionDays.default)
  }

  private def testSetThenGetRoundTrips = test("set then get round-trips a typed value") {
    for {
      _ <- AppSetting.set(CacheRetentionDays, 14)
      v <- AppSetting.get(CacheRetentionDays)
    } yield assertTrue(v == 14)
  }

  private def testSetOverridesDefault = test("a stored value overrides the compiled default") {
    for {
      _ <- AppSetting.set(CacheRetentionDays, 3)
      v <- AppSetting.get(CacheRetentionDays)
    } yield assertTrue(v == 3, CacheRetentionDays.default != 3)
  }

  private def testGetUnparseableFallsBackToDefault =
    test("get falls back to the default when the stored value is unparseable") {
      for {
        _ <- writeRaw(CacheRetentionDays.key, "not-a-number")
        v <- AppSetting.get(CacheRetentionDays)
      } yield assertTrue(v == CacheRetentionDays.default)
    }

  private def testSelectAllReflectsWrites = test("selectAll returns the stored rows") {
    for {
      _ <- AppSetting.set(CacheRetentionDays, 9)
      all <- AppSetting.selectAll
    } yield assertTrue(all.exists(r => r.key == CacheRetentionDays.key && r.value == "9"))
  }

  // Write a raw (possibly invalid) value directly, to exercise the parse-fallback path get cannot reach via set.
  private def writeRaw(key: String, value: String) =
    connectZIO {
      sql"""INSERT INTO app_setting (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""".update.run()
    }
}
