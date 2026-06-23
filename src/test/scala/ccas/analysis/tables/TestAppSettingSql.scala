package ccas.analysis.tables

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestAppSettingSql extends ZIOSpecDefault {

  import AppSetting.CacheRetentionDays

  // Each DB test wipes first, so tests are order-independent; sequential keeps the shared table race-free.
  override def spec: Spec[Any, Throwable] = suite("TestAppSettingSql")(
    testRegistryKeysUniqueAndDefaultsRoundTrip,
    testGetMissingReturnsDefault,
    testSetThenGetRoundTrips,
    testSetOverridesDefault,
    testGetUnparseableFallsBackToDefault,
    testSelectAllReflectsWrites
  ).provideShared(
    FreshSchemaLayer("test_app_setting", onInit = AppSetting.createTable.unit)
  ) @@ TestAspect.sequential

  // Pure guard (no DB): the registry must have unique keys, and every key's codec must round-trip its own default —
  // else get would store-then-reject a setting and silently fall back. Bites when a 2nd key is added.
  private def testRegistryKeysUniqueAndDefaultsRoundTrip =
    test("registry keys are unique and each codec round-trips its default") {
      val keys = AppSetting.all.map(_.key)
      assertTrue(keys.distinct.size == keys.size, AppSetting.all.forall(k => roundTrips(k)))
    }

  private def testGetMissingReturnsDefault = test("get returns the compiled default when the row is absent") {
    for {
      _ <- wipe
      v <- AppSetting.get(CacheRetentionDays)
    } yield assertTrue(v == CacheRetentionDays.default)
  }

  private def testSetThenGetRoundTrips = test("set then get round-trips a typed value") {
    for {
      _ <- wipe
      _ <- AppSetting.set(CacheRetentionDays, 14)
      v <- AppSetting.get(CacheRetentionDays)
    } yield assertTrue(v == 14)
  }

  private def testSetOverridesDefault = test("a stored value overrides the compiled default") {
    for {
      _ <- wipe
      _ <- AppSetting.set(CacheRetentionDays, 3)
      v <- AppSetting.get(CacheRetentionDays)
    } yield assertTrue(v == 3, CacheRetentionDays.default != 3)
  }

  private def testGetUnparseableFallsBackToDefault =
    test("get falls back to the default when the stored value is unparseable") {
      for {
        _ <- wipe
        _ <- writeRaw(CacheRetentionDays.key, "not-a-number")
        v <- AppSetting.get(CacheRetentionDays)
      } yield assertTrue(v == CacheRetentionDays.default)
    }

  private def testSelectAllReflectsWrites = test("selectAll returns the stored rows") {
    for {
      _ <- wipe
      _ <- AppSetting.set(CacheRetentionDays, 9)
      all <- AppSetting.selectAll
    } yield assertTrue(all.exists(r => r.key == CacheRetentionDays.key && r.value == "9"))
  }

  private def roundTrips[A](k: AppSetting.Key[A]): Boolean = k.parse(k.render(k.default)).contains(k.default)

  private val wipe = connectZIO(sql"DELETE FROM app_setting".update.run())

  // Write a raw (possibly invalid) value directly, to exercise the parse-fallback path get cannot reach via set.
  private def writeRaw(key: String, value: String) =
    connectZIO {
      sql"""INSERT INTO app_setting (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""".update.run()
    }
}
