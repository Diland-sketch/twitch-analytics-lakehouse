import importlib.util
import unittest
from twitch_lakehouse.config import Settings
from twitch_lakehouse.analytics_pipeline import targets, build_analytics
from tests.test_write_guards import FakeSpark


class AnalyticsGuardTests(unittest.TestCase):
    def test_names_never_target_i1(self):
        result = targets(Settings(), "r01")
        self.assertEqual(len(set(result.values())), 4)
        self.assertTrue(all(".gold_i3_" in name for name in result.values()))
        for value in ("", "../x", "r01;delete", "R01", "r-01"):
            with self.assertRaises(ValueError):
                targets(Settings(), value)

    @unittest.skipUnless(importlib.util.find_spec("pyspark"), "PySpark requerido para importar transformaciones")
    def test_any_existing_target_blocks_before_source_reads(self):
        for table in targets(Settings(), "r01").values():
            spark = FakeSpark()
            spark.tables[table] = ["untouched"]
            with self.assertRaisesRegex(RuntimeError, "Destino existente"):
                build_analytics(spark, Settings(), "r01")
            self.assertEqual(spark.events, [])
            self.assertEqual(spark.tables[table], ["untouched"])


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "PySpark no instalado; ejecutar notebook 07")
class AnalyticsSparkTests(unittest.TestCase):
    def test_fixtures(self):
        from pyspark.sql import SparkSession
        from tests.analytics_checks import run_analytics_checks
        spark = (SparkSession.builder.master("local[2]").appName("i3-fixtures")
                 .config("spark.sql.shuffle.partitions", "2")
                 .config("spark.sql.ansi.enabled", "true").getOrCreate())
        try:
            self.assertEqual(run_analytics_checks(spark)["status"], "PASS")
        finally:
            spark.stop()
