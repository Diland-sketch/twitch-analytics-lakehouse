import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "PySpark no instalado; ejecutar notebook 01")
class SparkTests(unittest.TestCase):
    def test_artificial_transformations(self):
        from pyspark.sql import SparkSession
        from tests.spark_checks import run_spark_checks
        spark = (SparkSession.builder.master("local[2]").appName("i1-artificial-tests")
                 .config("spark.sql.ansi.enabled", "true").getOrCreate())
        try:
            self.assertEqual(run_spark_checks(spark)["status"], "PASS")
        finally:
            spark.stop()
