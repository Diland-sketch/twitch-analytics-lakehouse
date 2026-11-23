"""Fixtures artificiales sobre Spark real: rechazos, dos dedup y conciliación."""
import importlib.util
import unittest
from kafka_ingest.medallion import frames, reconcile
from tests.test_kafka_medallion import authorization_fixture


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Requiere PySpark")
class MedallionSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession, functions as F
        cls.spark = (SparkSession.builder.master("local[2]").appName("twitch-medallion-fixtures")
            .config("spark.sql.shuffle.partitions", "2").config("spark.sql.ansi.enabled", "true").getOrCreate())
        _, binding = authorization_fixture()
        cls.m = binding["manifest"]
        source = cls.spark.sql("""SELECT * FROM VALUES
            (1L, '1,11,alpha,0,2'), (2L, ' 1 ,11,alpha,0,2'),
            (3L, '0,12,alpha,0,2'), (4L, '2,13,beta,2,2'),
            (5L, 'bad'), (6L, '3,14,beta,1,4') AS t(twitch_source_row, raw_line)""")
        eid = F.sha2(F.concat_ws("|", F.lit(cls.m["source_sha256"]), "twitch_source_row"), 256)
        payload = F.to_json(F.struct(F.lit(cls.m["twitch_load_id"]).alias("twitch_load_id"),
            F.lit(cls.m["source_sha256"]).alias("source_sha256"), "twitch_source_row",
            eid.alias("event_id"), F.lit(True).alias("historical_replay"), "raw_line"))
        cls.bronze = source.select(eid.cast("binary").alias("key"), payload.cast("binary").alias("value"))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_quality_source_then_business_dedup_and_gold(self):
        bronze = self.bronze.unionByName(self.bronze.limit(1))
        result = frames(bronze, self.m)
        report = reconcile(result["source_classification"], result["rejects"], result["silver"], result["gold"])
        self.assertEqual(report["status"], "PASS")
        self.assertEqual({k: report[k] for k in ("bronze_rows", "source_rows", "source_duplicate_rows",
            "rejected_source_rows", "business_duplicate_rows", "silver_rows", "estimated_minutes")},
            {"bronze_rows": 7, "source_rows": 6, "source_duplicate_rows": 1,
             "rejected_source_rows": 2, "business_duplicate_rows": 1, "silver_rows": 3, "estimated_minutes": 50})
        self.assertEqual([r.asDict() for r in result["gold"].orderBy("streamer_username").collect()], [
            {"streamer_username": "alpha", "interactions": 1, "unique_users": 1, "unique_streams": 1, "estimated_minutes": 20},
            {"streamer_username": "beta", "interactions": 2, "unique_users": 2, "unique_streams": 2, "estimated_minutes": 30}])
        rejects = result["rejects"].orderBy("twitch_source_row").collect()
        self.assertIn("invalid_user_id", rejects[0].quality_errors)
        self.assertIn("field_count", rejects[1].quality_errors)
        self.assertEqual(result["silver"].filter("user_id = 1").first().twitch_source_row, 1)
        from pyspark.sql import functions as F
        corrupt = result["gold"].withColumn("unique_users", F.lit(999))
        self.assertEqual(reconcile(result["source_classification"], result["rejects"], result["silver"], corrupt)["status"], "FAIL")

    def test_conflicting_identity_or_malformed_json_stops(self):
        from pyspark.sql import functions as F
        altered = self.bronze.limit(1).withColumn("value",
            F.regexp_replace(F.col("value").cast("string"), "alpha", "changed").cast("binary"))
        with self.assertRaises(ValueError):
            frames(self.bronze.unionByName(altered), self.m)
        with self.assertRaises(ValueError):
            frames(self.bronze.limit(1).withColumn("value", F.lit("not-json").cast("binary")), self.m)

    def test_all_rejected_has_empty_silver_and_gold_with_zero_sums(self):
        from pyspark.sql import functions as F
        from kafka_ingest.contracts import event_id
        rejected_input = self.bronze.filter(F.col("key").cast("string").isin(
            event_id(self.m["source_sha256"], 3), event_id(self.m["source_sha256"], 5)))
        result = frames(rejected_input, self.m)
        report = reconcile(result["source_classification"], result["rejects"], result["silver"], result["gold"])
        self.assertEqual(report["status"], "PASS")
        self.assertEqual((report["rejected_source_rows"], report["silver_rows"],
            report["gold_streamers"], report["estimated_minutes"]), (2, 0, 0, 0))


if __name__ == "__main__":
    unittest.main()
