"""Spark REAL sobre fixtures artificiales. No usa Delta ni acredita Databricks."""
import importlib.util
import json
import unittest
from unittest.mock import patch
from kafka_ingest.contracts import VERSION, canonical, digest, event_id
from kafka_ingest.bronze import project, audit


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Requiere PySpark")
class SparkPilotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession, functions as F
        cls.spark = (SparkSession.builder.master("local[2]").appName("twitch-pilot-fixtures")
            .config("spark.sql.shuffle.partitions", "2").config("spark.sql.ansi.enabled", "true").getOrCreate())
        cls.m = {"version": VERSION, "twitch_load_id": "00000000-0000-4000-8000-000000000001",
            "source_sha256": "a" * 64, "source_cluster_id": "b" * 64,
            "topic": "twitch.pilot.fixture", "start_row": 1, "count": 1000,
            "payload_sha256": digest(canonical([f"{i},1,fixture,0,2" for i in range(1, 1001)]))}
        base = cls.spark.range(1, 1001).selectExpr("id AS twitch_source_row", "id - 1 AS offset",
            "concat(cast(id as string), ',1,fixture,0,2') AS raw_line")
        envelope = F.to_json(F.struct(F.lit(cls.m["twitch_load_id"]).alias("twitch_load_id"),
            "twitch_source_row", F.lit(cls.m["source_sha256"]).alias("source_sha256"),
            F.sha2(F.concat_ws("|", F.lit(cls.m["source_sha256"]), "twitch_source_row"), 256).alias("event_id"),
            F.lit(True).alias("historical_replay"), F.lit("2026-01-01T00:00:00Z").alias("produced_at"), "raw_line"))
        cls.kafka = base.select(F.lit(cls.m["topic"]).alias("topic"), F.lit(0).alias("partition"),
            "offset", F.lit(None).cast("binary").alias("key"), envelope.cast("binary").alias("value"),
            F.current_timestamp().alias("timestamp"), F.lit(0).alias("timestampType"))
        cls.bronze = project(cls.kafka, cls.m["source_cluster_id"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_audit(self, frame):
        frame.createOrReplaceTempView("pilot_fixture")
        with patch("kafka_ingest.bronze.BRONZE", "pilot_fixture"):
            return audit(self.spark, self.m)

    def test_1000_and_resend_keep_source_count(self):
        from pyspark.sql import functions as F
        report = self.run_audit(self.bronze)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["transport_rows"], 1000)
        replay = project(self.kafka.withColumn("offset", F.col("offset") + 1000), self.m["source_cluster_id"])
        report = self.run_audit(self.bronze.unionByName(replay))
        self.assertEqual(report["status"], "PASS")
        self.assertEqual((report["transport_rows"], report["source_rows"], report["source_replays"]), (2000, 1000, 1000))

    def test_offset_replay_payload_conflict_and_source_gap_fail(self):
        from pyspark.sql import functions as F
        duplicate = self.run_audit(self.bronze.unionByName(self.bronze.limit(1)))
        self.assertEqual(duplicate["status"], "FAIL")
        conflict = self.bronze.limit(1).withColumn("raw_line", F.lit("1,1,altered,0,2"))
        self.assertEqual(self.run_audit(self.bronze.unionByName(conflict))["status"], "FAIL")
        self.assertEqual(self.run_audit(self.bronze.filter("twitch_source_row <> 500"))["status"], "FAIL")

    def test_business_duplicates_and_rejections_are_distinct(self):
        from pyspark.sql import functions as F
        changed = self.bronze.withColumn("raw_line", F.when(F.col("twitch_source_row") == 2,
            F.lit("1,1,fixture,0,2")).when(F.col("twitch_source_row") == 3, F.lit("invalid")).otherwise(F.col("raw_line")))
        report = self.run_audit(changed)
        self.assertEqual(report["business_duplicates"], 1)
        self.assertEqual(report["rejected_source_rows"], 1)
        self.assertEqual(report["status"], "FAIL")  # hash original cambió

    def test_native_metadata_adapter_preserves_offset_and_payload(self):
        from pyspark.sql import functions as F
        native = self.kafka.select("key", "value", F.struct("topic", "partition", "offset",
            "timestamp", "timestampType").alias("kafka_source"))
        normalized = project(native.select("key", "value", "kafka_source.*"), self.m["source_cluster_id"])
        report = self.run_audit(normalized)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["offsets"], [{"partition": 0, "min_offset": 0,
            "max_offset": 999, "offset_count": 1000}])


if __name__ == "__main__":
    unittest.main()
