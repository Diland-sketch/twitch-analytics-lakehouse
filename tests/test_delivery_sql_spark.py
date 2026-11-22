"""Regresión real de auditoría SQL: buckets globales distintos y dominios NULL."""
import importlib.util
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Requiere Spark para SQL de auditoría")
class DeliverySQLTests(unittest.TestCase):
    def test_distinct_buckets_and_null_domains(self):
        from pyspark.sql import SparkSession, functions as F
        spark = (SparkSession.builder.master("local[2]").appName("candidate-audit-sql")
                 .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        try:
            streamer = spark.sql("SELECT 'alpha' streamer_username, 2L interactions, 1L unique_users, "
                "1L unique_streams, 1L recurring_users, 1D recurrence_rate, 10L estimated_minutes")
            streamer.createOrReplaceTempView("candidate_streamer")
            duration = spark.sql("SELECT * FROM VALUES (0, 1L, 0L), (0, 1L, 0L), (1, 1L, 10L) "
                                 "AS t(duration_bucket,interactions,estimated_minutes)")
            duration.createOrReplaceTempView("candidate_duration")
            spark.sql("SELECT 3L silver_rows").createOrReplaceTempView("candidate_quality")
            source = (Path(__file__).resolve().parents[1] / "docs/entrega_final/sql/07_reglas_i3.sql").read_text(encoding="utf-8")
            for kind in ("streamer", "duration", "quality"):
                source = source.replace(f"workspace.twitch_lakehouse.gold_i3_{kind}_r01", f"candidate_{kind}")
            result = spark.sql(source).first()
            self.assertEqual(result.buckets_distintos, 2)  # Tres filas, dos buckets.
            self.assertEqual(result.delta_buckets, 0)
            self.assertEqual(result.violaciones_streamer, 0)
            streamer.withColumn("unique_users", F.lit(None).cast("long")).createOrReplaceTempView("candidate_streamer")
            duration.withColumn("duration_bucket", F.lit(None).cast("int")).createOrReplaceTempView("candidate_duration")
            bad = spark.sql(source).first()
            self.assertEqual(bad.violaciones_streamer, 1)
            self.assertEqual(bad.buckets_invalidos, 3)
        finally:
            spark.stop()
