"""DAG sobre Spark real con almacenamiento fake versionado, NO Delta/Job remoto."""
import importlib.util
import json
import unittest
from unittest.mock import patch
from kafka_ingest.contracts import VERSION, canonical, digest
from kafka_ingest.orchestration import verify_bronze, authorize, transform, reconcile_release, publish
from tests.test_kafka_orchestration import context


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Requiere PySpark")
class JobSparkTests(unittest.TestCase):
    def test_five_stages_then_repair_without_appends_and_corruption_block(self):
        from pyspark.sql import SparkSession, functions as F
        from kafka_ingest.medallion import BRONZE
        from types import SimpleNamespace
        spark = (SparkSession.builder.master("local[2]").appName("job-fixtures")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        self.addCleanup(spark.stop)
        cfg = context()
        lines = [f"{0 if i % 50 == 0 else i},11,s{i % 3},0,2" for i in range(1, 1001)]
        m = {"version": VERSION, "twitch_load_id": "00000000-0000-4000-8000-000000000001",
            "source_sha256": "a" * 64, "source_cluster_id": "b" * 64, "topic": "twitch.pilot.fixture",
            "start_row": 1, "count": 1000, "payload_sha256": digest(canonical(lines))}
        base = spark.range(1, 1001).select(F.col("id").alias("twitch_source_row"),
            F.concat(F.when(F.col("id") % 50 == 0, F.lit(0)).otherwise(F.col("id")).cast("string"),
                F.lit(",11,s"), (F.col("id") % 3).cast("string"), F.lit(",0,2")).alias("raw_line"))
        eid = F.sha2(F.concat_ws("|", F.lit(m["source_sha256"]), "twitch_source_row"), 256)
        body = F.to_json(F.struct(F.lit(m["twitch_load_id"]).alias("twitch_load_id"),
            F.lit(m["source_sha256"]).alias("source_sha256"), "twitch_source_row",
            eid.alias("event_id"), "raw_line", F.lit(True).alias("historical_replay")))
        tables = {BRONZE: base.select(eid.cast("binary").alias("key"), body.cast("binary").alias("value")),
            cfg.manifest_table: spark.createDataFrame([(canonical({"manifest": m, "pipeline_id": "fixture"}),)], "binding_json STRING")}
        versions = {BRONZE: 7, cfg.manifest_table: 1}
        writes = []
        class Reader:
            def option(inner, key, value):
                inner.version = value
                return inner
            def table(inner, name):
                if inner.version != versions[name]:
                    raise ValueError("Fixture version mismatch")
                return tables[name]
        fake = SimpleNamespace(read=Reader(), catalog=SimpleNamespace(tableExists=lambda name: name in tables),
            table=lambda name: tables[name], createDataFrame=spark.createDataFrame,
            sql=lambda query: spark.sql(f"SELECT {versions[query.split()[2]]}L AS version"))
        def write(_spark, df, table):
            if table in tables:
                raise RuntimeError("Existing fixture destination")
            tables[table] = df
            versions[table] = 1
            writes.append(table)
        with patch("kafka_ingest.orchestration.write_new", side_effect=write):
            verify_bronze(fake, cfg)
            authorize(fake, cfg)
            transform(fake, cfg)
            result = reconcile_release(fake, cfg)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual((result["source_rows"], result["rejected_source_rows"], result["silver_rows"]), (1000, 20, 980))
            self.assertEqual(result["estimated_minutes"], 19600)
            publication = publish(fake, cfg)
            self.assertEqual(publication["publication_state"], "CONFIRMED")
            previous = list(writes)
            # Repair del mismo run: se validan artefactos existentes, sin nuevos appends.
            verify_bronze(fake, cfg)
            authorize(fake, cfg)
            transform(fake, cfg)
            reconcile_release(fake, cfg)
            publish(fake, cfg)
            self.assertEqual(writes, previous)
            versions[cfg.release.table("gold")] += 1
            with self.assertRaises(ValueError):
                publish(fake, cfg)


if __name__ == "__main__":
    unittest.main()
