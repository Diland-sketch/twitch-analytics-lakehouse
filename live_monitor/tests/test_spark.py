"""JSON y sink sobre Spark real; transporte Kafka se prueba en e2e.py."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from live_monitor.common import event_id


@unittest.skipUnless(os.environ.get("I4_SPARK_TESTS") == "1", "Ejecutar dentro de imagen I4 con I4_SPARK_TESTS=1")
class SparkContractTests(unittest.TestCase):
    def test_quality_and_idempotent_sink(self):
        from pyspark.sql import SparkSession
        from live_monitor.quality import transform
        from live_monitor.consumer import foreach_batch
        from live_monitor.storage import connect, snapshot
        spark = (SparkSession.builder.master("local[2]").appName("i4-artificial-checks")
                 .config("spark.sql.shuffle.partitions", "2").config("spark.sql.ansi.enabled", "true").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        try:
            self.assertEqual(spark.version, "3.5.8")
            run, sha = "00000000-0000-0000-0000-000000000001", "a" * 64
            def payload(row, line):
                return json.dumps(dict(run_id=run, source_sha256=sha, source_row=row,
                    event_id=event_id(run, sha, row), historical_replay=True,
                    produced_at="2026-01-01T00:00:00+00:00", raw_line=line))
            values = [payload(1, "1,11,alpha,0,2"), payload(2, "2,11,alpha,2,2"),
                      payload(3, "bad,11,alpha,0,1"), "{invalid", payload(4, "1,11,alpha,5,2"),
                      payload(5, "9223372036854775808,11,alpha,0,1")]
            def frame(offset_delta=0):
                literals = ",".join(f"({i + offset_delta},'{v.replace(chr(39), chr(39)*2)}')" for i, v in enumerate(values))
                return transform(spark.sql("SELECT 'fixture' AS topic, 0 AS partition, CAST(o AS BIGINT) AS offset, "
                    "current_timestamp() AS timestamp, CAST(v AS BINARY) AS value FROM VALUES " + literals + " AS t(o,v)"))
            result = frame()
            self.assertEqual(result.filter("valid").count(), 2)
            self.assertEqual(result.filter("NOT valid").count(), 4)
            with tempfile.TemporaryDirectory() as folder:
                db = Path(folder) / "fixture.sqlite"
                with patch("live_monitor.consumer.connect", side_effect=lambda: connect(db)):
                    foreach_batch(result, 0, "fixture-pipeline")
                    foreach_batch(result, 0, "fixture-pipeline")
                    # Metadata inválida no tiene identidad estable: se cuenta por offset.
                    identifiable = frame(10).filter("identity_valid")
                    foreach_batch(identifiable, 1, "fixture-pipeline")
                saved = snapshot(db)
                self.assertEqual(saved["total"], 6)
                self.assertEqual(saved["transport_total"], 11)
                self.assertEqual(saved["streamers"][0]["interactions"], 2)
                self.assertEqual(saved["streamers"][0]["estimated_minutes"], 20)
        finally:
            spark.stop()
