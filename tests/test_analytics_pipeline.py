"""Decisiones de publicación con dobles; no prueba Delta ni Spark Connect real."""
import importlib.util
import unittest
from unittest.mock import MagicMock, patch
from contextlib import ExitStack
from twitch_lakehouse.config import Settings
from twitch_lakehouse.analytics_pipeline import build_analytics


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Requiere importar módulo de transformaciones")
class PublicationTests(unittest.TestCase):
    def run_pipeline(self, fail_at=None):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        spark.sql.return_value.first.return_value = {"version": 7}
        spark.read.option.return_value.table.return_value.count.return_value = 5
        events = []
        def write(spark_arg, frame, table):
            kind = table.split("gold_i3_")[1].removesuffix("_r01")
            events.append(kind)
            if kind == fail_at:
                raise RuntimeError("artificial write failure")
        def compare(*args):
            if fail_at == "compare":
                raise ValueError("artificial persistence mismatch")
        with ExitStack() as stack:
            for name in ("audience_by_streamer", "duration_distribution", "activity_by_step"):
                stack.enter_context(patch(f"twitch_lakehouse.analytics.{name}", return_value=MagicMock()))
            stack.enter_context(patch("twitch_lakehouse.analytics.validate_sources", return_value={"silver_rows": 5}))
            stack.enter_context(patch("twitch_lakehouse.analytics.validate_aggregates"))
            stack.enter_context(patch("twitch_lakehouse.analytics.assert_equal_frames", side_effect=compare))
            stack.enter_context(patch("twitch_lakehouse.analytics_pipeline.write_new", side_effect=write))
            if fail_at:
                with self.assertRaises((RuntimeError, ValueError)):
                    build_analytics(spark, Settings())
            else:
                result = build_analytics(spark, Settings())
                self.assertEqual(result["quality"]["silver_version"], 7)
        self.assertEqual(spark.read.option.call_count, 4)
        self.assertTrue(all(call.args == ("versionAsOf", 7) for call in spark.read.option.call_args_list))
        return events

    def test_success_writes_quality_last(self):
        self.assertEqual(self.run_pipeline(), ["streamer", "duration", "activity", "quality"])

    def test_partial_failure_never_publishes_quality(self):
        self.assertEqual(self.run_pipeline("duration"), ["streamer", "duration"])

    def test_persistence_mismatch_never_publishes_quality(self):
        self.assertEqual(self.run_pipeline("compare"), ["streamer", "duration", "activity"])
