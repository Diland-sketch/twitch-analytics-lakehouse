"""Controles de autorización y aislamiento, sin servicios externos."""
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from kafka_ingest.contracts import VERSION
from kafka_ingest.medallion import Release, validate_authorization, run, BRONZE, AUTHORIZATION, STAGES


def authorization_fixture():
    m = {"version": VERSION, "twitch_load_id": "00000000-0000-4000-8000-000000000001",
        "source_sha256": "a" * 64, "source_cluster_id": "b" * 64,
        "topic": "twitch.pilot.fixture", "start_row": 1, "count": 1000, "payload_sha256": "c" * 64}
    binding = {"manifest": m, "pipeline_id": "fixture-pipeline"}
    report = {"audit_contract": "twitch-native-source-audit-v2", "status": "PASS",
        "source_coverage_status": "PASS", "identity_integrity_status": "PASS",
        "status_scope": "SOURCE_IDENTITY_AND_CONTENT_ONLY", "bronze_table": BRONZE,
        "bronze_delta_version": 7, "bronze_rows": 1000, "source_rows": 1000,
        "distinct_event_ids": 1000, "first_source_row": 1, "last_source_row": 1000,
        "missing_source_rows": 0, "missing_event_ids": 0, "unexpected_event_ids": 0,
        "duplicate_event_id_rows": 0, "identity_errors": 0, "key_identity_errors": 0,
        "payload_conflicts": 0, "event_payload_conflicts": 0,
        "source_sha256": m["source_sha256"], "twitch_load_id": m["twitch_load_id"],
        "payload_sha256": m["payload_sha256"], "expected_payload_sha256": m["payload_sha256"],
        "pipeline_id": binding["pipeline_id"], "expected_event_ids_sha256": "d" * 64,
        "observed_event_ids_sha256": "d" * 64, "offset_audit_status": "NO VERIFICABLE"}
    return report, binding


class MedallionGuards(unittest.TestCase):
    def test_authorized_v7_accepts_unknown_offsets_without_claiming_them(self):
        report, binding = authorization_fixture()
        self.assertEqual(validate_authorization(report, binding), binding["manifest"])
        for field, value in (("bronze_delta_version", 8), ("source_rows", 999),
                ("source_coverage_status", "FAIL"), ("duplicate_event_id_rows", 1),
                ("payload_sha256", "e" * 64), ("bronze_table", "workspace.twitch_lakehouse.bronze_interactions")):
            with self.assertRaises(ValueError):
                validate_authorization({**report, field: value}, binding)

    def test_destinations_are_versioned_and_cannot_target_r01(self):
        for name in ("r01", "kafka_b8_r001", "../twitch_lakehouse", "kafka_b7_r001; DROP"):
            with self.assertRaises(ValueError):
                Release(name)
        for stage in STAGES:
            self.assertTrue(Release().table(stage).startswith("workspace.twitch_kafka_experimental."))
        with self.assertRaises(ValueError):
            Release().table("bronze")

    def test_existing_destination_stops_before_any_read_or_write(self):
        spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda _: True))
        with patch("kafka_ingest.medallion.fixed_json") as read, patch("kafka_ingest.medallion.write_new") as write:
            with self.assertRaises(RuntimeError):
                run(spark)
            read.assert_not_called()
            write.assert_not_called()

    def test_failed_authorization_cannot_write(self):
        report, binding = authorization_fixture()
        report["status"] = "FAIL"
        spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda _: False))
        with patch("kafka_ingest.medallion.fixed_json", side_effect=[(report, 0), (binding, 0)]), patch("kafka_ingest.medallion.write_new") as write:
            with self.assertRaises(ValueError):
                run(spark)
            write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
