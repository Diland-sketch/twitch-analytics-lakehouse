"""Piloto artificial: cobertura fuente independiente de offsets no observados."""
import copy
import unittest
from kafka_ingest.contracts import VERSION, canonical, digest, event_id, verify_report
from kafka_ingest.native_audit import audit_records


class NativeAuditTests(unittest.TestCase):
    def setUp(self):
        self.lines = [f"{i},1,fixture,0,2" for i in range(1, 1001)]
        self.m = {"version": VERSION, "twitch_load_id": "00000000-0000-4000-8000-000000000001",
            "source_sha256": "a" * 64, "source_cluster_id": "b" * 64,
            "topic": "twitch.pilot.fixture", "start_row": 1, "count": 1000,
            "payload_sha256": digest(canonical(self.lines))}
        self.records = []
        for row, line in enumerate(self.lines, 1):
            eid = event_id(self.m["source_sha256"], row)
            e = {"twitch_load_id": self.m["twitch_load_id"], "source_sha256": self.m["source_sha256"],
                "twitch_source_row": row, "event_id": eid, "historical_replay": True, "raw_line": line}
            self.records.append({"key": eid.encode(), "value": canonical(e).encode()})

    def change(self, index, **changes):
        import json
        e = json.loads(self.records[index]["value"])
        e.update(changes)
        self.records[index]["value"] = canonical(e).encode()

    def test_binary_only_pass_without_fabricated_metadata(self):
        report = audit_records(self.records, self.m)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["source_coverage_status"], "PASS")
        self.assertEqual(report["distinct_event_ids"], 1000)
        self.assertEqual(report["source_rows"], 1000)
        self.assertEqual(report["payload_sha256"], self.m["payload_sha256"])
        self.assertEqual(report["offset_audit_status"], "NO VERIFICABLE")
        self.assertIsNone(report["offsets"])
        self.assertIsNone(report["transport_duplicates"])
        self.assertNotIn("topic", report)
        self.assertNotIn("source_cluster_id", report)
        # El controlador estricto de transporte NO acepta este PASS parcial.
        with self.assertRaises(RuntimeError):
            verify_report(self.m, report)

    def test_missing_event_cannot_pass_even_with_1000_bronze_rows(self):
        self.records[-1] = self.records[0]
        report = audit_records(self.records, self.m)
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["missing_event_ids"], 1)
        self.assertEqual(report["missing_source_rows"], 1)
        self.assertEqual(report["source_rows"], 999)
        self.assertEqual(report["duplicate_event_id_rows"], 1)

    def test_identical_duplicate_preserves_coverage_but_fails_integrity(self):
        report = audit_records(self.records + [self.records[0]], self.m)
        self.assertEqual(report["source_coverage_status"], "PASS")
        self.assertEqual(report["identity_integrity_status"], "FAIL")
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["payload_conflicts"], 0)

    def test_payload_conflict_and_wrong_content_hash_fail(self):
        duplicate = copy.deepcopy(self.records[0])
        self.change(0, raw_line="1,1,altered,0,2")
        report = audit_records(self.records + [duplicate], self.m)
        self.assertEqual(report["source_coverage_status"], "FAIL")
        self.assertEqual(report["payload_conflicts"], 1)
        self.assertEqual(report["event_payload_conflicts"], 1)
        self.assertIsNone(report["payload_sha256"])
        report = audit_records(self.records, self.m)
        self.assertEqual(report["identity_errors"], 0)
        self.assertNotEqual(report["payload_sha256"], self.m["payload_sha256"])
        self.assertEqual(report["source_coverage_status"], "FAIL")

    def test_identity_fields_key_and_json_errors_fail(self):
        for field, value, counter in (("source_sha256", "c" * 64, "source_sha256_errors"),
                ("twitch_load_id", "another-load", "load_id_errors"),
                ("twitch_source_row", True, "source_row_errors"),
                ("event_id", "0" * 64, "event_id_errors")):
            original = self.records[0]["value"]
            self.change(0, **{field: value})
            report = audit_records(self.records, self.m)
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report[counter], 1)
            self.records[0]["value"] = original
        self.records[0]["key"] = b"wrong"
        self.assertEqual(audit_records(self.records, self.m)["key_identity_errors"], 1)
        self.records[0]["value"] = b"not-json"
        self.assertEqual(audit_records(self.records, self.m)["malformed_payload_rows"], 1)

    def test_metadata_is_a_separate_control_including_partial_history(self):
        for i, r in enumerate(self.records):
            r["kafka_source"] = {"topic": self.m["topic"], "partition": 0, "offset": i + 300}
        report = audit_records(self.records, self.m)
        self.assertEqual(report["offset_audit_status"], "PASS")
        self.assertEqual(report["offsets"][0]["min_offset"], 300)
        self.records[0]["kafka_source"]["offset"] = 301
        report = audit_records(self.records, self.m)
        self.assertEqual(report["source_coverage_status"], "PASS")
        self.assertEqual(report["offset_audit_status"], "FAIL")
        self.records[0]["kafka_source"] = None
        report = audit_records(self.records, self.m)
        self.assertEqual(report["source_coverage_status"], "PASS")
        self.assertEqual(report["offset_audit_status"], "NO VERIFICABLE")
        self.assertIsNone(report["offsets"])

    def test_empty_and_size_limit(self):
        report = audit_records([], self.m)
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["missing_source_rows"], 1000)
        with self.assertRaises(ValueError):
            audit_records(self.records * 6, self.m)


if __name__ == "__main__":
    unittest.main()
