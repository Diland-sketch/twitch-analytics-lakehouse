"""Fixtures artificiales de contrato: NO acreditan Kafka/Databricks remotos."""
import gzip
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from kafka_ingest.contracts import canonical, event_id, validate_manifest, verify_report
from kafka_ingest.producer import prepare, publish, load_block
from kafka_ingest.controller import execute
from kafka_ingest.bronze import options


class Producer:
    def __init__(self, fail_at=None):
        self.messages = []
        self.fail_at = fail_at
        self.closed = False

    def send(self, topic, key, value):
        offset = len(self.messages)
        if offset == self.fail_at:
            raise RuntimeError("Fixture: fallo de ACK")
        self.messages.append(value)
        return SimpleNamespace(get=lambda timeout: SimpleNamespace(topic=topic, partition=0, offset=offset))

    def flush(self, **_):
        pass

    def close(self, **_):
        self.closed = True


class PilotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = self.root / "fixture.csv.gz"
        with gzip.open(source, "wt") as stream:
            for i in range(1005):
                stream.write(f"{i+1},1,fixture_streamer,0,2\n")
        self.block = self.root / "block"
        self.m = prepare(source, self.block, "twitch.pilot.fixture", "fixture-cluster")

    def report(self):
        return {**{k: self.m[k] for k in ("twitch_load_id", "source_sha256", "payload_sha256", "topic", "source_cluster_id")},
            "status": "PASS", "source_rows": 1000, "first_source_row": 1, "last_source_row": 1000,
            "transport_rows": 1000, "identity_errors": 0, "payload_conflicts": 0,
            "transport_duplicates": 0, "audit_table": "workspace.twitch_kafka_experimental.audit_fixture",
            "offsets": [{"partition": 0, "min_offset": 0, "max_offset": 999, "offset_count": 1000}]}

    def send(self):
        producer = Producer()
        with patch("kafka_ingest.producer.time.sleep"):
            publish(self.block, factory=lambda: producer)
        return producer

    def test_prepare_bounded_and_tamper_detected(self):
        m, rows = load_block(self.block)
        self.assertEqual(len(rows), 1000)
        self.assertEqual(rows[-1], "1000,1,fixture_streamer,0,2")
        with self.assertRaises(ValueError):
            prepare(self.root / "fixture.csv.gz", self.block, m["topic"], "fixture-cluster")
        rows[0] = "altered"
        (self.block / "payload.json").write_text(canonical(rows), encoding="utf-8")
        with self.assertRaises(ValueError):
            load_block(self.block)

    def test_stable_id_and_ack_resume(self):
        producer = self.send()
        self.assertTrue(producer.closed)
        self.assertEqual(len(producer.messages), 1000)
        self.assertEqual(producer.messages[0]["event_id"], event_id(self.m["source_sha256"], 1))
        self.assertEqual(publish(self.block, factory=lambda: self.fail("No republicar"))["acknowledged"], 1000)

    def test_partial_failure_preserves_ack_and_no_cursor(self):
        producer = Producer(fail_at=2)
        with patch("kafka_ingest.producer.time.sleep"), self.assertRaises(RuntimeError):
            publish(self.block, factory=lambda: producer)
        self.assertTrue(producer.closed)
        self.assertEqual(len((self.block / "acks.jsonl").read_text().splitlines()), 2)
        self.assertFalse((self.block / "acked.json").exists())
        self.assertFalse((self.block / "cursor.json").exists())
        retry = self.send()
        self.assertEqual(retry.messages[0]["event_id"], producer.messages[0]["event_id"])

    def test_controller_resume_after_lost_job_response(self):
        self.send()
        report = self.report()
        class Jobs:
            tokens = []
            def run(inner, m, token):
                inner.tokens.append(token)
                if len(inner.tokens) == 1:
                    raise RuntimeError("Fixture: respuesta perdida después de disparar")
                return 42
            def wait(inner, run_id):
                return report
        jobs = Jobs()
        with self.assertRaises(RuntimeError):
            execute(self.block, jobs)
        self.assertFalse((self.block / "cursor.json").exists())
        result = execute(self.block, jobs)
        self.assertEqual(jobs.tokens[0], jobs.tokens[1])
        self.assertEqual(result["next_source_row"], 1001)
        self.assertEqual(execute(self.block, jobs), result)
        self.assertEqual(len(jobs.tokens), 2)

    def test_controller_no_advance_on_missing_coverage(self):
        self.send()
        report = self.report()
        report["offsets"][0]["max_offset"] = 998
        jobs = SimpleNamespace(run=lambda m, t: 1, wait=lambda _: report)
        with self.assertRaises(RuntimeError):
            execute(self.block, jobs)
        self.assertFalse((self.block / "cursor.json").exists())

    def test_controller_no_advance_on_false_remote_pass(self):
        self.send()
        for field, value in (("source_rows", 999), ("payload_sha256", "0" * 64), ("transport_duplicates", 1)):
            report = self.report()
            report[field] = value
            with self.assertRaises(RuntimeError):
                verify_report(self.m, report)

    def test_stop_and_single_writer(self):
        (self.block / "STOP").touch()
        with self.assertRaises(RuntimeError):
            publish(self.block, factory=Producer)
        (self.block / "controller.lock").touch()
        with self.assertRaises(FileExistsError):
            execute(self.block, None)

    def test_pilot_fence_and_tls_options(self):
        for change in ({"count": 1000000}, {"start_row": 1001}, {"topic": "production"}):
            with self.assertRaises(ValueError):
                validate_manifest({**self.m, **change})
        configured = options("broker:9093", self.m["topic"], 'user"', "pass\\")
        self.assertEqual(configured["failOnDataLoss"], "true")
        self.assertEqual(configured["kafka.security.protocol"], "SASL_SSL")
        self.assertIn('user\\"', configured["kafka.sasl.jaas.config"])
        with self.assertRaises(ValueError):
            options("broker:9093", self.m["topic"], "user", "password\n")


if __name__ == "__main__":
    unittest.main()
