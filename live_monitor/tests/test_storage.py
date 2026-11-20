"""Datos ARTIFICIALES: atomicidad, duplicados y lectores concurrentes."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from live_monitor.storage import connect, commit_batch, snapshot


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "test.sqlite"
        self.conn = connect(self.path)
        self.rows = [dict(event_key="e1", run_id="fixture", payload_hash="hash1", valid=True,
                          errors="", streamer_username="alpha", estimated_minutes=20,
                          topic="fixture", partition=0, offset=0),
                     dict(event_key="e2", run_id="fixture", payload_hash="hash2", valid=False,
                          errors="invalid", streamer_username=None, estimated_minutes=None,
                          topic="fixture", partition=0, offset=1)]
        self.aggregates = [dict(run_id="fixture", streamer_username="alpha", interactions=1, estimated_minutes=20)]

    def tearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def write(self, batch=0, rows=None, aggregates=None):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            result = commit_batch(self.conn, "pipeline", batch,
                self.rows if rows is None else rows, self.aggregates if aggregates is None else aggregates, [])
            self.conn.commit()
            return result
        except BaseException:
            self.conn.rollback()
            raise

    def test_commit_and_retry_same_microbatch(self):
        self.assertTrue(self.write())
        self.assertFalse(self.write())
        data = snapshot(self.path)
        self.assertEqual((data["total"], data["valid"], data["invalid"]), (2, 1, 1))
        self.assertEqual(data["streamers"][0]["interactions"], 1)

    def test_same_events_new_kafka_offsets_do_not_inflate(self):
        self.write()
        replay = [dict(r, offset=r["offset"] + 2) for r in self.rows]
        self.write(1, replay, [])
        data = snapshot(self.path)
        self.assertEqual(data["total"], 2)
        self.assertEqual(data["transport_total"], 4)
        self.assertEqual(data["batches"][0]["new_events"], 0)

    def test_mismatched_aggregate_rolls_back_everything(self):
        with self.assertRaises(ValueError):
            self.write(aggregates=[])
        self.assertEqual(snapshot(self.path)["total"], 0)
        self.assertEqual(snapshot(self.path)["batches"], [])

    def test_conflicting_event_identity_stops_without_mutation(self):
        self.write()
        changed = [dict(self.rows[0], payload_hash="changed", offset=99)]
        with self.assertRaises(ValueError):
            self.write(1, changed, [])
        self.assertEqual(snapshot(self.path)["transport_total"], 2)

    def test_reader_does_not_observe_uncommitted_batch(self):
        self.conn.execute("BEGIN IMMEDIATE")
        commit_batch(self.conn, "pipeline", 0, self.rows, self.aggregates, [])
        self.assertEqual(snapshot(self.path)["total"], 0)
        self.conn.commit()
        self.assertEqual(snapshot(self.path)["total"], 2)

    def test_minutes_mismatch_rolls_back(self):
        bad = [dict(self.aggregates[0], estimated_minutes=999)]
        with self.assertRaises(ValueError):
            self.write(aggregates=bad)
        self.assertEqual(snapshot(self.path)["total"], 0)

    def test_run_selection_is_bound_parameter(self):
        self.write()
        self.assertEqual(snapshot(self.path, "fixture")["total"], 2)
        self.assertEqual(snapshot(self.path, "' OR 1=1 --")["total"], 0)

    def test_readonly_connection_cannot_mutate(self):
        reader = connect(self.path, readonly=True)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute("INSERT INTO metadata VALUES('x','y')")
        finally:
            reader.close()
