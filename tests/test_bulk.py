"""Contratos bulk con fixtures: ningún conteo equivale a procesamiento real."""
import copy
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from bulk_lakehouse.source import inspect, prepare
from bulk_lakehouse.pipeline import Config, ROOT, NS, validate_manifest, blocks
from bulk_lakehouse.job import settings


class BulkContracts(unittest.TestCase):
    def test_physical_count_hash_and_last_line_without_newline(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "fixture.gz"
            with gzip.open(source, "wb") as f:
                f.write(b"1,2,fixture,0,2\n3,4,fixture,1,3")
            report = inspect(source, root / "profile.json")
            self.assertEqual(report["physical_rows"], 2)
            self.assertEqual(report["expanded_bytes"], 31)
            with self.assertRaises(FileExistsError):
                inspect(source, root / "profile.json")

    def test_arrow_fragment_preserves_real_order_and_partial_evidence(self):
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "fixture.gz"
            with gzip.open(source, "wt") as f:
                for i in range(100_001):
                    f.write(f"{i+1},1,fixture,0,2\n")
            inspect(source, root / "profile.json")
            manifest = prepare(source, root / "parts", 100_000, root / "profile.json")
            validate_manifest(manifest, 100_000)
            table = pq.read_table(root / "parts/part-000000.parquet")
            self.assertEqual(table.num_rows, 100_000)
            self.assertEqual(table.column("source_row")[0].as_py(), 1)
            self.assertEqual(table.column("source_row")[-1].as_py(), 100_000)
            self.assertEqual(table.column("raw_line")[-1].as_py(), "100000,1,fixture,0,2")
            bad = copy.deepcopy(manifest)
            bad["parts"][0]["first_row"] = 2
            with self.assertRaises(ValueError):
                validate_manifest(bad, 100_000)
            with self.assertRaises(FileExistsError):
                prepare(source, root / "parts", 100_000, root / "profile.json")

    def test_isolation_predecessor_and_job(self):
        for args in ((100_000, "r01", ROOT + "/m100k/manifest.json"),
                     (1_000_000, "bulk_1m_r001", ROOT + "/m1m/manifest.json"),
                     (100_000, "bulk_100k_r001", "/Volumes/workspace/twitch_lakehouse/source.json")):
            with self.assertRaises(ValueError):
                Config(*args)
        cfg = Config(100_000, "bulk_100k_r001", ROOT + "/m100k/manifest.json")
        self.assertEqual(cfg.table("gold"), NS + ".gold_bulk_100k_r001")
        job = settings("a" * 40)
        for i, task in enumerate(job["tasks"]):
            self.assertEqual(task["max_retries"], 0)
            self.assertEqual(task["run_if"], "ALL_SUCCESS")
            self.assertEqual(len(task["depends_on"]), int(i > 0))
            self.assertNotIn("new_cluster", task)

    def test_logical_ranges_reuse_prefix_without_small_table_explosion(self):
        manifest = {"parts": [{"first_row": i*100000+1, "last_row": (i+1)*100000, "rows": 100000} for i in range(500)]}
        grouped = blocks(manifest)
        self.assertEqual(len(grouped), 51)
        self.assertEqual([sum(p["rows"] for p in b) for b in grouped[:3]], [100000, 900000, 1000000])
        self.assertEqual(grouped[-1][-1]["last_row"], 50_000_000)


if __name__ == "__main__":
    unittest.main()
