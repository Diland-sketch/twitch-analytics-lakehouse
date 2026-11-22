"""Contratos de manifiesto y aislamiento 10M. No es E2E de Databricks."""
import copy
import unittest
from twitch_lakehouse.full_dataset_10m import (
    ROWS, BATCHES, settings, manifest_checked, batch_table, expected_parts,
)


class FullTenMillionContracts(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "schema_version":"twitch-fragments-v1",
            "source_sha256":"a"*64,
            "requested_rows":ROWS, "extracted_rows":ROWS,
            "parts":[
                dict(file=f"part-{i:06d}.csv", first_row=i*100_000+1,
                     last_row=(i+1)*100_000, rows=100_000, bytes=100_000,
                     sha256="b"*64)
                for i in range(100)
            ]
        }

    def test_valid(self):
        self.assertEqual(len(manifest_checked(self.manifest)["parts"]), 100)

    def test_isolated_tables(self):
        cfg = settings()
        self.assertEqual(cfg.namespace, "workspace.twitch_full_10m_experimental")
        self.assertEqual(batch_table(cfg, 0), cfg.table("bronze_batch_000_10m"))
        self.assertEqual(batch_table(cfg, 9), cfg.table("bronze_batch_009_10m"))
        self.assertEqual(len(expected_parts(self.manifest,9)),10)

    def test_wrong_population(self):
        m=copy.deepcopy(self.manifest);m["extracted_rows"]=9_999_999
        with self.assertRaises(ValueError):manifest_checked(m)

    def test_wrong_part_count(self):
        m=copy.deepcopy(self.manifest);m["parts"].pop()
        with self.assertRaises(ValueError):manifest_checked(m)

    def test_wrong_part_size(self):
        m=copy.deepcopy(self.manifest);m["parts"][0]["rows"]=99_999
        with self.assertRaises(ValueError):manifest_checked(m)

    def test_traversal_disallowed(self):
        m=copy.deepcopy(self.manifest);m["parts"][3]["file"]="../escape.csv"
        with self.assertRaises(ValueError):manifest_checked(m)

    def test_bad_batch_index(self):
        cfg=settings()
        for value in (-1, BATCHES, "1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                batch_table(cfg, value)


if __name__=="__main__":
    unittest.main()
