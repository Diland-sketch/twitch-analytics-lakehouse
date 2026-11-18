import unittest
from twitch_lakehouse.config import Settings


class SettingsTests(unittest.TestCase):
    def test_volume_and_table_paths(self):
        cfg = Settings()
        self.assertEqual(cfg.raw_path, "/Volumes/workspace/twitch_lakehouse/raw_data/100k_a.csv")
        self.assertEqual(cfg.table("bronze_interactions"),
                         "workspace.twitch_lakehouse.bronze_interactions")

    def test_unsafe_identifiers_and_full_dataset_are_rejected(self):
        for kwargs in ({"schema": "a; DROP TABLE x"}, {"volume": "../raw"},
                       {"catalog": ""}, {"filename": "full_a.csv.gz"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Settings(**kwargs)
        with self.assertRaises(ValueError):
            Settings().table("silver_candidate_../../x")

    def test_isolated_schema_supported(self):
        self.assertEqual(Settings(schema="twitch_lakehouse_i1_retry").namespace,
                         "workspace.twitch_lakehouse_i1_retry")
