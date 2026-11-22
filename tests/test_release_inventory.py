import unittest
from types import SimpleNamespace
from twitch_lakehouse.config import Settings
from twitch_lakehouse.release_inventory import inspect_release


class ReleaseInventoryTests(unittest.TestCase):
    def inspect(self, exists):
        calls = []
        def present(name):
            calls.append(name)
            return exists(name)
        spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=present))
        report = inspect_release(spark, Settings(), "r01")
        self.assertEqual(len(calls), 4)
        self.assertFalse(report["automatic_write_allowed"])
        return report

    def test_all_absent_still_never_authorizes_writing(self):
        self.assertEqual(self.inspect(lambda _: False)["state"], "ABSENT")

    def test_partial_release_must_not_be_retried_as_new(self):
        report = self.inspect(lambda name: "_quality_" not in name)
        self.assertEqual(report["state"], "PARTIAL")
        self.assertFalse(report["present"]["quality"])

    def test_complete_requires_quality_and_all_families(self):
        self.assertEqual(self.inspect(lambda _: True)["state"], "COMPLETE")

    def test_invalid_release_fails_before_catalog_access(self):
        with self.assertRaises(ValueError):
            inspect_release(SimpleNamespace(), Settings(), "r01; DROP")
