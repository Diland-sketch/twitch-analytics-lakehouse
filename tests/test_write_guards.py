"""Prueba la protección real usando dobles; no simula procesamiento Spark."""
import unittest
from types import SimpleNamespace
from twitch_lakehouse.storage import require_absent, write_new


class WriteGuardTests(unittest.TestCase):
    def test_existing_destination_blocks(self):
        spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda _: True))
        with self.assertRaises(RuntimeError):
            require_absent(spark, "workspace.demo.silver_interactions")

    def test_new_destinations_pass(self):
        spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda _: False))
        require_absent(spark, "workspace.demo.silver", "workspace.demo.rejects")

    def test_second_existing_destination_also_blocks(self):
        spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda t: t.endswith("rejects")))
        with self.assertRaises(RuntimeError):
            require_absent(spark, "workspace.demo.silver", "workspace.demo.rejects")

    def test_writer_never_overwrites(self):
        class Writer:
            def format(self, value):
                self.format_value = value
                return self
            def mode(self, value):
                self.mode_value = value
                return self
            def saveAsTable(self, value):
                self.table = value
        writer = Writer()
        write_new(SimpleNamespace(write=writer), "workspace.demo.target")
        self.assertEqual((writer.format_value, writer.mode_value, writer.table),
                         ("delta", "errorifexists", "workspace.demo.target"))
