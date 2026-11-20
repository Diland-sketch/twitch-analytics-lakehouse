import tempfile
import unittest
from pathlib import Path
from live_monitor.common import event_id, source_digest


class IdentityTests(unittest.TestCase):
    def test_stable_identity_and_distinct_run_or_row(self):
        a = event_id("run1", "sha", 1)
        self.assertEqual(a, event_id("run1", "sha", 1))
        self.assertNotEqual(a, event_id("run2", "sha", 1))
        self.assertNotEqual(a, event_id("run1", "sha", 2))

    def test_file_content_changes_digest(self):
        with tempfile.TemporaryDirectory() as root:
            file = Path(root) / "fixture.txt"
            file.write_text("ARTIFICIAL1")
            before = source_digest(file)
            file.write_text("ARTIFICIAL2")
            self.assertNotEqual(before, source_digest(file))
