"""Los tests solo usan GZIP artificial en carpetas temporales."""
import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from twitch_lakehouse.full_dataset_prepare import prepare


class FragmentPreparationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "full_a.csv.gz"
        self.lines = [b"1,2,a,0,1\n", b"1,3,b,1,3\r\n", b"invalid,row\n",
                      b"4,5,a,2,2"]
        with gzip.open(self.source, "wb") as output:
            output.write(b"".join(self.lines))

    def test_prefix_bytes_and_manifest(self):
        dst = self.root / "sample"
        manifest = prepare(self.source, dst, 3, 2)
        self.assertEqual([p["rows"] for p in manifest["parts"]], [2, 1])
        self.assertEqual(b"".join((dst / p["file"]).read_bytes()
                                  for p in manifest["parts"]), b"".join(self.lines[:3]))
        self.assertEqual(manifest["source_sha256"],
                         hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertEqual(json.loads((dst / "manifest.json").read_text())["extracted_rows"], 3)
        for part in manifest["parts"]:
            self.assertEqual(hashlib.sha256((dst / part["file"]).read_bytes()).hexdigest(),
                             part["sha256"])

    def test_last_line_without_newline(self):
        dst = self.root / "all"
        result = prepare(self.source, dst, 4, 3)
        self.assertEqual([p["rows"] for p in result["parts"]], [3, 1])
        self.assertEqual((dst / "part-000001.csv").read_bytes(), self.lines[3])

    def test_short_source_has_no_partial_published_output(self):
        dst = self.root / "short"
        with self.assertRaisesRegex(ValueError, "Se pidieron"):
            prepare(self.source, dst, 5, 2)
        self.assertFalse(dst.exists())
        self.assertFalse(list(self.root.glob(".pending-*")))

    def test_existing_destination_preserved(self):
        dst = self.root / "existing"
        dst.mkdir()
        (dst / "sentinel").write_text("unchanged")
        with self.assertRaises(FileExistsError):
            prepare(self.source, dst, 1, 1)
        self.assertEqual((dst / "sentinel").read_text(), "unchanged")

    def test_reject_bad_args(self):
        with self.assertRaises(ValueError):
            prepare(self.source, self.root / "bad", 0, 2)
        with self.assertRaises(ValueError):
            prepare(self.source, self.root / "bad", 1, 0)


if __name__ == "__main__":
    unittest.main()
