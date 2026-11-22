import copy
import unittest
from twitch_lakehouse.full_dataset_pipeline import FullSettings, validate_manifest

class FullExperimentalContractTests(unittest.TestCase):
    def setUp(self):
        self.manifest = dict(schema_version="twitch-fragments-v1",
            source_sha256="a"*64, requested_rows=1000000, extracted_rows=1000000,
            parts=[dict(file=f"part-{i:06d}.csv",rows=100000,
                first_row=i*100000+1,last_row=(i+1)*100000,
                bytes=100000,sha256="b"*64) for i in range(10)])

    def test_valid(self):
        self.assertEqual(validate_manifest(self.manifest)["extracted_rows"],1000000)

    def test_isolation(self):
        self.assertEqual(FullSettings().namespace,"workspace.twitch_full_experimental")
        with self.assertRaises(ValueError):
            FullSettings(schema="twitch_lakehouse")

    def test_reject_wrong_source_size(self):
        m=copy.deepcopy(self.manifest);m["extracted_rows"]=999999
        with self.assertRaises(ValueError):validate_manifest(m)

    def test_reject_unsafe_file(self):
        m=copy.deepcopy(self.manifest);m["parts"][0]["file"]="../escape.csv"
        with self.assertRaises(ValueError):validate_manifest(m)

    def test_reject_missing_part(self):
        m=copy.deepcopy(self.manifest);m["parts"].pop()
        with self.assertRaises(ValueError):validate_manifest(m)

    def test_reject_overlapping_ranges(self):
        m=copy.deepcopy(self.manifest);m["parts"][1]["first_row"]=2
        with self.assertRaises(ValueError):validate_manifest(m)

if __name__=="__main__":unittest.main()
