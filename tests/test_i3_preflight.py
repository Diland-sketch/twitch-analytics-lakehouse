import unittest
from twitch_lakehouse.i3_preflight import verify_counts, EXPECTED_SCOPE


def marker():
    return dict(status="PASS", scope=EXPECTED_SCOPE, bronze_rows=7,
                silver_rows=5, rejected_rows=1,
                duplicates_removed_inferred=1, silver_duplicate_keys=0,
                gold_streamers=2, estimated_minutes=120)


def families():
    return {"streamer": (5, 120), "duration": (5, 120), "activity": (5, 120)}


class I3PreflightTests(unittest.TestCase):
    def test_healthy_release(self):
        self.assertTrue(verify_counts(marker(), families()))

    def test_missing_family_fails(self):
        x = families()
        del x["activity"]
        with self.assertRaises(ValueError):
            verify_counts(marker(), x)

    def test_wrong_minutes_fails_even_if_rows_match(self):
        x = families()
        x["duration"] = (5, 119)
        with self.assertRaises(ValueError):
            verify_counts(marker(), x)

    def test_bad_quality_marker_fails(self):
        q = marker()
        q["status"] = "FAILED"
        with self.assertRaises(ValueError):
            verify_counts(q, families())

    def test_bronze_silver_difference_fails(self):
        q = marker()
        q["duplicates_removed_inferred"] = 0
        with self.assertRaises(ValueError):
            verify_counts(q, families())

    def test_null_totals_fail(self):
        x = families()
        x["streamer"] = (None, 120)
        with self.assertRaises(ValueError):
            verify_counts(marker(), x)

    def test_unexpected_extra_family_fails(self):
        x = families()
        x["additional"] = (5, 120)
        with self.assertRaises(ValueError):
            verify_counts(marker(), x)

    def test_quality_zero_duplicates_required(self):
        q = marker()
        q["silver_duplicate_keys"] = 2
        with self.assertRaises(ValueError):
            verify_counts(q, families())


if __name__ == "__main__":
    unittest.main()
