"""Fixtures only: prefix reuse and local validation never credit remote processing."""
import copy
import gzip
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from bulk_lakehouse import pipeline as p
from bulk_lakehouse.preflight import validate
from bulk_lakehouse.source import inspect, prepare


class PreflightTests(unittest.TestCase):
    def test_local_ordinals_line_hashes_and_footer_tamper(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            source=root/"fixture.gz"
            with gzip.open(source,"wt") as f:
                for i in range(100000):
                    f.write(f"{i+1},1,fixture,0,2\n")
            inspect(source,root/"profile.json")
            prepare(source,root/"parts",100000,root/"profile.json")
            report=validate(root/"parts",100000,source=source)
            self.assertEqual(report["rows_verified"],100000)
            self.assertTrue(report["source_gzip_hash_checked"])
            with (root/"parts/part-000000.parquet").open("ab") as f:
                f.write(b"tamper")
            with self.assertRaises(ValueError):
                validate(root/"parts",100000)

    def test_only_certified_100_parts_reused_version_and_hash_guards(self):
        parts=[{"file":f"part-{i:06d}.parquet","first_row":i*100000+1,
            "last_row":(i+1)*100000,"rows":100000,"bytes":1,"sha256":"b"*64} for i in range(500)]
        prior={"parts":parts[:100],"source_sha256":"a"*64}
        manifest={"parts":parts,"source_sha256":"a"*64}
        pc=p.Config(10000000,"bulk_10m_r001",p.ROOT+"/m10m/manifest.json",p.NS+".publication_bulk_1m_r001",1342177280)
        cfg=p.Config(50000000,"bulk_50m_r001",p.ROOT+"/m50m/manifest.json",pc.table("publication"),6442450944)
        reports={pc.table("publication"):{"config":p.asdict(pc),"status":"PASS","publication_state":"CONFIRMED",
            "source_sha256":"a"*64,"checks":{str(i):True for i in range(7)}}}
        receipts=[]
        for block in p.blocks(prior):
            table=p.block_table(prior,block)
            rows=sum(x["rows"] for x in block)
            saved={"status":"PASS","delta_version":1,"input":{"source_sha256":"a"*64,"parts":block,
                "first_row":block[0]["first_row"],"last_row":block[-1]["last_row"],"rows":rows,"table":table},
                "stats":{"rows":rows,"distinct_rows":rows,"first_row":block[0]["first_row"],
                    "last_row":block[-1]["last_row"],"hash_errors":0}}
            receipts.append(saved)
            reports[p.receipt_table(table)]=saved
        reports[pc.table("bronze_ledger")]={"status":"PASS","config_sha256":pc.fingerprint,"receipts":receipts}
        with patch.object(p,"read_record",side_effect=lambda _,name:reports[name]),patch.object(p,"gate",return_value=prior),patch.object(p,"current_version",return_value=1):
            paths, reuse=p.input_locations(None,cfg,manifest)
            self.assertEqual(reuse["bronze_blocks"],11)
            self.assertEqual(sum("/m10m/" in x for x in paths.values()),100)
            self.assertEqual(sum("/m50m/" in x for x in paths.values()),400)
            bad=copy.deepcopy(manifest)
            bad["parts"][0]["sha256"]="c"*64
            with self.assertRaises(ValueError):
                p.input_locations(None,cfg,bad)
            with patch.object(p,"current_version",return_value=2),self.assertRaises(ValueError):
                p.input_locations(None,cfg,manifest)


if __name__ == "__main__":
    unittest.main()
