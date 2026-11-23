"""Spark real, entradas Parquet artificiales, fake Delta versionado: no evidencia remota."""
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from bulk_lakehouse import pipeline as p
from twitch_lakehouse.full_dataset_prepare import sha256


class BulkSparkTests(unittest.TestCase):
    def test_hundred_thousand_fixture_quality_resume_and_publish(self):
        from pyspark.sql import SparkSession, functions as F
        spark = SparkSession.builder.master("local[2]").appName("bulk-fixtures").config("spark.sql.shuffle.partitions", "2").getOrCreate()
        self.addCleanup(spark.stop)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        source = spark.range(1,100001).select(F.col("id").alias("source_row"),
            F.concat(F.when(F.col("id") % 1000 == 0, F.lit(0)).otherwise(F.col("id")).cast("string"),
                F.lit(",11,fixture,0,2")).alias("raw_line"))
        source = source.withColumn("raw_sha256", F.sha2("raw_line",256))
        source.coalesce(1).write.parquet(str(root / "staging"))
        folder = root / "m100k"
        folder.mkdir()
        target = folder / "part-000000.parquet"
        shutil.copyfile(next((root / "staging").glob("part-*.parquet")), target)
        m = {"schema_version": "twitch-bulk-parquet-v1", "source_sha256": "a"*64,
            "requested_rows":100000, "extracted_rows":100000,
            "parts":[{"file":target.name,"first_row":1,"last_row":100000,"rows":100000,"bytes":target.stat().st_size,"sha256":sha256(target)}]}
        (folder / "manifest.json").write_text(json.dumps(m))
        tables, versions, writes = {}, {}, []
        class Reader:
            def __init__(self):
                self.reader=spark.read
                self.version=None
            def option(self,key,value):
                if key=="versionAsOf": self.version=value
                else: self.reader=self.reader.option(key,value)
                return self
            def table(self,name):
                if self.version is not None and self.version != versions[name]: raise ValueError("Version mismatch")
                return tables[name]
            def __getattr__(self,name):
                return getattr(self.reader,name)
        class Fake:
            catalog=SimpleNamespace(tableExists=lambda name:name in tables)
            createDataFrame=spark.createDataFrame
            @property
            def read(self): return Reader()
            def table(self,name): return tables[name]
            def sql(self,query):
                if "HISTORY" in query: return spark.sql(f"SELECT {versions[query.split()[2]]}L AS version")
                return spark.sql("SELECT 1024L AS sizeInBytes, 1 AS numFiles")
        fake=Fake()
        def write(_,df,table):
            if table in tables: raise ValueError("Existing table")
            tables[table]=df
            versions[table]=1
            writes.append(table)
        with patch.object(p,"ROOT",str(root)), patch.object(p,"write_new",side_effect=write):
            cfg=p.Config(100000,"bulk_100k_r999",str(folder/"manifest.json"))
            for stage in p.STAGES:
                report=p.execute(fake,cfg,stage)
            self.assertEqual(report["publication_state"],"CONFIRMED")
            self.assertEqual(report["silver"]["rejected_rows"],100)
            self.assertEqual(report["silver"]["silver_rows"],99900)
            self.assertEqual(report["estimated_minutes"],1998000)
            previous=list(writes)
            p.bronze(fake,cfg)
            self.assertEqual(previous,writes)
            # Cambiar bytes de entrada invalida la verificación incluso con el mismo conteo.
            with target.open("ab") as out: out.write(b"changed")
            with self.assertRaises(ValueError): p.verify(fake,cfg)


if __name__=="__main__": unittest.main()
