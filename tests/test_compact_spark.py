"""Real Spark fixture, fake atomic Delta transactions; never production evidence."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from pyspark.sql import SparkSession,functions as F
from bulk_lakehouse import compact as c


class CompactSparkTests(unittest.TestCase):
    def test_migration_recovery_quality_and_seven_controls(self):
        spark=SparkSession.builder.master("local[2]").appName("compact-fixture").config("spark.sql.shuffle.partitions","2").getOrCreate()
        self.addCleanup(spark.stop)
        tables,versions,txns={},{},set()
        class Reader:
            def option(self,*_): return self
            def table(self,name): return tables[name]
        fake=SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda n:n in tables),
            createDataFrame=spark.createDataFrame,read=Reader(),table=lambda n:tables[n],
            sql=lambda _:spark.sql("SELECT 1000L AS sizeInBytes, 1 AS numFiles"))
        def ensure(_,df,table,partitions=()):
            if table not in tables: tables[table]=df.limit(0);versions[table]=0
        def append(_,df,table,key,partitions=()):
            ensure(fake,df,table,partitions)
            if (table,key) not in txns:
                frame=tables[table].unionByName(df).cache()
                frame.count()
                tables[table]=frame
                versions[table]+=1
                txns.add((table,key))
        entries=[]
        for start,stop in ((1,500),(501,1000)):
            name=f"legacy.block_{start}"
            df=spark.range(start,stop+1).select(F.col("id").alias("source_row"),
                F.concat(F.when(F.col("id")%500==0,F.lit(0)).otherwise(F.col("id")).cast("string"),F.lit(",11,fixture,0,2")).alias("raw_line"))
            tables[name]=df.withColumn("raw_sha256",F.sha2("raw_line",256)).withColumn("source_sha256",F.lit("a"*64)).withColumn("event_id",F.sha2(F.col("source_row").cast("string"),256))
            versions[name]=1
            entries.append({"table":name,"version":1,"rows":500,"first_row":start,"last_row":stop})
        inv={"status":"PASS","source_blocks":entries,"source_sha256":"a"*64}
        cfg=c.Config()
        with patch.object(c,"ROWS",1000),patch.object(c,"ensure",side_effect=ensure),patch.object(c,"append",side_effect=append),patch.object(c.old,"current_version",side_effect=lambda _,t:versions[t]):
            c.record(fake,cfg,"manifests",c.MANIFEST_SHA,{"fixture":True})
            c.audit(fake,cfg,"inventory",inv)
            original=c.record
            once=[True]
            def interrupt(s,cf,kind,key,report=None):
                if key.startswith("block/") and report and once[0]:
                    once[0]=False
                    raise RuntimeError("fixture crash after committed data, before receipt")
                return original(s,cf,kind,key,report)
            with patch.object(c,"record",side_effect=interrupt),self.assertRaises(RuntimeError):
                c.bronze(fake,cfg)
            self.assertEqual(tables[cfg.table("bronze")].count(),500)
            c.bronze(fake,cfg)
            n=versions[cfg.table("bronze")]
            c.bronze(fake,cfg)
            self.assertEqual(versions[cfg.table("bronze")],n)
            c.silver(fake,cfg)
            c.gold(fake,cfg)
            result=c.publish(fake,cfg)
            self.assertEqual(result["publication_state"],"CONFIRMED")
            self.assertEqual(result["silver"]["rejected_rows"],2)
            self.assertEqual(result["silver"]["silver_rows"],998)
            self.assertEqual(result["estimated_minutes"],19960)
            self.assertEqual(len(result["checks"]),7)
            self.assertTrue(all(result["checks"].values()))
            self.assertEqual(len([t for t in tables if t.startswith(c.NS+".")]),7)
            self.assertTrue(all(versions[e["table"]]==1 for e in entries))


if __name__=="__main__": unittest.main()
