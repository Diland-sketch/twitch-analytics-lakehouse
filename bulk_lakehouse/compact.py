"""Seven fixed Delta tables. Immutable records and append-only release partitions."""
import hashlib
import json
import re
import time
from dataclasses import dataclass

from kafka_ingest.contracts import canonical, digest
from bulk_lakehouse import pipeline as old

NS = "workspace.twitch_bulk_consolidated"
TABLES = ("bronze", "silver", "gold", "rejects", "manifests", "audits", "publications")
MANIFEST_SHA = "17595c746c42e62910480c0574bbfe948c6fb713b22d25833343112d4442d6d0"
STAGES = ("inventory", "bronze", "silver", "gold", "publish")
ROWS = 50000000


@dataclass(frozen=True)
class Config:
    release: str = "bulk_50m_c001"
    def __post_init__(self):
        if not re.fullmatch(r"bulk_50m_c[0-9]{3}", self.release):
            raise ValueError("Only a new consolidated 50M release is allowed")
    def table(self, name):
        if name not in TABLES:
            raise ValueError("Table outside fixed consolidated model")
        return NS + "." + name


def ensure(spark, df, table, partitions=()):
    if table not in [NS+"."+n for n in TABLES]:
        raise ValueError("Destination outside consolidated namespace")
    fields=", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    suffix=" PARTITIONED BY ("+", ".join(partitions)+")" if partitions else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({fields}) USING DELTA{suffix}").collect()
    if ({f.name:f.dataType.simpleString() for f in spark.table(table).schema.fields}
        !={f.name:f.dataType.simpleString() for f in df.schema.fields}):
        raise ValueError("Existing consolidated schema differs")


def append(spark, df, table, token, partitions=()):
    ensure(spark,df,table,partitions)
    (df.select(*spark.table(table).columns).write.format("delta").mode("append").option("txnAppId", "compact-"+digest(token))
        .option("txnVersion",1).option("userMetadata",canonical({"token":token}))
        .saveAsTable(table))


def record(spark, cfg, kind, key, report=None):
    from pyspark.sql import functions as F
    table=cfg.table(kind)
    rows=[]
    if spark.catalog.tableExists(table):
        rows=spark.table(table).filter(F.col("record_key")==key).select("report_json").limit(2).collect()
    if len(rows)>1:
        raise ValueError("Duplicate control identity")
    found=json.loads(rows[0][0]) if rows else None
    if report is None:
        if found is None: raise ValueError("Required control missing: "+key)
        return found
    if found is not None:
        if found!=report: raise ValueError("Immutable control differs: "+key)
        return found
    df=spark.createDataFrame([(key,canonical(report))],"record_key STRING, report_json STRING")
    append(spark,df,table,table+"/"+key)
    return record(spark,cfg,kind,key)


def source(spark):
    cfg=old.Config(50000000,"bulk_50m_r001",old.ROOT+"/m50m/manifest.json",
        old.NS+".publication_bulk_10m_r001",6442450944)
    raw=spark.read.text(cfg.manifest_path,wholetext=True).first()[0]
    if hashlib.sha256(raw.encode()).hexdigest()!=MANIFEST_SHA:
        raise ValueError("Approved manifest changed")
    m=old.gate(spark,cfg)
    ledger=old.read_record(spark,cfg.table("bronze_ledger"))
    if ledger["status"]!="PASS" or ledger["rows"]!=50000000 or ledger["config_sha256"]!=cfg.fingerprint:
        raise ValueError("The existing 50M Bronze is not certified")
    expected=old.blocks(m)
    if len(ledger["receipts"])!=len(expected): raise ValueError("Missing source blocks")
    for r,parts in zip(ledger["receipts"],expected):
        if (r["status"]!="PASS" or r["input"]["parts"]!=parts
            or r["input"]["table"]!=old.block_table(m,parts)
            or r["input"]["source_sha256"]!=m["source_sha256"]
            or r["input"]["first_row"]!=parts[0]["first_row"]
            or r["input"]["last_row"]!=parts[-1]["last_row"]
            or r["stats"]["rows"]!=sum(p["rows"] for p in parts)
            or r["stats"]["first_row"]!=parts[0]["first_row"] or r["stats"]["last_row"]!=parts[-1]["last_row"]
            or r["stats"]["distinct_rows"]!=r["stats"]["rows"] or r["stats"]["hash_errors"]!=0):
            raise ValueError("Invalid existing source receipt")
    return m,ledger


def inventory(spark,cfg):
    m,ledger=source(spark)
    entries=[]
    for r in ledger["receipts"]:
        table=r["input"]["table"]
        if old.current_version(spark,table)!=r["delta_version"]:
            raise ValueError("Certified source version changed: "+table)
        entries.append({"table":table,"version":r["delta_version"],"rows":r["stats"]["rows"],
            "first_row":r["input"]["first_row"],"last_row":r["input"]["last_row"]})
    objects=[]
    for row in spark.sql("SHOW TABLES IN "+old.NS).collect():
        name=row["tableName"]
        kind="bronze_block" if name.startswith("bronze_2475") else "block_receipt" if name.startswith("receipt_") else "release_or_control"
        objects.append({"table":old.NS+"."+name,"kind":kind,"deletion_authorized":False})
    partial={}
    for name in ("silver_bulk_50m_r001","rejects_bulk_50m_r001"):
        table=old.NS+"."+name
        if spark.catalog.tableExists(table):
            partial[table]={"rows":spark.table(table).count(),"version":old.current_version(spark,table)}
    spark.sql("CREATE SCHEMA IF NOT EXISTS "+NS).collect()
    record(spark,cfg,"manifests",MANIFEST_SHA,{"manifest_sha256":MANIFEST_SHA,"manifest":m})
    report={"status":"PASS","rows":50000000,"objects":objects,"partial_objects":partial,
        "source_blocks":entries,"ledger":ledger,"source_sha256":m["source_sha256"],
        "manifest_sha256":MANIFEST_SHA,"old_namespace_read_only":True}
    record(spark,cfg,"audits","inventory/"+cfg.release,report)
    return {"status":"PASS","objects":len(objects),"certified_blocks":len(entries),"rows":50000000,"partial_objects":partial}


def audit(spark,cfg,stage,value=None):
    return record(spark,cfg,"audits",stage+"/"+cfg.release,value)


def tagged(spark,cfg,name):
    from pyspark.sql import functions as F
    return spark.table(cfg.table(name)).filter(F.col("release_id")==cfg.release).drop("release_id")


def bronze(spark,cfg):
    from pyspark.sql import functions as F
    inv=audit(spark,cfg,"inventory")
    if inv["status"]!="PASS": raise ValueError("Inventory not authorized")
    table=cfg.table("bronze")
    migrated=0
    for entry in inv["source_blocks"]:
        bid=f"{entry['first_row']}_{entry['last_row']}"
        key="block/"+inv["source_sha256"]+"/"+bid
        expected={"status":"PASS","source":entry,"source_sha256":inv["source_sha256"],"block_id":bid}
        if spark.catalog.tableExists(cfg.table("audits")):
            hit=spark.table(cfg.table("audits")).filter(F.col("record_key")==key).count()
            if hit:
                record(spark,cfg,"audits",key,expected)
                migrated+=entry["rows"]
                continue
        if old.current_version(spark,entry["table"])!=entry["version"]:
            raise ValueError("Source changed after inventory")
        df=(spark.read.option("versionAsOf",entry["version"]).table(entry["table"])
            .withColumn("block_id",F.lit(bid)).withColumn("origin_table",F.lit(entry["table"]))
            .withColumn("origin_version",F.lit(entry["version"]).cast("long")))
        ensure(spark,df,table,("source_sha256","block_id"))
        persisted=spark.table(table).filter((F.col("source_sha256")==inv["source_sha256"]) & (F.col("block_id")==bid))
        before=persisted.count()
        if before==0:
            append(spark,df.repartition(4),table,key,("source_sha256","block_id"))
            persisted=spark.table(table).filter((F.col("source_sha256")==inv["source_sha256"]) & (F.col("block_id")==bid))
        elif before!=entry["rows"]:
            raise ValueError("Partial consolidated block; preserve and stop")
        stats=old.block_stats(persisted)
        if (stats["rows"],stats["distinct_rows"],stats["first_row"],stats["last_row"],stats["hash_errors"])!=(entry["rows"],entry["rows"],entry["first_row"],entry["last_row"],0):
            raise ValueError("Consolidated block reconciliation failed")
        record(spark,cfg,"audits",key,expected)
        migrated+=entry["rows"]
        print(canonical({"stage":"bronze","certified_rows":migrated,"block_id":bid}),flush=True)
    actual=spark.table(table).filter(F.col("source_sha256")==inv["source_sha256"]).count()
    if actual!=ROWS or migrated!=actual: raise ValueError("Bronze total differs")
    result={"status":"PASS","rows":actual,"blocks":len(inv["source_blocks"]),"delta_version":old.current_version(spark,table)}
    audit(spark,cfg,"bronze",result)
    return result


def persist_release(spark,cfg,name,df):
    from pyspark.sql import functions as F
    table=cfg.table(name)
    frame=df.withColumn("release_id",F.lit(cfg.release))
    ensure(spark,frame,table,("release_id",))
    prior=tagged(spark,cfg,name)
    count=prior.count()
    if count:
        if prior.exceptAll(df).limit(1).count() or df.exceptAll(prior).limit(1).count():
            raise ValueError("Existing release content differs; no replacement")
    else:
        append(spark,frame,table,name+"/"+cfg.release+"/"+MANIFEST_SHA,("release_id",))


def silver(spark,cfg):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import silver_frames
    b=audit(spark,cfg,"bronze")
    if b["status"]!="PASS": raise ValueError("Bronze not authorized")
    inv=audit(spark,cfg,"inventory")
    src=spark.read.option("versionAsOf",b["delta_version"]).table(cfg.table("bronze")).filter(F.col("source_sha256")==inv["source_sha256"])
    accepted,rejected,valid=silver_frames(src)
    persist_release(spark,cfg,"rejects",rejected)
    persist_release(spark,cfg,"silver",accepted)
    rejects=tagged(spark,cfg,"rejects").count()
    nvalid=valid.count()
    nsilver=tagged(spark,cfg,"silver").count()
    if nvalid+rejects!=b["rows"] or nsilver>nvalid: raise ValueError("Silver balance failed")
    result={"status":"PASS","rows_read":b["rows"],"valid_rows":nvalid,"rejected_rows":rejects,
        "business_duplicates":nvalid-nsilver,"silver_rows":nsilver,
        "silver_version":old.current_version(spark,cfg.table("silver")),"rejects_version":old.current_version(spark,cfg.table("rejects"))}
    audit(spark,cfg,"silver",result)
    return result


def gold(spark,cfg):
    from twitch_lakehouse.transforms import gold_by_streamer
    s=audit(spark,cfg,"silver")
    if s["status"]!="PASS": raise ValueError("Silver not authorized")
    persist_release(spark,cfg,"gold",gold_by_streamer(tagged(spark,cfg,"silver")))
    report={"status":"PASS","streamers":tagged(spark,cfg,"gold").count(),"gold_version":old.current_version(spark,cfg.table("gold"))}
    audit(spark,cfg,"gold",report)
    return report


def publish(spark,cfg):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import KEY, gold_by_streamer
    b,s,g=[audit(spark,cfg,k) for k in ("bronze","silver","gold")]
    if any(x["status"]!="PASS" for x in (b,s,g)): raise ValueError("Stages not authorized")
    for name,version in (("bronze",b["delta_version"]),("silver",s["silver_version"]),("rejects",s["rejects_version"]),("gold",g["gold_version"])):
        if old.current_version(spark,cfg.table(name))!=version: raise ValueError("A table changed after audit")
    accepted,actual=tagged(spark,cfg,"silver"),tagged(spark,cfg,"gold")
    totals=actual.agg(F.sum("interactions"),F.sum("estimated_minutes")).first()
    minutes=accepted.agg(F.sum("estimated_minutes")).first()[0]
    expected=gold_by_streamer(accepted)
    checks={"source_balance":s["valid_rows"]+s["rejected_rows"]==b["rows"]==ROWS,
        "silver_balance":s["silver_rows"]+s["business_duplicates"]==s["valid_rows"],
        "silver_unique_keys":accepted.select(*KEY).distinct().count()==s["silver_rows"],
        "rejects_persisted":tagged(spark,cfg,"rejects").count()==s["rejected_rows"],
        "gold_interactions":totals[0]==s["silver_rows"],"gold_minutes":totals[1]==minutes,
        "gold_exact":not actual.exceptAll(expected).limit(1).count() and not expected.exceptAll(actual).limit(1).count()}
    inv=audit(spark,cfg,"inventory")
    versions_unchanged=all(old.current_version(spark,x["table"])==x["version"] for x in inv["source_blocks"])
    if not versions_unchanged: raise ValueError("Legacy source changed; do not publish")
    report={"status":"PASS" if all(checks.values()) else "FAIL","publication_state":"CONFIRMED" if all(checks.values()) else "FAILED",
        "release":cfg.release,"route":"SPARK_BATCH_CONSOLIDATED_NOT_KAFKA","rows":b["rows"],"bronze":b,"silver":s,
        "gold_streamers":g["streamers"],"gold_interactions":totals[0],"estimated_minutes":minutes,"checks":checks,
        "source_sha256":inv["source_sha256"],"manifest_sha256":MANIFEST_SHA,"legacy_versions_unchanged":versions_unchanged,
        "delta_details":{name:spark.sql("DESCRIBE DETAIL "+cfg.table(name)).select("sizeInBytes","numFiles").first().asDict() for name in ("bronze","silver","gold","rejects")},
        "fixed_tables":list(TABLES),"cleanup_executed":False}
    record(spark,cfg,"publications",cfg.release,report)
    if report["status"]!="PASS": raise ValueError("Publication failed; evidence preserved")
    return report


def execute(spark,cfg,stage):
    if stage not in STAGES: raise ValueError("Invalid stage")
    start=time.monotonic()
    result={"inventory":inventory,"bronze":bronze,"silver":silver,"gold":gold,"publish":publish}[stage](spark,cfg)
    return {**result,"elapsed_seconds":time.monotonic()-start}
