# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

import time
from twitch_lakehouse.full_dataset_10m import (
    settings, manifest_checked, ingest_batch, verify_all_batches,
)
full10 = settings()
manifest = manifest_checked(dbutils.fs.head(full10.root + "/manifest.json", 4 * 1024 * 1024))
print({"source_sha256": manifest["source_sha256"], "parts": len(manifest["parts"]),
       "rows": manifest["extracted_rows"]})

# Cada tabla Bronze es un checkpoint persistido; lotes existentes se verifican
# antes de reutilizar. No ejecutar varias copias del notebook simultáneamente.
reports = []
for index in range(10):
    started = time.perf_counter()
    report = ingest_batch(spark, full10, manifest, index)
    report["elapsed_seconds"] = round(time.perf_counter() - started, 2)
    print(report, flush=True)
    reports.append(report)
print({"status": "ALL_BRONZE_VERIFIED",
       "bronze_rows": sum(r["rows"] for r in verify_all_batches(spark, full10, manifest)),
       "elapsed_seconds_by_batch": [r["elapsed_seconds"] for r in reports]})
