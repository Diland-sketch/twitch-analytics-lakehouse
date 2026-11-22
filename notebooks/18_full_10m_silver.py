# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

import time
from twitch_lakehouse.full_dataset_10m import settings, manifest_checked, build_silver
full10 = settings()
manifest = manifest_checked(dbutils.fs.head(full10.root + "/manifest.json", 4 * 1024 * 1024))
started=time.perf_counter()
metrics=build_silver(spark, full10, manifest)
print({"metrics": metrics, "elapsed_seconds": round(time.perf_counter()-started, 2)})
