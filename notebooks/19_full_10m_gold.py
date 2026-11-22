# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

import time
from twitch_lakehouse.full_dataset_10m import settings, build_gold
full10=settings()
started=time.perf_counter()
print({"metrics": build_gold(spark,full10),
       "elapsed_seconds": round(time.perf_counter()-started, 2)})
