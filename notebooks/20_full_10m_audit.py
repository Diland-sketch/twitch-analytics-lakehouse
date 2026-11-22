# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from twitch_lakehouse.full_dataset_10m import settings, manifest_checked, audit
full10 = settings()
manifest=manifest_checked(dbutils.fs.head(full10.root+"/manifest.json",4*1024*1024))
print(audit(spark,full10,manifest))  # SOLO LECTURA; debe indicar PASS
