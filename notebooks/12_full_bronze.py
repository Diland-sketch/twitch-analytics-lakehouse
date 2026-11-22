# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from twitch_lakehouse.full_dataset_pipeline import FullSettings, validate_manifest, bronze
full_cfg=FullSettings()
manifest=validate_manifest(dbutils.fs.head(full_cfg.root+"/manifest.json",2*1024*1024))
print({"source_sha256":manifest["source_sha256"],"fragments":len(manifest["parts"])})
print(bronze(spark,full_cfg,manifest))
