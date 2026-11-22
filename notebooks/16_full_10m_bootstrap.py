# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from twitch_lakehouse.full_dataset_10m import settings, bootstrap_volume
full10 = settings()
print({"namespace": full10.namespace, "volume": bootstrap_volume(spark, full10),
       "required": "100 part-*.csv + manifest.json"})
