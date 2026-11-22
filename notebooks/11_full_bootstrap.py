# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from twitch_lakehouse.full_dataset_pipeline import FullSettings, bootstrap
full_cfg = FullSettings()
print({"namespace":full_cfg.namespace,"upload_to":bootstrap(spark,full_cfg)})
# Subir 10 part-*.csv + manifest.json al volumen indicado, antes de 12.
