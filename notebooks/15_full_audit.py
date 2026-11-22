# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# SOLO LECTURA.
from twitch_lakehouse.full_dataset_pipeline import FullSettings, audit
full_cfg=FullSettings()
print(audit(spark,full_cfg))
