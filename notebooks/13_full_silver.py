# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from twitch_lakehouse.full_dataset_pipeline import FullSettings, silver
full_cfg=FullSettings()
print(silver(spark,full_cfg))
display(spark.table(full_cfg.table("silver_rejects_1m")).groupBy("quality_errors").count())
