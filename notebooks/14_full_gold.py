# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from twitch_lakehouse.full_dataset_pipeline import FullSettings, gold
full_cfg=FullSettings()
print(gold(spark,full_cfg))
display(spark.table(full_cfg.table("gold_streamer_metrics_1m")).orderBy("interactions",ascending=False).limit(20))
