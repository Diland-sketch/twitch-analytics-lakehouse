# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
from twitch_lakehouse.pipeline import build_gold
print(build_gold(spark, cfg))
display(spark.table(cfg.table("gold_streamer_metrics"))
        .orderBy("estimated_minutes", ascending=False).limit(20))
