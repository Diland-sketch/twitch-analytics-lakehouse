# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
from twitch_lakehouse.pipeline import build_silver
print(build_silver(spark, cfg))
display(spark.table(cfg.table("silver_rejects")).groupBy("quality_errors").count())
display(spark.table(cfg.table("silver_interactions")).limit(10))
