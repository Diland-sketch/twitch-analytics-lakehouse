# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
from twitch_lakehouse.pipeline import ingest_bronze
print(ingest_bronze(spark, cfg))
display(spark.table(cfg.table("bronze_interactions")).limit(10))
