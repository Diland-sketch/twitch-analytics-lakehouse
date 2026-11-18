# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
# Crea solamente objetos ausentes. No crea compute ni servicios externos.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.namespace}.{cfg.volume}")
print({"upload_to": cfg.raw_path, "status": "ready_for_manual_upload"})
