# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
# Actualizar Git y reiniciar Python ANTES de ejecutar este notebook (ver guía).
for name in ("bronze_interactions", "silver_interactions", "silver_rejects", "gold_streamer_metrics"):
    table = cfg.table(name)
    print(table)
    spark.table(table).printSchema()
    display(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1"))

# COMMAND ----------
from twitch_lakehouse.analytics_pipeline import build_analytics
release = "r01"  # Nuevo identificador por ejecución; nunca reutilizar tras un fallo.
result = build_analytics(spark, cfg, release)
print(result)
display(spark.table(result["tables"]["quality"]))
