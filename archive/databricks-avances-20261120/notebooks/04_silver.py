# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_config

# COMMAND ----------


import inspect
from twitch_lakehouse.pipeline import build_silver

print("=== IMPLEMENTACIÓN SILVER ===")
print(inspect.getsource(build_silver))

print("\n=== TABLAS DESTINO ===")

for nombre in ["silver_interactions", "silver_rejects"]:
    tabla = cfg.table(nombre)
    print(tabla, "Existe:", spark.catalog.tableExists(tabla))

print("\n=== BRONZE ===")
print(
    "Existe:",
    spark.catalog.tableExists(cfg.table("bronze_interactions"))
)


# COMMAND ----------


import inspect
from twitch_lakehouse import pipeline

print("=== REGLAS DE TRANSFORMACIÓN ===")
print(inspect.getsource(pipeline.silver_frames))

print("\n=== MÉTRICAS Y CONCILIACIÓN ===")
print(inspect.getsource(pipeline.silver_metrics))

print("\n=== ESTADO DE LAS TABLAS ===")
for nombre in [
    "bronze_interactions",
    "silver_interactions",
    "silver_rejects"
]:
    tabla = cfg.table(nombre)
    print(nombre, spark.catalog.tableExists(tabla))


# COMMAND ----------

from twitch_lakehouse.pipeline import build_silver
print(build_silver(spark, cfg))
display(spark.table(cfg.table("silver_rejects")).groupBy("quality_errors").count())
display(spark.table(cfg.table("silver_interactions")).limit(10))