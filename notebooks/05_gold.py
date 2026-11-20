# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_config

# COMMAND ----------


import inspect
from twitch_lakehouse import pipeline

print("=== REGLAS DE AGREGACIÓN GOLD ===")
print(inspect.getsource(pipeline.gold_by_streamer))

print("\n=== ESTADO DE GOLD ===")
tabla = cfg.table("gold_streamer_metrics")

print("Tabla:", tabla)
print("Existe:", spark.catalog.tableExists(tabla))


# COMMAND ----------


import inspect
from twitch_lakehouse import pipeline

print("=== IMPLEMENTACIÓN GOLD ===")
print(inspect.getsource(pipeline.build_gold))

print("\n=== TABLAS ACTUALES ===")
spark.sql("""
SHOW TABLES IN workspace.twitch_lakehouse
""").show(truncate=False)


# COMMAND ----------

from twitch_lakehouse.pipeline import build_gold
print(build_gold(spark, cfg))
display(spark.table(cfg.table("gold_streamer_metrics"))
        .orderBy("estimated_minutes", ascending=False).limit(20))

# COMMAND ----------


from pyspark.sql import functions as F

gold = spark.table(cfg.table("gold_streamer_metrics"))

print("=== TOP 10 STREAMERS POR INTERACCIONES ===")

display(
    gold.orderBy(F.desc("interactions")).limit(10)
)

print("=== VERIFICACIÓN GOLD ===")

gold.agg(
    F.sum("interactions").alias("total_interactions"),
    F.sum("estimated_minutes").alias("total_minutes")
).show()
