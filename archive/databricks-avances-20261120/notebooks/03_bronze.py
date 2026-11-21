# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_config

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

tabla = "workspace.twitch_lakehouse.bronze_interactions"

print("¿Existe Bronze?", spark.catalog.tableExists(tabla))

spark.sql("""
SHOW TABLES IN workspace.twitch_lakehouse
""").show(truncate=False)

# COMMAND ----------

spark.catalog.tableExists(
    "workspace.twitch_lakehouse.bronze_interactions"
)

# COMMAND ----------

from twitch_lakehouse.pipeline import ingest_bronze
print(ingest_bronze(spark, cfg))
display(spark.table(cfg.table("bronze_interactions")).limit(10))

# COMMAND ----------


tabla = cfg.table("bronze_interactions")

spark.sql(f"DESCRIBE DETAIL {tabla}") \
    .select(
        "format",
        "numFiles",
        "sizeInBytes"
    ).show(truncate=False)
