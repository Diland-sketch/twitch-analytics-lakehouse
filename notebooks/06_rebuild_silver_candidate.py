# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
# Nunca reemplaza ni borra Silver. No forma parte del recorrido normal I1.
from twitch_lakehouse.pipeline import build_silver, silver_metrics
from twitch_lakehouse.transforms import silver_frames
bronze = spark.table(cfg.table("bronze_interactions"))
candidate, rejects, valid = silver_frames(bronze)
print(silver_metrics(bronze, candidate, rejects, valid))
display(candidate.limit(10))

# COMMAND ----------
# Vista previa por defecto. Para guardar, especificar un sufijo nuevo, por ejemplo revision_01.
dbutils.widgets.text("candidate_suffix", "")
suffix = dbutils.widgets.get("candidate_suffix")
if suffix:
    print(build_silver(spark, cfg, candidate_suffix=suffix))
else:
    print("Solo vista previa. Ninguna tabla candidata escrita.")
