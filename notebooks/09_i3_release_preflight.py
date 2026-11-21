# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# Solo lectura. Ejecutar DESPUÉS de publicar I3 con 08_i3_analytics.py.
# Si la release es otra, cambiar este valor y el sufijo de las consultas SQL.
from twitch_lakehouse.i3_preflight import verify_release

release = "r01"
report = verify_release(spark, cfg, release)
print({
    "status": report["status"],
    "release": report["release"],
    "source_namespace": report["quality"]["source_namespace"],
    "source_versions": {k: report["quality"][k] for k in (
        "bronze_version", "silver_version", "rejects_version", "gold_version")},
    "bronze_rows": report["quality"]["bronze_rows"],
    "silver_rows": report["quality"]["silver_rows"],
    "rejected_rows": report["quality"]["rejected_rows"],
    "gold_streamers": report["quality"]["gold_streamers"],
    "estimated_minutes": report["quality"]["estimated_minutes"],
    "family_totals": report["family_totals"],
})
# PASS en este notebook NO sustituye la validación de la interfaz AI/BI.
