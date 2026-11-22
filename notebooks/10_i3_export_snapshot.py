# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# Exportación de SOLO métricas agregadas I3, sin credenciales ni filas originales.
# Ejecutar sobre release publicada y verificada con notebook 09.
from datetime import datetime, timezone
import json
from pyspark.sql import functions as F
from twitch_lakehouse.i3_preflight import verify_release

release = "r01"
report = verify_release(spark, cfg, release)
tables = report["tables"]
q = report["quality"]

# COMMAND ----------

ranking = (spark.table(tables["streamer"])
    .orderBy(F.desc("interactions"), F.asc("streamer_username"))
    .limit(20)
    .select("streamer_username", "interactions", "unique_users",
            "unique_streams", "recurring_users", "recurrence_rate",
            "estimated_minutes").collect())
ranked = spark.sql(f"""
SELECT streamer_username, interactions,
       ROW_NUMBER() OVER (ORDER BY interactions DESC, streamer_username ASC) AS position
FROM {tables['streamer']}
""")
concentration = ranked.agg(
    F.sum("interactions").alias("n"),
    F.sum(F.when(F.col("position") <= 10, F.col("interactions")).otherwise(0)).alias("n10"),
    F.sum(F.when(F.col("position") <= 100, F.col("interactions")).otherwise(0)).alias("n100")
).first()
if concentration["n"] != q["silver_rows"]:
    raise ValueError("Concentración no concilia con Silver")
duration = (spark.table(tables["duration"])
    .groupBy("duration_bucket").agg(F.sum("interactions").alias("interactions"))
    .orderBy("duration_bucket").collect())
duration_map = {int(r["duration_bucket"]): int(r["interactions"]) for r in duration}
if set(duration_map) - set(range(5)):
    raise ValueError("Bucket inesperado")
if sum(duration_map.values()) != q["silver_rows"]:
    raise ValueError("Distribución no concilia con Silver")

payload = {
    "schema_version": "i3-historical-v1",
    "status": "PASS",
    "release": release,
    "source_namespace": cfg.namespace,
    "exported_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "scope": "historical_chat_connections_not_live_viewers",
    "metrics": {
        "bronze_rows": int(q["bronze_rows"]),
        "silver_rows": int(q["silver_rows"]),
        "rejected_rows": int(q["rejected_rows"]),
        "gold_streamers": int(q["gold_streamers"]),
        "estimated_minutes": int(q["estimated_minutes"]),
        "top10_share": float(concentration["n10"] / concentration["n"]),
        "top100_share": float(concentration["n100"] / concentration["n"])
    },
    "ranking": [r.asDict(recursive=True) for r in ranking],
    "duration": [{"duration_bucket": i, "interactions": duration_map.get(i, 0)}
                 for i in range(5)]
}
content = json.dumps(payload, ensure_ascii=False, allow_nan=False, sort_keys=True)
print("Release", release, "Tamaño JSON", len(content.encode("utf-8")))
print("Totales verificados", payload["metrics"])

# COMMAND ----------

# Volumen independiente del CSV original. El archivo nunca se sobrescribe.
# Si ya existe, elegir otro nombre, conservando la versión anterior.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.namespace}.historical_exports")
target = f"/Volumes/{cfg.catalog}/{cfg.schema}/historical_exports/historical_{release}.json"
dbutils.fs.put(target, content, overwrite=False)
print("Exportación lista para descargar:", target)
