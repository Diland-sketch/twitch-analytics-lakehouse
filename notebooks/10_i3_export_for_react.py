"""Publicación de un snapshot histórico I3 verificado y acotado.

Databricks ejecuta este notebook. Exporta datos AGREGADOS, no eventos crudos,
a un archivo JSON para transferir al PC. No modifica tablas Gold.
"""
# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

import json
from datetime import datetime, timezone
from twitch_lakehouse.i3_preflight import verify_release
from pyspark.sql import functions as F

release = "r01"
check = verify_release(spark, cfg, release)
names = check["tables"]
quality = check["quality"]

# No sumar usuarios únicos entre streamers: no equivale a población única global.
streamers = (spark.table(names["streamer"])
    .orderBy(F.desc("interactions"), F.asc("streamer_username"))
    .select("streamer_username", "interactions", "unique_users", "unique_streams",
            "recurring_users", "recurrence_rate", "estimated_minutes")
    .limit(20).collect())

duration = (spark.table(names["duration"])
    .groupBy("duration_bucket")
    .agg(F.sum("interactions").alias("interactions"),
         F.sum("estimated_minutes").alias("estimated_minutes"))
    .orderBy("duration_bucket").collect())

# Intervalos nativos de 10 minutos, agrupados en ventanas relativas de 60 min.
# Máximo 120 buckets. Es una serie RECORTADA, no todos los pasos del dataset.
activity_frame = (spark.table(names["activity"])
    .withColumn("relative_step", F.floor(F.col("time_start") / F.lit(6)) * 6)
    .groupBy("relative_step")
    .agg(F.sum("connection_starts").alias("connection_starts"))
    .orderBy("relative_step"))
activity = activity_frame.limit(121).collect()
truncated = len(activity) > 120
activity = activity[:120]

payload = {
    "schema_version": "1.0",
    "kind": "i3_historical_aggregates",
    "release": release,
    "source_namespace": cfg.namespace,
    "exported_at": datetime.now(timezone.utc).isoformat(),
    "source_versions": {k: int(quality[k]) for k in (
        "bronze_version", "silver_version", "rejects_version", "gold_version")},
    "quality": {"status": quality["status"], "scope": quality["scope"],
                "bronze_rows": int(quality["bronze_rows"]),
                "silver_rows": int(quality["silver_rows"]),
                "rejected_rows": int(quality["rejected_rows"]),
                "duplicates_removed_inferred": int(quality["duplicates_removed_inferred"]),
                "silver_duplicate_keys": int(quality["silver_duplicate_keys"]),
                "gold_streamers": int(quality["gold_streamers"]),
                "estimated_minutes": int(quality["estimated_minutes"])},
    "top_streamers": [dict(s.asDict()) for s in streamers],
    "duration_buckets": [dict(r.asDict()) for r in duration],
    "activity": [{"relative_minutes": int(r["relative_step"]) * 10,
                  "connection_starts": int(r["connection_starts"])} for r in activity],
    "activity_truncated": truncated,
    "definitions": {
        "interaction": "Presencia observada en chat, no espectador activo comprobado",
        "estimated_minutes": "Pasos relativos de 10 minutos; no tiempo visto",
        "recurrence": "Usuarios con >=2 filas Silver por streamer / usuarios distintos del streamer; no retención"
    }
}

# Guardar fuera del checkout Git, dentro del volumen UC existente.
# No borra ni sobrescribe: aborta si el archivo ya existe.
out_dir = f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}/exports"
out_path = f"{out_dir}/i3_historical_{release}.json"
dbutils.fs.mkdirs(out_dir)
if any(f.path.rstrip("/") == out_path for f in dbutils.fs.ls(out_dir)):
    raise ValueError(f"Exportación ya existe; conservar y revisar antes de crear otra: {out_path}")
dbutils.fs.put(out_path, json.dumps(payload, ensure_ascii=False, allow_nan=False), overwrite=False)
print({"status": "EXPORT_READY", "path": out_path, "top_streamers": len(streamers),
       "duration_buckets": len(duration), "activity_points": len(activity),
       "activity_truncated": truncated, "release": release})
