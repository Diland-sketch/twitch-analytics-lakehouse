"""Procesamiento experimental de 10M: 10 lotes Bronze y Silver/Gold globales.

No reconstruir Gold sumando agregados por lote: distintos y deduplicación
requieren una operación global sobre los diez lotes completados.
"""
from __future__ import annotations
from collections import Counter
import time
from twitch_lakehouse.full_dataset_pipeline import (
    FullSettings, validate_manifest, bootstrap as bootstrap_volume,
)
from twitch_lakehouse.storage import require_absent, write_new

ROWS = 10_000_000
BATCH_ROWS = 1_000_000
BATCHES = 10


def settings():
    return FullSettings(schema="twitch_full_10m_experimental", volume="fragments_10m")


def manifest_checked(raw):
    manifest = validate_manifest(raw, expected_rows=ROWS)
    parts = manifest["parts"]
    if len(parts) != 100:
        raise ValueError("El experimento requiere exactamente 100 fragmentos")
    if any(part["rows"] != 100_000 for part in parts):
        raise ValueError("Cada fragmento debe tener 100000 líneas")
    return manifest


def batch_table(cfg, index):
    if not isinstance(index, int) or not 0 <= index < BATCHES:
        raise ValueError("Lote fuera del rango 0..9")
    return cfg.table(f"bronze_batch_{index:03d}_10m")


def expected_parts(manifest, index):
    return manifest["parts"][index * 10:(index + 1) * 10]


def audit_batch(spark, cfg, manifest, index):
    """Lee el lote persistido: filas, 10 rutas y huella fuente declarada."""
    from pyspark.sql import functions as F
    table = batch_table(cfg, index)
    rows = spark.table(table)
    totals = rows.groupBy("source_file", "source_sha256").agg(
        F.count("*").alias("records")).collect()
    wanted = {f"{cfg.root}/{p['file']}": p["rows"] for p in expected_parts(manifest, index)}
    actual = {}
    for item in totals:
        if item["source_sha256"] != manifest["source_sha256"]:
            raise ValueError(f"Lote {index}: SHA fuente registrado distinto")
        path = item["source_file"]
        if path in actual:
            raise ValueError(f"Lote {index}: origen duplicado")
        actual[path] = item["records"]
    if actual != wanted:
        raise ValueError(f"Lote {index}: conteos/rutas no concilian; NO reescribir")
    return {"batch": index, "status": "VERIFIED", "rows": sum(actual.values()),
            "files": len(actual), "table": table}


def ingest_batch(spark, cfg, manifest, index):
    """Salta SOLO un lote existente cuya población/procedencia ha sido verificada."""
    from pyspark.sql import functions as F
    table = batch_table(cfg, index)
    if spark.catalog.tableExists(table):
        report = audit_batch(spark, cfg, manifest, index)
        return {**report, "action": "REUSED"}
    frames = []
    for part in expected_parts(manifest, index):
        path = f"{cfg.root}/{part['file']}"
        df = spark.read.text(path).withColumnRenamed("value", "raw_line")
        actual = df.count()
        if actual != part["rows"]:
            raise ValueError(f"{part['file']}: esperado {part['rows']}, leído {actual}")
        frames.append(df.withColumn("source_file", F.lit(path))
            .withColumn("source_sha256", F.lit(manifest["source_sha256"]))
            .withColumn("ingested_at", F.current_timestamp()))
    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame)
    if unioned.count() != BATCH_ROWS:
        raise ValueError(f"Lote {index}: conteo previo de 1M incorrecto")
    write_new(spark, unioned, table)
    report = audit_batch(spark, cfg, manifest, index)
    return {**report, "action": "CREATED"}


def verify_all_batches(spark, cfg, manifest):
    """Falla antes de Silver si falta un lote o no concilia."""
    reports = [audit_batch(spark, cfg, manifest, i) for i in range(BATCHES)]
    if sum(r["rows"] for r in reports) != ROWS:
        raise ValueError("No se certificaron 10M en Bronze")
    return reports


def bronze_union(spark, cfg):
    frames = [spark.table(batch_table(cfg, i)) for i in range(BATCHES)]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def build_silver(spark, cfg, manifest):
    from twitch_lakehouse.transforms import silver_frames
    from twitch_lakehouse.pipeline import silver_metrics
    target, quarantine = cfg.table("silver_interactions_10m"), cfg.table("silver_rejects_10m")
    require_absent(spark, target, quarantine)
    verify_all_batches(spark, cfg, manifest)
    source = bronze_union(spark, cfg)
    accepted, rejected, valid = silver_frames(source)
    metrics = silver_metrics(source, accepted, rejected, valid)
    if metrics["bronze_rows"] != ROWS:
        raise ValueError("Silver no recibió los 10M")
    write_new(spark, rejected, quarantine)
    write_new(spark, accepted, target)
    if (spark.table(target).count() != metrics["silver_rows"] or
        spark.table(quarantine).count() != metrics["rejected_rows"]):
        raise RuntimeError("Silver persistida no concilia")
    return metrics


def build_gold(spark, cfg):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import gold_by_streamer
    table = cfg.table("gold_streamer_metrics_10m")
    require_absent(spark, table)
    source = spark.table(cfg.table("silver_interactions_10m"))
    gold = gold_by_streamer(source)
    expected = source.agg(F.count("*").alias("n"),
                          F.sum("estimated_minutes").alias("m")).first()
    actual = gold.agg(F.sum("interactions").alias("n"),
                      F.sum("estimated_minutes").alias("m")).first()
    if expected["n"] < 1 or tuple(expected) != tuple(actual):
        raise ValueError("Gold global no coincide con Silver")
    write_new(spark, gold, table)
    saved = spark.table(table).agg(F.sum("interactions").alias("n"),
                                  F.sum("estimated_minutes").alias("m")).first()
    if tuple(saved) != tuple(expected):
        raise RuntimeError("Gold persistida no concilia")
    return {"streamers": spark.table(table).count(),
            "interactions": expected["n"], "estimated_minutes": expected["m"]}


def audit(spark, cfg, manifest):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import KEY
    batches = verify_all_batches(spark, cfg, manifest)
    silver = spark.table(cfg.table("silver_interactions_10m"))
    rejects = spark.table(cfg.table("silver_rejects_10m"))
    gold = spark.table(cfg.table("gold_streamer_metrics_10m"))
    silver_count, rejected_count = silver.count(), rejects.count()
    if silver_count <= 0 or silver_count + rejected_count > ROWS:
        raise ValueError("Silver/rechazos no concilian con Bronze")
    if silver.select(*KEY).distinct().count() != silver_count:
        raise ValueError("Claves duplicadas en Silver global")
    minutes = silver.agg(F.sum("estimated_minutes").alias("minutes")).first()["minutes"]
    totals = gold.agg(F.sum("interactions").alias("n"),
                      F.sum("estimated_minutes").alias("minutes")).first()
    if (totals["n"], totals["minutes"]) != (silver_count, minutes):
        raise ValueError("Gold no concilia con Silver")
    return {"status": "PASS", "bronze_rows": ROWS, "bronze_batches": len(batches),
            "silver_rows": silver_count, "rejected_rows": rejected_count,
            "duplicate_rows": ROWS - silver_count - rejected_count,
            "gold_interactions": totals["n"], "gold_streamers": gold.count(),
            "estimated_minutes": minutes}
