"""Orquestación explícita. Las escrituras fallan si existe el destino."""
from pyspark.sql import functions as F
from twitch_lakehouse.transforms import KEY, silver_frames, gold_by_streamer
from twitch_lakehouse.storage import require_absent, write_new


def ingest_bronze(spark, cfg):
    target = cfg.table("bronze_interactions")
    require_absent(spark, target)
    df = (spark.read.text(cfg.raw_path).withColumnRenamed("value", "raw_line")
          .withColumn("source_file", F.lit(cfg.raw_path))
          .withColumn("ingested_at", F.current_timestamp()))
    count = df.count()
    if count == 0:
        raise ValueError("Fuente vacía: no se crea Bronze")
    write_new(df, target)
    actual = spark.table(target).count()
    if actual != count:
        raise RuntimeError("Conteo Bronze distinto de la fuente; detener y revisar")
    return {"source_rows": count, "bronze_rows": actual}


def silver_metrics(bronze, silver, rejects, valid):
    counts = {"bronze_rows": bronze.count(), "valid_rows": valid.count(),
              "rejected_rows": rejects.count(), "silver_rows": silver.count()}
    counts["duplicate_rows"] = counts["valid_rows"] - counts["silver_rows"]
    if counts["bronze_rows"] != counts["valid_rows"] + counts["rejected_rows"]:
        raise ValueError("Falla conciliación Bronze = válidas + rechazadas")
    if counts["silver_rows"] == 0:
        raise ValueError("No hay filas Silver válidas; revisar el contrato antes de escribir")
    if counts["duplicate_rows"] < 0:
        raise ValueError("Conteo de duplicados inconsistente")
    if silver.select(*KEY).distinct().count() != counts["silver_rows"]:
        raise ValueError("Silver contiene duplicados")
    return counts


def build_silver(spark, cfg, candidate_suffix=None):
    # Reconstruir significa crear otra tabla, nunca reemplazar Silver.
    suffix = "" if candidate_suffix is None else f"_candidate_{candidate_suffix}"
    target = cfg.table(f"silver_interactions{suffix}")
    quarantine = cfg.table(f"silver_rejects{suffix}")
    require_absent(spark, target, quarantine)
    bronze = spark.table(cfg.table("bronze_interactions"))
    silver, rejects, valid = silver_frames(bronze)
    metrics = silver_metrics(bronze, silver, rejects, valid)
    write_new(rejects, quarantine)
    write_new(silver, target)
    if (spark.table(target).count() != metrics["silver_rows"] or
            spark.table(quarantine).count() != metrics["rejected_rows"]):
        raise RuntimeError("Conteos persistidos inconsistentes; detener y revisar")
    return {**metrics, "silver_table": target, "rejects_table": quarantine}


def build_gold(spark, cfg):
    target = cfg.table("gold_streamer_metrics")
    require_absent(spark, target)
    silver = spark.table(cfg.table("silver_interactions"))
    gold = gold_by_streamer(silver)
    expected = silver.agg(F.count("*").alias("rows"),
                         F.sum("estimated_minutes").alias("minutes")).first()
    actual = gold.agg(F.sum("interactions").alias("rows"),
                      F.sum("estimated_minutes").alias("minutes")).first()
    if expected["rows"] == 0 or tuple(expected) != tuple(actual):
        raise ValueError("Gold no concilia interacciones/minutos con Silver")
    write_new(gold, target)
    return {"gold_rows": spark.table(target).count(),
            "interactions": actual["rows"], "estimated_minutes": actual["minutes"]}
