"""Verificación I3 de solo lectura: release Gold y conciliación entre familias.

No crea, reemplaza ni elimina tablas. No certifica la procedencia semántica del
dataset: sólo verifica el marcador y los agregados publicados por el pipeline I3.
"""
from twitch_lakehouse.analytics_pipeline import targets

EXPECTED_SCOPE = "I1_COUNTS_AND_I3_EXACT_AGGREGATES"


def verify_counts(record, families):
    """Valida contratos aritméticos con enteros, sin Spark (fácil de probar)."""
    if record.get("status") != "PASS" or record.get("scope") != EXPECTED_SCOPE:
        raise ValueError("Marcador quality ausente, inválido o con scope distinto")
    for key in ("bronze_rows", "silver_rows", "rejected_rows",
                "duplicates_removed_inferred", "silver_duplicate_keys",
                "gold_streamers", "estimated_minutes"):
        value = record.get(key)
        if type(value) is not int or value < 0:
            raise ValueError(f"Quality {key} debe ser un entero no negativo")
    if not record["silver_rows"] or not record["gold_streamers"]:
        raise ValueError("Publicación vacía")
    if record["silver_duplicate_keys"] != 0:
        raise ValueError("Claves Silver duplicadas")
    if (record["bronze_rows"] - record["rejected_rows"] -
            record["duplicates_removed_inferred"] != record["silver_rows"]):
        raise ValueError("La población Bronze/Silver no concilia")
    if set(families) != {"streamer", "duration", "activity"}:
        raise ValueError("Faltan familias de agregados")
    expected = (record["silver_rows"], record["estimated_minutes"])
    for family, actual in families.items():
        if not isinstance(actual, tuple) or len(actual) != 2 or any(
                type(v) is not int or v < 0 for v in actual):
            raise ValueError(f"Agregado {family} nulo o inválido")
        if actual != expected:
            raise ValueError(f"Agregado {family} no concilia: {actual} vs {expected}")
    return True


def verify_release(spark, cfg, release="r01"):
    """Inspección explícita posterior a publicación: no modifica Delta."""
    from pyspark.sql import functions as F
    names = targets(cfg, release)
    for name in names.values():
        if not spark.catalog.tableExists(name):
            raise ValueError(f"Falta tabla de publicación I3: {name}")

    rows = spark.table(names["quality"]).limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Quality debe contener exactamente una fila")
    record = rows[0].asDict()
    if record.get("release") != release or record.get("source_namespace") != cfg.namespace:
        raise ValueError("Release o namespace incorrectos en quality")
    for name in ("bronze_version", "silver_version", "rejects_version", "gold_version"):
        version = record.get(name)
        if type(version) is not int or version < 0:
            raise ValueError(f"Versión de origen inválida: {name}")

    columns = {
        "streamer": ("interactions", "estimated_minutes"),
        "duration": ("interactions", "estimated_minutes"),
        "activity": ("connection_starts", "started_estimated_minutes"),
    }
    totals = {}
    for family, (count_col, minutes_col) in columns.items():
        agg = spark.table(names[family]).agg(
            F.sum(count_col).alias("rows"),
            F.sum(minutes_col).alias("minutes")
        ).first()
        totals[family] = (agg["rows"], agg["minutes"])
    verify_counts(record, totals)
    return {"release": release, "status": "PASS", "quality": record,
            "family_totals": totals, "tables": names}
