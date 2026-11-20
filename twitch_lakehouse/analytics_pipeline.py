"""Publicación I3 por versión nueva. Ninguna fuente es destino de escritura."""
from twitch_lakehouse.config import identifier
from twitch_lakehouse.storage import require_absent, write_new


def targets(cfg, release):
    identifier(release)
    return {kind: cfg.table(f"gold_i3_{kind}_{release}")
            for kind in ("streamer", "duration", "activity", "quality")}


def build_analytics(spark, cfg, release="r01"):
    from twitch_lakehouse.analytics import (
        audience_by_streamer, duration_distribution, activity_by_step,
        validate_sources, validate_aggregates, assert_equal_frames)
    destinations = targets(cfg, release)
    # Rechazar TODOS los destinos antes de leer o crear cualquiera.
    require_absent(spark, *destinations.values())
    sources, versions = {}, {}
    for key, name in {"bronze": "bronze_interactions", "silver": "silver_interactions",
                      "rejects": "silver_rejects", "gold": "gold_streamer_metrics"}.items():
        table = cfg.table(name)
        # cfg valida identificadores; no se admite SQL arbitrario.
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        versions[f"{key}_version"] = version
        sources[key] = spark.read.option("versionAsOf", version).table(table)
    totals = validate_sources(sources["silver"], sources["gold"],
                              sources["bronze"].count(), sources["rejects"].count())
    frames = {"streamer": audience_by_streamer(sources["silver"], sources["gold"]),
              "duration": duration_distribution(sources["silver"]),
              "activity": activity_by_step(sources["silver"])}
    validate_aggregates(frames, totals)
    for kind, frame in frames.items():
        write_new(spark, frame, destinations[kind])
    persisted = {kind: spark.table(destinations[kind]) for kind in frames}
    validate_aggregates(persisted, totals)
    # Validación exacta de persistencia; coste deliberado, solo durante publicación.
    for kind, frame in frames.items():
        assert_equal_frames(frame, persisted[kind], frame.columns)
    # Marcador final: si falta, ninguna tabla de la release se considera publicada.
    record = {**totals, **versions, "release": release, "source_namespace": cfg.namespace,
              "status": "PASS", "scope": "I1_COUNTS_AND_I3_EXACT_AGGREGATES"}
    quality = spark.createDataFrame([record])
    write_new(spark, quality, destinations["quality"])
    assert_equal_frames(quality, spark.table(destinations["quality"]), quality.columns)
    return {"tables": destinations, "quality": record}
