"""I3: agregados puros compatibles con Spark Connect; ninguna escritura."""
from pyspark.sql import functions as F


def audience_by_streamer(silver, gold):
    """Recurrencia = usuarios con >=2 filas Silver / usuarios distintos del streamer."""
    users = silver.groupBy("streamer_username", "user_id").agg(
        F.count("*").alias("user_interactions"))
    recurring = users.groupBy("streamer_username").agg(
        F.sum(F.when(F.col("user_interactions") >= 2, 1).otherwise(0))
        .alias("recurring_users"))
    return gold.join(recurring, "streamer_username").withColumn(
        "recurrence_rate", F.col("recurring_users") / F.col("unique_users"))


def duration_distribution(silver):
    minutes = F.col("estimated_minutes")
    bucket = (F.when(minutes == 0, 0).when(minutes <= 10, 1)
              .when(minutes <= 30, 2).when(minutes <= 60, 3).otherwise(4))
    return silver.withColumn("duration_bucket", bucket).groupBy(
        "streamer_username", "duration_bucket").agg(
            F.count("*").alias("interactions"),
            F.sum("estimated_minutes").alias("estimated_minutes"))


def activity_by_step(silver):
    """Inicios por paso nativo (10 min). No concurrencia ni intervalos expandidos."""
    return silver.groupBy("streamer_username", "time_start").agg(
        F.count("*").alias("connection_starts"),
        F.sum("estimated_minutes").alias("started_estimated_minutes"))


def assert_equal_frames(left, right, columns):
    """Equivalencia multiconjunto; incluye diferencias con igual suma global."""
    a, b = left.select(*columns), right.select(*columns)
    if a.exceptAll(b).limit(1).count() or b.exceptAll(a).limit(1).count():
        raise ValueError("Contenido no concilia: " + ", ".join(columns))


def validate_sources(silver, gold, bronze_rows, rejected_rows):
    from twitch_lakehouse.transforms import KEY, gold_by_streamer
    invalid = (F.col("user_id").isNull() | (F.col("user_id") <= 0)
               | F.col("stream_id").isNull() | (F.col("stream_id") <= 0)
               | F.col("streamer_username").isNull()
               | (F.trim("streamer_username") == "")
               | (F.col("streamer_username") != F.trim("streamer_username"))
               | F.col("time_start").isNull() | (F.col("time_start") < 0)
               | F.col("time_stop").isNull()
               | (F.col("time_stop") > 2147483647)
               | (F.col("time_stop") < F.col("time_start"))
               | F.col("estimated_minutes").isNull()
               | (F.col("estimated_minutes") !=
                  (F.col("time_stop") - F.col("time_start")) * 10))
    if silver.filter(invalid).limit(1).count():
        raise ValueError("Silver incumple contrato I1")
    totals = silver.agg(F.count("*").alias("silver_rows"),
                        F.sum("estimated_minutes").alias("estimated_minutes")).first().asDict()
    rows = totals["silver_rows"]
    if rows == 0 or silver.select(*KEY).distinct().count() != rows:
        raise ValueError("Silver vacía o con clave duplicada")
    duplicates = bronze_rows - rejected_rows - rows
    if rejected_rows < 0 or duplicates < 0:
        raise ValueError("Conteos I1 inconsistentes")
    assert_equal_frames(gold, gold_by_streamer(silver), [
        "streamer_username", "interactions", "unique_users", "unique_streams", "estimated_minutes"])
    return {**totals, "bronze_rows": bronze_rows, "rejected_rows": rejected_rows,
            "duplicates_removed_inferred": duplicates, "silver_duplicate_keys": 0,
            "gold_streamers": gold.count()}


def validate_aggregates(frames, totals):
    """Cada familia debe conservar filas y minutos; usuarios distintos no se suman."""
    names = {"streamer": ("interactions", "estimated_minutes"),
             "duration": ("interactions", "estimated_minutes"),
             "activity": ("connection_starts", "started_estimated_minutes")}
    for name, (rows, minutes) in names.items():
        actual = frames[name].agg(F.sum(rows), F.sum(minutes)).first()
        if tuple(actual) != (totals["silver_rows"], totals["estimated_minutes"]):
            raise ValueError(f"No concilia agregado {name}")
    if frames["streamer"].filter(
        (F.col("recurring_users") < 0) | (F.col("recurring_users") > F.col("unique_users"))
        | F.col("recurrence_rate").isNull()).limit(1).count():
        raise ValueError("Recurrencia fuera de dominio")
