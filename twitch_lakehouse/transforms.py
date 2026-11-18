"""Transformaciones DataFrame: sin escrituras, RDD, UDF ni servicios externos."""
from pyspark.sql import functions as F

KEY = ["user_id", "stream_id", "streamer_username", "time_start", "time_stop"]


def classify(bronze):
    """Contrato I1: cinco tokens sin comillas; conserva línea y procedencia."""
    df = bronze.withColumn("_parts", F.split("raw_line", ",", -1))
    # get devuelve NULL ante posiciones ausentes, incluso con ANSI activado.
    for index, name in enumerate(KEY):
        df = df.withColumn(name, F.trim(F.get("_parts", index)))
    for name in ("user_id", "stream_id", "time_start", "time_stop"):
        # Regex excluye decimales; try_cast impide abortar por overflow o texto.
        df = df.withColumn(name, F.when(F.col(name).rlike(r"^[0-9]+$"),
                                       F.expr(f"try_cast({name} AS BIGINT)")))
    rules = {
        "field_count": F.size("_parts") != 5,
        "unsupported_quotes": F.instr("raw_line", '"') > 0,
        "invalid_user_id": F.col("user_id").isNull() | (F.col("user_id") <= 0),
        "invalid_stream_id": F.col("stream_id").isNull() | (F.col("stream_id") <= 0),
        "empty_streamer": F.col("streamer_username").isNull() | (F.col("streamer_username") == ""),
        "invalid_start": F.col("time_start").isNull() | (F.col("time_start") > 2147483647),
        "invalid_stop": F.col("time_stop").isNull() | (F.col("time_stop") > 2147483647),
        "reversed_interval": F.col("time_stop") < F.col("time_start"),
    }
    return df.withColumn("quality_errors", F.concat_ws("|", *[
        F.when(condition, F.lit(name)) for name, condition in rules.items()
    ])).drop("_parts")


def silver_frames(bronze):
    classified = classify(bronze)
    valid = classified.filter(F.col("quality_errors") == "")
    rejects = classified.filter(F.col("quality_errors") != "")
    # Una fila Silver por combinación de los cinco campos normalizados.
    silver = valid.select(*KEY).dropDuplicates(KEY).withColumn(
        "estimated_minutes", (F.col("time_stop") - F.col("time_start")) * F.lit(10))
    return silver, rejects, valid


def gold_by_streamer(silver):
    return silver.groupBy("streamer_username").agg(
        F.count("*").alias("interactions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("stream_id").alias("unique_streams"),
        F.sum("estimated_minutes").alias("estimated_minutes"),
    )
