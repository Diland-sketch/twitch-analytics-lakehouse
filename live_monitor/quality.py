"""JSON explícito + contrato Silver I1, evaluado por Spark, sin UDF."""
from pyspark.sql import functions as F
from twitch_lakehouse.transforms import classify

SCHEMA = ("run_id STRING, source_sha256 STRING, source_row BIGINT, event_id STRING, "
          "historical_replay BOOLEAN, produced_at STRING, raw_line STRING, _corrupt_record STRING")


def transform(kafka):
    parsed = kafka.select("topic", "partition", "offset", F.col("timestamp").alias("received_at"),
                          F.col("value").cast("string").alias("json_value"))
    parsed = parsed.withColumn("e", F.from_json("json_value", SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"})).select("*", "e.*").drop("e")
    identity = (F.col("run_id").rlike(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
                & F.col("source_sha256").rlike(r"^[0-9a-f]{64}$")
                & (F.col("source_row") > 0)
                & (F.col("event_id") == F.sha2(F.concat_ws("|", "run_id", "source_sha256", "source_row"), 256)))
    parsed = parsed.withColumn("identity_valid", F.coalesce(identity, F.lit(False)))
    normalized = classify(parsed)
    errors = F.concat_ws("|", F.col("quality_errors"),
        F.when(F.col("_corrupt_record").isNotNull() | F.col("json_value").isNull(), "invalid_json"),
        F.when(~F.col("identity_valid"), "invalid_identity"),
        F.when(~F.coalesce(F.col("historical_replay"), F.lit(False)), "not_historical_replay"),
        F.when(F.expr("try_cast(produced_at AS TIMESTAMP)").isNull(), "invalid_produced_at"),
        F.when(F.col("raw_line").isNull(), "missing_raw_line"))
    # concat_ws con quality_errors vacío deja separador inicial: eliminarlo.
    normalized = normalized.withColumn("errors", F.regexp_replace(errors, r"^\|", ""))
    return (normalized.withColumn("valid", F.col("errors") == "")
        .withColumn("event_key", F.when(F.col("identity_valid"), F.col("event_id")).otherwise(
            F.concat_ws(":", F.lit("transport"), "topic", "partition", "offset")))
        .withColumn("run_id", F.when(F.col("identity_valid"), F.col("run_id")).otherwise(F.lit("unidentified")))
        .withColumn("payload_hash", F.sha2(F.concat_ws("|",
            F.coalesce("raw_line", "json_value", F.lit("<null>")), "errors"), 256))
        .withColumn("estimated_minutes", F.when(F.col("valid"), (F.col("time_stop") - F.col("time_start")) * 10)))
