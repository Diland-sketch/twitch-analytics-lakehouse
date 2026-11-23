"""Batch experimental sobre Bronze Kafka auditada: Spark Connect, destinos nuevos."""
from dataclasses import dataclass
import json
import re
import uuid
from kafka_ingest.bronze import NAMESPACE, ENVELOPE
from kafka_ingest.contracts import canonical, validate_manifest
from kafka_ingest.native_audit import audit_native
from twitch_lakehouse.storage import require_absent, write_new

BRONZE = NAMESPACE + ".bronze_kafka_native_pilot_v1"
BRONZE_VERSION = 7
AUTHORIZATION = NAMESPACE + ".audit_9663046b15734333a35d395af3486d99"
BINDING = NAMESPACE + ".native_pilot_binding_v1"
STAGES = ("manifest", "source_classification", "rejects", "silver", "gold", "reconciliation")


@dataclass(frozen=True)
class Release:
    name: str = "kafka_b7_r001"

    def __post_init__(self):
        if not re.fullmatch(r"kafka_b7_r[0-9]{3}", self.name):
            raise ValueError("Release permitida: kafka_b7_rNNN, solo esquema experimental")

    def table(self, stage):
        if stage not in STAGES and not re.fullmatch(r"failure_[a-f0-9]{32}", stage):
            raise ValueError("Etapa no permitida")
        return f"{NAMESPACE}.{stage}_{self.name}"


def validate_authorization(report, binding):
    m = validate_manifest(binding["manifest"])
    required = {"audit_contract": "twitch-native-source-audit-v2", "status": "PASS",
        "source_coverage_status": "PASS", "identity_integrity_status": "PASS",
        "status_scope": "SOURCE_IDENTITY_AND_CONTENT_ONLY", "bronze_table": BRONZE,
        "bronze_delta_version": BRONZE_VERSION, "bronze_rows": 1000, "source_rows": 1000,
        "distinct_event_ids": 1000, "first_source_row": 1, "last_source_row": 1000,
        "missing_source_rows": 0, "missing_event_ids": 0, "unexpected_event_ids": 0,
        "duplicate_event_id_rows": 0, "identity_errors": 0, "key_identity_errors": 0,
        "payload_conflicts": 0, "event_payload_conflicts": 0,
        "source_sha256": m["source_sha256"], "twitch_load_id": m["twitch_load_id"],
        "payload_sha256": m["payload_sha256"], "expected_payload_sha256": m["payload_sha256"],
        "pipeline_id": binding["pipeline_id"]}
    if any(report.get(k) != v for k, v in required.items()):
        raise ValueError("La auditoría persistida no autoriza este Bronze v7/manifiesto")
    if not report.get("expected_event_ids_sha256") or report["expected_event_ids_sha256"] != report.get("observed_event_ids_sha256"):
        raise ValueError("Las identidades auditadas no concilian")
    return m


def fixed_json(spark, table, column):
    version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
    rows = spark.read.option("versionAsOf", version).table(table).select(column).limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Se requiere exactamente un registro de autorización/binding")
    return json.loads(rows[0][0]), version


def frames(bronze, manifest):
    """Fuente → reglas I1 → KEY. Conserva disposición de cada identidad fuente."""
    from pyspark.sql import functions as F, Window
    from twitch_lakehouse.transforms import classify, KEY, gold_by_streamer
    m = validate_manifest(manifest)
    parsed = (bronze.select(F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload_original"))
        .withColumn("e", F.from_json("payload_original", ENVELOPE,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
        .select("kafka_key", "payload_original", "e.*"))
    identity = ((F.col("source_sha256") == m["source_sha256"])
        & (F.col("twitch_load_id") == m["twitch_load_id"])
        & F.col("twitch_source_row").between(1, m["count"])
        & (F.col("event_id") == F.sha2(F.concat_ws("|", "source_sha256", "twitch_source_row"), 256))
        & (F.col("kafka_key") == F.col("event_id")) & F.col("_corrupt_record").isNull()
        & F.col("raw_line").isNotNull() & (F.col("historical_replay") == F.lit(True)))
    if parsed.filter(~F.coalesce(identity, F.lit(False))).limit(1).count():
        raise ValueError("Envelope/identidad no válida; detener la carga auditada")
    source_key = ["source_sha256", "twitch_source_row"]
    if (parsed.groupBy(*source_key).agg(F.countDistinct("raw_line").alias("versions"))
            .filter("versions <> 1").limit(1).count()):
        raise ValueError("Conflicto de contenido para una identidad fuente")
    source_window = Window.partitionBy(*source_key)
    source = (parsed.withColumn("source_occurrences", F.count("*").over(source_window))
        .withColumn("_source_rank", F.row_number().over(source_window.orderBy("payload_original")))
        .filter("_source_rank = 1").drop("_source_rank"))
    classified = classify(source)
    valid = classified.filter(F.col("quality_errors") == "")
    valid = valid.withColumn("business_rank", F.row_number().over(Window.partitionBy(*KEY)
        .orderBy("twitch_source_row", "event_id")))
    rejects = (classified.filter(F.col("quality_errors") != "")
        .withColumn("business_rank", F.lit(None).cast("int")))
    quality = (valid.unionByName(rejects).withColumn("disposition",
        F.when(F.col("quality_errors") != "", "REJECTED")
        .when(F.col("business_rank") > 1, "BUSINESS_DUPLICATE").otherwise("SILVER")))
    silver = (valid.filter("business_rank = 1").select(*KEY, *source_key, "twitch_load_id", "event_id", "raw_line")
        .withColumn("estimated_minutes", (F.col("time_stop") - F.col("time_start")) * F.lit(10)))
    return {"source_classification": quality, "rejects": quality.filter("disposition = 'REJECTED'"),
            "silver": silver, "gold": gold_by_streamer(silver)}


def reconcile(quality, rejects, silver, gold):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import KEY, gold_by_streamer
    sources = quality.count()
    bronze = quality.agg(F.coalesce(F.sum("source_occurrences"), F.lit(0))).first()[0]
    rejected = rejects.count()
    duplicates = quality.filter("disposition = 'BUSINESS_DUPLICATE'").count()
    silver_count = silver.count()
    silver_minutes = silver.agg(F.coalesce(F.sum("estimated_minutes"), F.lit(0))).first()[0]
    totals = gold.agg(F.coalesce(F.sum("interactions"), F.lit(0)).alias("interactions"),
        F.coalesce(F.sum("estimated_minutes"), F.lit(0)).alias("minutes")).first()
    expected_keys = quality.filter("quality_errors = ''").select(*KEY).distinct()
    actual_keys = silver.select(*KEY)
    expected_gold = gold_by_streamer(silver).select(*gold.columns)
    key_differences = actual_keys.exceptAll(expected_keys).count() + expected_keys.exceptAll(actual_keys).count()
    gold_differences = gold.exceptAll(expected_gold).count() + expected_gold.exceptAll(gold).count()
    reject_differences = (rejects.exceptAll(quality.filter("disposition = 'REJECTED'")).count()
        + quality.filter("disposition = 'REJECTED'").exceptAll(rejects).count())
    checks = {"source_identity_unique": quality.select("source_sha256", "twitch_source_row").distinct().count() == sources,
        "source_balance": sources == rejected + duplicates + silver_count,
        "bronze_balance": bronze >= sources,
        "silver_keys_match_valid_sources": key_differences == 0,
        "silver_no_business_duplicates": actual_keys.distinct().count() == silver_count,
        "rejects_match_classification": reject_differences == 0,
        "gold_interactions_balance": totals["interactions"] == silver_count,
        "gold_minutes_balance": totals["minutes"] == silver_minutes,
        "gold_exact_aggregates": gold_differences == 0}
    return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
        "bronze_rows": bronze, "source_rows": sources, "source_duplicate_rows": bronze - sources,
        "rejected_source_rows": rejected, "valid_source_rows": sources - rejected,
        "business_duplicate_rows": duplicates, "silver_rows": silver_count,
        "gold_streamers": gold.count(), "gold_interactions": totals["interactions"],
        "estimated_minutes": silver_minutes}


def run(spark, release=Release()):
    require_absent(spark, *(release.table(stage) for stage in STAGES))
    authorization, audit_version = fixed_json(spark, AUTHORIZATION, "report_json")
    binding, binding_version = fixed_json(spark, BINDING, "binding_json")
    m = validate_authorization(authorization, binding)
    bronze = spark.read.option("versionAsOf", BRONZE_VERSION).table(BRONZE)
    observed = audit_native(bronze, m)
    if observed["status"] != "PASS" or any(observed[k] != authorization[k] for k in
            ("source_rows", "bronze_rows", "payload_sha256", "observed_event_ids_sha256")):
        raise ValueError("Bronze v7 no reproduce la autorización; no se escribe ninguna tabla")
    results = frames(bronze, m)
    preflight = reconcile(results["source_classification"], results["rejects"], results["silver"], results["gold"])
    if preflight["status"] != "PASS" or preflight["bronze_rows"] != authorization["bronze_rows"]:
        raise ValueError("Conciliación previa FAIL; no se escribe ninguna tabla")
    provenance = {"release_id": release.name, "bronze_table": BRONZE, "bronze_delta_version": BRONZE_VERSION,
        "authorization_table": AUTHORIZATION, "authorization_delta_version": audit_version,
        "authorization_report": authorization, "binding_table": BINDING,
        "binding_delta_version": binding_version, "binding": binding,
        "source_recheck": observed, "state": "AUTHORIZED_NOT_PUBLISHED"}
    try:
        write_new(spark, spark.createDataFrame([(canonical(provenance),)], "manifest_json STRING"), release.table("manifest"))
        from pyspark.sql import functions as F
        for stage in ("source_classification", "rejects", "silver", "gold"):
            frame = results[stage].withColumn("release_id", F.lit(release.name))
            write_new(spark, frame, release.table(stage))
        # Conciliación desde tablas realmente persistidas, con columnas de negocio.
        stored = {stage: spark.table(release.table(stage)).drop("release_id") for stage in results}
        report = reconcile(stored["source_classification"], stored["rejects"], stored["silver"], stored["gold"])
        report.update(release_id=release.name, bronze_delta_version=BRONZE_VERSION,
            authorization_table=AUTHORIZATION, authorization_delta_version=audit_version,
            offset_audit_status=authorization["offset_audit_status"],
            tables={s: release.table(s) for s in STAGES})
        for stage in results:
            report["checks"]["persisted_" + stage + "_matches_plan"] = (
                stored[stage].exceptAll(results[stage]).limit(1).count() == 0
                and results[stage].exceptAll(stored[stage]).limit(1).count() == 0)
        report["checks"]["persisted_counts_match_authorized_bronze"] = report["bronze_rows"] == authorization["bronze_rows"]
        report["status"] = "PASS" if all(report["checks"].values()) else "FAIL"
        write_new(spark, spark.createDataFrame([(canonical(report),)], "report_json STRING"), release.table("reconciliation"))
        if report["status"] != "PASS":
            raise RuntimeError("Conciliación persistida FAIL; conservar destinos y diagnosticar")
        return report
    except Exception as error:
        failure = {"release_id": release.name, "status": "FAILED", "error_type": type(error).__name__,
                   "authorization_table": AUTHORIZATION, "action": "Conservar tablas parciales; no reintentar con el mismo release"}
        write_new(spark, spark.createDataFrame([(canonical(failure),)], "report_json STRING"), release.table("failure_" + uuid.uuid4().hex))
        raise
