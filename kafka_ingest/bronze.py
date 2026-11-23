"""Spark Connect: append Delta nativo + AvailableNow, sin foreachBatch ni JVM/RDD."""
import json
import uuid
from kafka_ingest.contracts import canonical, digest, event_id, validate_manifest
from twitch_lakehouse.storage import write_new

NAMESPACE = "workspace.twitch_kafka_experimental"
BRONZE = NAMESPACE + ".bronze_kafka_pilot_v1"
BINDING = NAMESPACE + ".pilot_binding_v1"
ROOT = "/Volumes/workspace/twitch_kafka_experimental/kafka_state"
CHECKPOINT = ROOT + "/pilot_v1"
ENVELOPE = ("twitch_load_id STRING, twitch_source_row BIGINT, source_sha256 STRING, "
            "event_id STRING, historical_replay BOOLEAN, produced_at STRING, raw_line STRING, "
            "_corrupt_record STRING")


def options(bootstrap, topic, username, password, mechanism="SCRAM-SHA-512", ca_pem=None):
    if mechanism not in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        raise ValueError("Mecanismo SASL no soportado")
    # Valores secretos solo en memoria; no imprimir esta estructura ni excepciones Kafka.
    def quoted(value):
        if any(c in value for c in "\r\n\x00"):
            raise ValueError("Credencial con caracteres de control")
        return value.replace("\\", "\\\\").replace('"', '\\"')
    module = "plain.PlainLoginModule" if mechanism == "PLAIN" else "scram.ScramLoginModule"
    configured = {"kafka.bootstrap.servers": bootstrap, "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": mechanism,
        "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.{module} required username="{quoted(username)}" password="{quoted(password)}";',
        "kafka.ssl.endpoint.identification.algorithm": "https", "failOnDataLoss": "true",
        "startingOffsets": "earliest", "maxOffsetsPerTrigger": "250"}
    if ca_pem:
        configured.update({"kafka.ssl.truststore.type": "PEM", "kafka.ssl.truststore.certificates": ca_pem})
    return configured


def project(kafka, cluster_id):
    from pyspark.sql import functions as F
    return (kafka.select("topic", "partition", "offset", "key", "value",
            F.col("timestamp").alias("kafka_timestamp"), "timestampType")
        .withColumn("source_cluster_id", F.lit(cluster_id))
        .withColumn("transport_id", F.sha2(F.concat_ws("|", "source_cluster_id", "topic", "partition", "offset"), 256))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("payload_original", F.col("value").cast("string"))
        .withColumn("e", F.from_json("payload_original", ENVELOPE,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
        .select("*", "e.*").drop("e"))


def bootstrap(spark, m):
    validate_manifest(m)
    # El destino NO es configurable: impide apuntar por accidente a I1/r01.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NAMESPACE}").collect()
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {NAMESPACE}.kafka_state").collect()
    binding = canonical(m)
    present = [spark.catalog.tableExists(t) for t in (BINDING, BRONZE)]
    if any(present):
        if not all(present):
            raise RuntimeError("Bootstrap parcial: conservar y diagnosticar; no recrear")
        rows = spark.table(BINDING).collect()
        if len(rows) != 1 or rows[0]["manifest_json"] != binding:
            raise RuntimeError("Destino vinculado a otro manifiesto; no reutilizar checkpoint")
        return
    write_new(spark, spark.createDataFrame([(binding,)], "manifest_json STRING"), BINDING)
    spark.sql(f"""CREATE TABLE {BRONZE} (
        topic STRING, partition INT, offset BIGINT, key BINARY, value BINARY,
        kafka_timestamp TIMESTAMP, timestampType INT, source_cluster_id STRING,
        transport_id STRING, ingested_at TIMESTAMP, payload_original STRING,
        twitch_load_id STRING, twitch_source_row BIGINT, source_sha256 STRING,
        event_id STRING, historical_replay BOOLEAN, produced_at STRING,
        raw_line STRING, _corrupt_record STRING) USING DELTA""").collect()


def audit(spark, m, table=None):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import classify, KEY
    rows = spark.table(table or BRONZE)
    selected = rows.filter(F.col("twitch_load_id") == m["twitch_load_id"])
    transport_rows = selected.count()
    identity_bad = selected.filter(
        ~F.coalesce((F.col("source_sha256") == m["source_sha256"])
        & (F.col("topic") == m["topic"])
        & (F.col("source_cluster_id") == m["source_cluster_id"])
        & F.col("twitch_source_row").between(1, m["count"])
        & (F.col("event_id") == F.sha2(F.concat_ws("|", "source_sha256", "twitch_source_row"), 256))
        & (F.col("historical_replay") == F.lit(True))
        & F.col("_corrupt_record").isNull()
        & F.col("raw_line").isNotNull()
        & F.expr("try_cast(produced_at AS TIMESTAMP)").isNotNull(), F.lit(False))).count()
    conflicts = (selected.groupBy("twitch_source_row").agg(
        F.countDistinct("raw_line").alias("versions")).filter("versions <> 1").count())
    logical = selected.dropDuplicates(["source_sha256", "twitch_source_row"])
    # Solo el piloto está habilitado: recoger hasta 1001 filas permite comparar hash
    # exacto ordenado. NO reutilizar este collect para millones.
    source = logical.orderBy("twitch_source_row").select("twitch_source_row", "raw_line").limit(1001).collect()
    ranges = [r.asDict() for r in selected.groupBy("partition").agg(
        F.min("offset").alias("min_offset"), F.max("offset").alias("max_offset"),
        F.countDistinct("offset").alias("offset_count")).orderBy("partition").collect()]
    contiguous = all(p["offset_count"] == p["max_offset"] - p["min_offset"] + 1 for p in ranges)
    classified = classify(logical)
    rejected = classified.filter(F.col("quality_errors") != "").count()
    valid = classified.filter(F.col("quality_errors") == "")
    business_duplicates = valid.count() - valid.select(*KEY).distinct().count()
    report = {"twitch_load_id": m["twitch_load_id"], "source_sha256": m["source_sha256"],
        "source_cluster_id": m["source_cluster_id"], "topic": m["topic"],
        "transport_rows": transport_rows, "source_rows": len(source),
        "source_replays": transport_rows - len(source), "rejected_source_rows": rejected,
        "valid_source_rows": len(source) - rejected, "business_duplicates": business_duplicates,
        "transport_duplicates": transport_rows - selected.select("transport_id").distinct().count(),
        "identity_errors": identity_bad, "payload_conflicts": conflicts,
        "foreign_rows": rows.count() - transport_rows, "offsets": ranges,
        "first_source_row": source[0][0] if source else None,
        "last_source_row": source[-1][0] if source else None,
        "payload_sha256": digest(canonical([r["raw_line"] for r in source])),
        "audit_table": NAMESPACE + ".audit_" + uuid.uuid4().hex}
    report["status"] = "PASS" if (len(source) == m["count"]
        and [r[0] for r in source] == list(range(1, m["count"] + 1))
        and report["payload_sha256"] == m["payload_sha256"] and not identity_bad
        and not conflicts and not report["transport_duplicates"] and not report["foreign_rows"]
        and contiguous) else "FAIL"
    return report


def run(spark, m, kafka_options, checkpoint_present):
    validate_manifest(m)
    bootstrap(spark, m)
    if spark.table(BRONZE).limit(1).count() and not checkpoint_present():
        raise RuntimeError("Bronze contiene datos pero falta checkpoint; detener sin reiniciar offsets")
    query = None
    attempt_table = NAMESPACE + ".attempt_" + uuid.uuid4().hex
    write_new(spark, spark.createDataFrame([("STARTED", m["twitch_load_id"])],
        "status STRING, twitch_load_id STRING"), attempt_table)
    try:
        # Topic dedicado e inactivo. Freno del piloto; maxOffsetsPerTrigger NO
        # limita el total de AvailableNow. Se toleran hasta 5 reenvíos completos.
        probe_options = {k: v for k, v in kafka_options.items() if k != "maxOffsetsPerTrigger"}
        probe = spark.read.format("kafka").options(**probe_options).option("endingOffsets", "latest").load()
        if probe.limit(5001).count() > 5000:
            raise RuntimeError("Topic supera el límite del piloto; no iniciar stream")
        stream = spark.readStream.format("kafka").options(**kafka_options).load()
        query = (project(stream, m["source_cluster_id"]).writeStream.format("delta")
            .outputMode("append").option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True).toTable(BRONZE))
        query.awaitTermination()
        report = audit(spark, m)
        # No guardar lastProgress completo: puede incluir descripciones/opciones.
        report["query_id"] = str(query.id)
        report["stream_run_id"] = str(query.runId)
        last = query.lastProgress or {}
        report["last_microbatch"] = {k: last[k] for k in
            ("batchId", "numInputRows", "timestamp", "durationMs") if k in last}
        report["attempt_table"] = attempt_table
        write_new(spark, spark.createDataFrame([(canonical(report),)], "report_json STRING"), report["audit_table"])
        spark.createDataFrame([(report["status"], m["twitch_load_id"])],
            "status STRING, twitch_load_id STRING").write.format("delta").mode("append").saveAsTable(attempt_table)
        if report["status"] != "PASS":
            raise RuntimeError("Auditoría FAIL; conservar Bronze y checkpoint")
        return report
    except Exception as error:
        spark.createDataFrame([("FAILED:" + type(error).__name__, m["twitch_load_id"])],
            "status STRING, twitch_load_id STRING").write.format("delta").mode("append").saveAsTable(attempt_table)
        raise RuntimeError("Piloto remoto falló; revisar tablas attempt/audit sin exponer secretos") from None
