"""Prueba REAL local opcional; Kafka privado → Spark → Parquet, NO Databricks/Delta.

Usa un topic exclusivo nuevo. No modifica el topic ni el checkpoint I4.
Ejecutar solo dentro de la red Docker existente con dataset montado de solo lectura.
"""
import argparse
import json
import time
import uuid
from pathlib import Path
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession, functions as F
from kafka_ingest.producer import prepare, publish, load_block, save_new
from kafka_ingest.bronze import project
from kafka_ingest.contracts import canonical, digest, event_id
from live_monitor.common import utc_now


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    root = Path(args.evidence)
    if list(root.iterdir()):
        raise ValueError("El directorio de evidencia debe estar vacío")
    topic = "twitch.pilot.local." + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers="kafka:9092")
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    admin.close()
    m = prepare(args.source, root / "block", topic, "docker-private-local-pilot")
    def factory():
        return KafkaProducer(bootstrap_servers="kafka:9092", acks="all", enable_idempotence=True,
            key_serializer=lambda k: k.encode(), value_serializer=lambda v: canonical(v).encode())
    started = time.monotonic()
    publish(root / "block", factory=factory, rate=1000)
    spark = (SparkSession.builder.master("local[2]").appName("twitch-real-local-pilot")
        .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    def consume():
        source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", topic).option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 250).load())
        q = (project(source, m["source_cluster_id"]).writeStream.format("parquet")
            .option("path", str(root / "bronze_parquet"))
            .option("checkpointLocation", str(root / "checkpoint"))
            .trigger(availableNow=True).start())
        q.awaitTermination()
        data = spark.read.parquet(str(root / "bronze_parquet"))
        rows = data.select("twitch_source_row", "raw_line").distinct().orderBy("twitch_source_row").collect()
        metrics = {"transport_rows": data.count(), "source_rows": len(rows),
            "transport_ids": data.select("transport_id").distinct().count(),
            "payload_sha256": digest(canonical([r.raw_line for r in rows])),
            "min_offset": data.agg(F.min("offset")).first()[0],
            "max_offset": data.agg(F.max("offset")).first()[0]}
        assert metrics["source_rows"] == 1000 and metrics["payload_sha256"] == m["payload_sha256"]
        assert metrics["transport_ids"] == metrics["transport_rows"]
        return metrics
    try:
        first = consume()
        restart = consume()
        assert first == restart and first["transport_rows"] == 1000
        # Reenvío deliberado con los mismos IDs; no borrar estado del productor.
        _, rows = load_block(root / "block")
        producer = factory()
        try:
            for row, raw in enumerate(rows, 1):
                envelope = {"twitch_load_id": m["twitch_load_id"], "twitch_source_row": row,
                    "source_sha256": m["source_sha256"], "raw_line": raw,
                    "event_id": event_id(m["source_sha256"], row), "historical_replay": True,
                    "produced_at": utc_now()}
                producer.send(topic, key=envelope["event_id"], value=envelope).get(timeout=60)
            producer.flush(timeout=60)
        finally:
            producer.close(timeout=60)
        replay = consume()
        assert replay["transport_rows"] == 2000 and replay["source_rows"] == 1000
        report = {"status": "PASS", "scope": "LOCAL_KAFKA_SPARK_PARQUET_NOT_DATABRICKS",
            "twitch_load_id": m["twitch_load_id"], "source_sha256": m["source_sha256"],
            "topic": topic, "first": first, "restart": restart, "resend": replay,
            "elapsed_seconds": time.monotonic() - started, "spark_version": spark.version}
        save_new(root / "local-report.json", report)
        print(canonical(report))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
