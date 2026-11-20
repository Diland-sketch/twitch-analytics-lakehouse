"""Consumidor efectivo Spark Structured Streaming Kafka → SQLite transaccional."""
import json
import os
import signal
import time
import uuid
from live_monitor.common import state_dir
from live_monitor.storage import connect, writer_lock, status, commit_batch


def foreach_batch(frame, batch_id, pipeline_id):
    from pyspark.sql import functions as F
    frame.persist()
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM batches WHERE pipeline_id=? AND batch_id=?",
                        (pipeline_id, batch_id)).fetchone():
            conn.rollback()
            return
        rows = [r.asDict() for r in frame.select(
            "event_key", "run_id", "payload_hash", "valid", "errors", "streamer_username",
            "estimated_minutes", "topic", "partition", "offset").collect()]
        # Micro-lotes acotados; validar colisiones antes de deduplicar.
        seen, existing = {}, []
        for row in rows:
            key = row["event_key"]
            if key in seen and seen[key] != row["payload_hash"]:
                raise ValueError("Payloads diferentes para la misma identidad dentro del micro-lote")
            seen[key] = row["payload_hash"]
            old = conn.execute("SELECT payload_hash FROM events WHERE event_key=?", (key,)).fetchone()
            if old:
                if old[0] != row["payload_hash"]:
                    raise ValueError("Identidad existente con payload diferente")
                existing.append(key)
        new = frame.filter(~F.col("event_key").isin(existing)) if existing else frame
        # Las métricas de negocio se calculan realmente en Spark, solo sobre novedades.
        aggregates = [r.asDict() for r in new.dropDuplicates(["event_key"]).filter("valid")
            .groupBy("run_id", "streamer_username").agg(
                F.count("*").alias("interactions"), F.sum("estimated_minutes").alias("estimated_minutes")).collect()]
        offsets = [r.asDict() for r in frame.groupBy("topic", "partition").agg(
            F.min("offset").alias("first_offset"), F.max("offset").alias("last_offset"),
            F.count("*").alias("records")).collect()]
        commit_batch(conn, pipeline_id, batch_id, rows, aggregates, offsets)
        conn.commit()
        print(json.dumps({"completed_batch": batch_id, "input_rows": len(rows), "offsets": offsets}), flush=True)
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
        frame.unpersist()


def main():
    from pyspark.sql import SparkSession
    from live_monitor.quality import transform
    from kafka.admin import KafkaAdminClient
    root = state_dir()
    with writer_lock(root / "spark.lock"):
        checkpoint = root / "checkpoint"
        conn = connect()
        try:
            admin = KafkaAdminClient(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
            try:
                cluster = admin.describe_cluster()["cluster_id"]
            finally:
                admin.close()
            binding = json.dumps({"cluster": cluster, "topic": os.environ.get("KAFKA_TOPIC", "twitch.i4.historical")}, sort_keys=True)
            prior = conn.execute("SELECT value FROM metadata WHERE key='source_binding'").fetchone()
            if prior and prior[0] != binding:
                raise RuntimeError("El broker/topic cambió: conservar estado y diagnosticar, no reutilizar offsets")
            if conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] and not (checkpoint / "metadata").exists():
                raise RuntimeError("Checkpoint ausente con DB procesada; restaurar el conjunto persistente")
            conn.execute("INSERT OR IGNORE INTO metadata VALUES('source_binding',?)", (binding,))
            conn.execute("INSERT OR IGNORE INTO metadata VALUES('pipeline_id',?)", (str(uuid.uuid4()),))
            pipeline = conn.execute("SELECT value FROM metadata WHERE key='pipeline_id'").fetchone()[0]
            conn.commit()
        finally:
            conn.close()
        spark = SparkSession.builder.appName("twitch-i4-historical-monitor").config(
            "spark.sql.shuffle.partitions", "2").config("spark.sql.ansi.enabled", "true").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print(json.dumps({"spark_version": spark.version, "pipeline_id": pipeline,
                          "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
                          "scala_version": spark._jvm.scala.util.Properties.versionNumberString()}), flush=True)
        if spark.version != "3.5.8":
            raise RuntimeError("I4 requiere Spark 3.5.8 y conector Kafka 3.5.8 Scala 2.12")
        stopping = False
        def request_stop(*_):
            nonlocal stopping
            stopping = True
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        query = None
        try:
            status("STARTING")
            source = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
                .option("subscribe", os.environ.get("KAFKA_TOPIC", "twitch.i4.historical"))
                .option("startingOffsets", "earliest").option("failOnDataLoss", "true")
                .option("maxOffsetsPerTrigger", "25").load())
            query = (transform(source).writeStream.option("checkpointLocation", str(checkpoint))
                .foreachBatch(lambda frame, batch: foreach_batch(frame, batch, pipeline))
                .trigger(processingTime="2 seconds").start())
            last_progress = None
            while query.isActive and not stopping:
                status("RUNNING", json.dumps(query.status))
                if query.lastProgress and query.lastProgress["batchId"] != last_progress:
                    progress = query.lastProgress
                    with (root / "spark_progress.jsonl").open("a") as log:
                        log.write(json.dumps(progress) + "\n")
                    last_progress = progress["batchId"]
                time.sleep(2)
            if query.exception():
                raise RuntimeError(str(query.exception()))
            status("STOPPING")
            if query.isActive:
                query.stop()
            status("STOPPED")
        except BaseException as error:
            status("FAILED", str(error))
            raise
        finally:
            if query is not None and query.isActive:
                query.stop()
            spark.stop()


if __name__ == "__main__":
    main()
