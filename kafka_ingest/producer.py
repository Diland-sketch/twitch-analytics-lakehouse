"""Preparar un bloque real y publicar con ACK persistidos, sin tocar el productor I4."""
import gzip
import json
import os
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
from live_monitor.common import source_digest
from kafka_ingest.contracts import VERSION, PILOT_ROWS, canonical, digest, event_id, validate_manifest


def save_new(path, value):
    with Path(path).open("x", encoding="utf-8") as target:
        target.write(canonical(value))
        target.flush()
        os.fsync(target.fileno())


def prepare(source, directory, topic, cluster_name):
    source = Path(source)
    root = Path(directory)
    # Un directorio nuevo distingue fuentes/bloques y conserva intentos anteriores.
    if root.exists():
        raise ValueError("El directorio del bloque ya existe; usar resume, no preparar de nuevo")
    source_sha = source_digest(source)
    opener = gzip.open if source.suffix == ".gz" else open
    rows = []
    with opener(source, "rt", encoding="utf-8", newline="") as stream:
        for ordinal, line in enumerate(stream, 1):
            rows.append(line.rstrip("\r\n"))
            if ordinal == PILOT_ROWS:
                break
    if len(rows) != PILOT_ROWS:
        raise ValueError("Se requieren 1000 líneas reales; no se publica nada")
    m = validate_manifest({"version": VERSION, "twitch_load_id": str(uuid.uuid4()),
        "source_name": source.name, "source_sha256": source_sha, "start_row": 1,
        "count": len(rows), "payload_sha256": digest(canonical(rows)), "topic": topic,
        "source_cluster_id": digest(cluster_name)})
    root.mkdir(parents=True)
    save_new(root / "payload.json", rows)
    save_new(root / "manifest.json", m)
    return m


def load_block(directory):
    root = Path(directory)
    m = validate_manifest(json.loads((root / "manifest.json").read_text(encoding="utf-8")))
    rows = json.loads((root / "payload.json").read_text(encoding="utf-8"))
    if len(rows) != m["count"] or digest(canonical(rows)) != m["payload_sha256"]:
        raise ValueError("Payload local alterado; detener")
    return m, rows


def kafka_factory():
    from kafka import KafkaProducer
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    if mechanism not in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        raise ValueError("Mecanismo SASL no soportado")
    return KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        security_protocol="SASL_SSL", sasl_mechanism=mechanism,
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
        ssl_cafile=os.environ.get("KAFKA_SSL_CA_FILE"), ssl_check_hostname=True,
        acks="all", enable_idempotence=True, retries=5,
        key_serializer=lambda x: x.encode(),
        value_serializer=lambda x: canonical(x).encode())


def publish(directory, factory=kafka_factory, rate=100.0):
    if rate <= 0:
        raise ValueError("rate debe ser positivo")
    root = Path(directory)
    m, rows = load_block(root)
    if (root / "acked.json").exists():
        return json.loads((root / "acked.json").read_text())
    producer = factory()
    started = time.monotonic()
    try:
        # En una caída con ACK incompleto se reenvía TODO el bloque con los mismos IDs.
        # La idempotencia Kafka no garantiza dedup entre procesos: Bronze los audita.
        with (root / "acks.jsonl").open("a", encoding="utf-8") as log:
            for ordinal, raw in enumerate(rows, 1):
                if (root / "STOP").exists():
                    raise RuntimeError("Reproducción detenida; cursor conservado")
                eid = event_id(m["source_sha256"], ordinal)
                record = {"twitch_load_id": m["twitch_load_id"], "twitch_source_row": ordinal,
                    "source_sha256": m["source_sha256"], "event_id": eid,
                    "historical_replay": True, "raw_line": raw,
                    "produced_at": datetime.now(timezone.utc).isoformat()}
                ack = producer.send(m["topic"], key=eid, value=record).get(timeout=60)
                log.write(canonical({"source_row": ordinal, "event_id": eid,
                    "topic": ack.topic, "partition": ack.partition, "offset": ack.offset}) + "\n")
                log.flush()
                os.fsync(log.fileno())
                time.sleep(1 / rate)
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=60)
    result = {"acknowledged": len(rows), "elapsed_seconds": time.monotonic() - started}
    save_new(root / "acked.json", result)
    return result
