"""Contratos sin dependencias Spark/Kafka; identidad fuente independiente del replay."""
import hashlib
import json
import uuid

VERSION = "twitch-kafka-block-v1"
PILOT_ROWS = 1000


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def event_id(source_sha256, row):
    return digest(f"{source_sha256}|{row}")


def validate_manifest(m):
    import re
    if m.get("version") != VERSION or m.get("count") != PILOT_ROWS or m.get("start_row") != 1:
        raise ValueError("Esta entrega admite solamente el piloto inicial de 1000 filas desde 1")
    for field in ("source_sha256", "payload_sha256"):
        if not re.fullmatch("[a-f0-9]{64}", m.get(field, "")):
            raise ValueError("Digest inválido")
    if str(uuid.UUID(m["twitch_load_id"])) != m["twitch_load_id"]:
        raise ValueError("load_id inválido")
    if not re.fullmatch(r"twitch[._-]pilot[._-][A-Za-z0-9._-]+", m.get("topic", "")):
        raise ValueError("Usar un topic dedicado twitch.pilot.<nombre>")
    if not re.fullmatch("[a-f0-9]{64}", m.get("source_cluster_id", "")):
        raise ValueError("source_cluster_id debe ser SHA256 de una identidad estable del broker")
    return m


def verify_report(m, report):
    """No basta un SUCCESS de Jobs ni un número enviado por el productor."""
    validate_manifest(m)
    expected = {"twitch_load_id": m["twitch_load_id"], "source_sha256": m["source_sha256"],
                "payload_sha256": m["payload_sha256"], "source_rows": m["count"],
                "first_source_row": 1, "last_source_row": m["count"], "status": "PASS",
                "topic": m["topic"], "source_cluster_id": m["source_cluster_id"],
                "identity_errors": 0, "payload_conflicts": 0, "transport_duplicates": 0}
    if any(report.get(k) != v for k, v in expected.items()):
        raise RuntimeError("Bronze no concilia con el manifiesto; cursor conservado")
    if report.get("transport_rows", 0) < m["count"] or not report.get("audit_table"):
        raise RuntimeError("Falta evidencia persistida Bronze")
    if report.get("foreign_rows", 0) != 0 or not report["audit_table"].startswith("workspace.twitch_kafka_experimental.audit_"):
        raise RuntimeError("Evidencia fuera del destino o bloque esperado")
    offsets = report.get("offsets", [])
    if not offsets or any(p["offset_count"] != p["max_offset"] - p["min_offset"] + 1 for p in offsets):
        raise RuntimeError("Cobertura de offsets no contigua")
