"""Auditor del piloto binario: cobertura fuente independiente de metadata Kafka."""
from collections import Counter, defaultdict
import json
from kafka_ingest.contracts import canonical, digest, event_id, validate_manifest


def audit_records(records, manifest, metadata_column="kafka_source"):
    """Solo piloto de 1000 filas, máximo 5000 filas Bronze; no usar para millones.

    key/value son bytes reales de Delta. Nunca se fabrican topic/offset/partition
    a partir del ordinal fuente ni de los ACK del productor.
    """
    m = validate_manifest(manifest)
    if len(records) > 5000:
        raise ValueError("Auditor limitado a 5000 filas Bronze del piloto")
    expected = {event_id(m["source_sha256"], i) for i in range(1, 1001)}
    ids = Counter()
    source_payloads = defaultdict(set)
    event_payloads = defaultdict(set)
    actual_ids = set()
    invalid = 0
    malformed = 0
    key_errors = 0
    field_errors = Counter()
    for record in records:
        try:
            value = record["value"]
            e = json.loads(bytes(value).decode("utf-8"))
            if not isinstance(e, dict):
                raise ValueError("Objeto JSON requerido")
        except (ValueError, TypeError, KeyError, UnicodeError):
            malformed += 1
            invalid += 1
            continue
        eid, row, raw = e.get("event_id"), e.get("twitch_source_row"), e.get("raw_line")
        field_errors["source_sha256_errors"] += e.get("source_sha256") != m["source_sha256"]
        field_errors["load_id_errors"] += e.get("twitch_load_id") != m["twitch_load_id"]
        row_valid = type(row) is int and 1 <= row <= 1000
        field_errors["source_row_errors"] += not row_valid
        field_errors["event_id_errors"] += not (row_valid and eid == event_id(m["source_sha256"], row))
        if isinstance(eid, str):
            ids[eid] += 1
            actual_ids.add(eid)
            event_payloads[eid].add(canonical(raw))
        if type(row) is int:
            source_payloads[row].add(canonical(raw))
        valid = (type(row) is int and 1 <= row <= 1000
            and e.get("source_sha256") == m["source_sha256"]
            and e.get("twitch_load_id") == m["twitch_load_id"]
            and eid == event_id(m["source_sha256"], row)
            and isinstance(raw, str) and e.get("historical_replay") is True)
        if not valid:
            invalid += 1
        try:
            key_matches = bytes(record["key"]).decode("utf-8") == eid
        except (KeyError, TypeError, UnicodeError):
            key_matches = False
        if not key_matches:
            key_errors += 1
    conflicts = sum(len(values) > 1 for values in source_payloads.values())
    event_conflicts = sum(len(values) > 1 for values in event_payloads.values())
    duplicate_ids = sum(n - 1 for n in ids.values())
    ordered = [json.loads(next(iter(source_payloads[i]))) for i in range(1, 1001)
               if len(source_payloads.get(i, ())) == 1]
    payload_sha = digest(canonical(ordered)) if len(ordered) == 1000 else None
    coverage = (actual_ids == expected and set(source_payloads) == set(range(1, 1001))
        and not invalid and not key_errors and not conflicts and not event_conflicts
        and payload_sha == m["payload_sha256"])
    report = {"audit_contract": "twitch-native-source-audit-v2",
        "status": "PASS" if coverage and not duplicate_ids else "FAIL",
        "status_scope": "SOURCE_IDENTITY_AND_CONTENT_ONLY",
        "source_coverage_status": "PASS" if coverage else "FAIL",
        "identity_integrity_status": "PASS" if not invalid and not duplicate_ids and not key_errors and not conflicts and not event_conflicts else "FAIL",
        "twitch_load_id": m["twitch_load_id"], "source_sha256": m["source_sha256"],
        "bronze_rows": len(records), "source_rows": len(source_payloads),
        "first_source_row": min(source_payloads) if source_payloads else None,
        "last_source_row": max(source_payloads) if source_payloads else None,
        "missing_source_rows": len(set(range(1, 1001)) - set(source_payloads)),
        "distinct_event_ids": len(actual_ids), "duplicate_event_id_rows": duplicate_ids,
        "identity_errors": invalid, "malformed_payload_rows": malformed,
        "key_identity_errors": key_errors, "payload_conflicts": conflicts,
        "event_payload_conflicts": event_conflicts,
        "missing_event_ids": len(expected - actual_ids), "unexpected_event_ids": len(actual_ids - expected),
        "missing_event_id_sample": sorted(expected - actual_ids)[:20],
        "unexpected_event_id_sample": sorted(actual_ids - expected)[:20],
        "expected_event_ids_sha256": digest(canonical(sorted(expected))),
        "observed_event_ids_sha256": digest(canonical(sorted(actual_ids))),
        "payload_sha256": payload_sha, "expected_payload_sha256": m["payload_sha256"],
        "offset_audit_status": "NO VERIFICABLE", "offsets": None,
        "transport_duplicates": None,
        "offset_audit_reason": "Metadata Kafka ausente o incompleta en Bronze; los ACK no sustituyen la observación del consumidor"}
    report.update({name: field_errors[name] for name in
        ("source_sha256_errors", "load_id_errors", "source_row_errors", "event_id_errors")})
    metadata = [r.get(metadata_column) for r in records]
    if records and all(isinstance(p, dict) and all(p.get(k) is not None for k in ("topic", "partition", "offset")) for p in metadata):
        valid_metadata = all(p["topic"] == m["topic"] and type(p["partition"]) is int
            and p["partition"] >= 0 and type(p["offset"]) is int and p["offset"] >= 0 for p in metadata)
        if valid_metadata:
            partitions = defaultdict(list)
            for p in metadata:
                partitions[p["partition"]].append(p["offset"])
            ranges = [{"partition": p, "min_offset": min(v), "max_offset": max(v),
                "offset_count": len(set(v))} for p, v in sorted(partitions.items())]
            duplicates = len(metadata) - len({(p["topic"], p["partition"], p["offset"]) for p in metadata})
            contiguous = all(p["offset_count"] == p["max_offset"] - p["min_offset"] + 1 for p in ranges)
            report.update(offset_audit_status="PASS" if not duplicates and contiguous else "FAIL",
                offsets=ranges, transport_duplicates=duplicates,
                offset_audit_reason="Solo offsets realmente persistidos; comparación con ACK es un control adicional")
        else:
            report.update(offset_audit_status="FAIL", offset_audit_reason="Metadata Kafka persistida inválida o topic distinto")
    return report


def audit_native(native, manifest):
    """Spark Connect: proyección binaria y lectura acotada de una versión Delta fija."""
    if not {"key", "value"}.issubset(set(native.columns)):
        raise ValueError("Se requieren key y value en Bronze")
    if any(native.schema[name].dataType.simpleString() != "binary" for name in ("key", "value")):
        raise ValueError("El contrato del piloto requiere key/value BINARY")
    columns = ["key", "value"] + (["kafka_source"] if "kafka_source" in native.columns else [])
    records = [r.asDict(recursive=True) for r in native.select(*columns).limit(5001).collect()]
    return audit_records(records, manifest)
