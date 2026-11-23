"""Bronze por rangos inmutables, Silver/Gold globales y publicación por hito."""
from dataclasses import dataclass, asdict
import json
import re
import time
from bulk_lakehouse.source import save
from kafka_ingest.contracts import canonical, digest
from twitch_lakehouse.storage import write_new

NS = "workspace.twitch_bulk_experimental"
VOLUME = NS + ".source_parts"
ROOT = "/Volumes/workspace/twitch_bulk_experimental/source_parts"
MILESTONES = {100_000: "100k", 1_000_000: "1m", 10_000_000: "10m", 50_000_000: "50m"}
STAGES = ("verify", "bronze", "silver", "gold", "publish")


@dataclass(frozen=True)
class Config:
    rows: int
    release: str
    manifest_path: str
    predecessor: str = ""
    max_input_bytes: int = 128 * 1024 * 1024

    def __post_init__(self):
        if self.rows not in MILESTONES or not re.fullmatch(f"bulk_{MILESTONES[self.rows]}_r[0-9]{{3}}", self.release):
            raise ValueError("Hito/release inválidos")
        if not re.fullmatch(re.escape(ROOT) + r"/[a-z0-9_]+/manifest\.json", self.manifest_path):
            raise ValueError("Manifiesto fuera del volumen experimental")
        if self.rows > 100_000 and not re.fullmatch(re.escape(NS) + r"\.publication_bulk_[a-z0-9_]+", self.predecessor):
            raise ValueError("Hito previo CONFIRMED obligatorio")
        if type(self.max_input_bytes) is not int or self.max_input_bytes < 1:
            raise ValueError("Límite físico de entrada requerido")

    def table(self, stage):
        if stage not in ("manifest", "input_audit", "bronze_ledger", "rejects", "silver", "silver_audit", "gold", "gold_audit", "publication"):
            raise ValueError("Destino no permitido")
        return f"{NS}.{stage}_{self.release}"

    @property
    def fingerprint(self):
        return digest(canonical(asdict(self)))


def validate_manifest(m, expected_rows):
    if (m.get("schema_version") != "twitch-bulk-parquet-v1" or m.get("requested_rows") != expected_rows
        or m.get("extracted_rows") != expected_rows or not re.fullmatch(r"[a-f0-9]{64}", m.get("source_sha256", ""))):
        raise ValueError("Contrato de manifiesto/población inválido")
    prior = 0
    for index, p in enumerate(m.get("parts", [])):
        if (p.get("file") != f"part-{index:06d}.parquet" or type(p.get("rows")) is not int
            or p["rows"] != 100_000 or p.get("first_row") != prior + 1 or p.get("last_row") != prior + p["rows"]
            or type(p.get("bytes")) is not int or p["bytes"] <= 0
            or not re.fullmatch(r"[a-f0-9]{64}", p.get("sha256", ""))):
            raise ValueError("Rango, archivo o hash de fragmento inválido")
        prior = p["last_row"]
    if prior != expected_rows:
        raise ValueError("Los rangos globales no cubren exactamente el hito")
    return m


def read_record(spark, table):
    rows = spark.table(table).select("report_json").limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Reporte debe ser único")
    return json.loads(rows[0][0])


def put_record(spark, table, value):
    if spark.catalog.tableExists(table):
        if read_record(spark, table) != value:
            raise ValueError("Reporte inmutable distinto; no sobrescribir")
    else:
        write_new(spark, spark.createDataFrame([(canonical(value),)], "report_json STRING"), table)


def load_manifest(spark, cfg):
    # text() applies its own wholetext=False default after reader options.
    # Pass the argument directly so pretty-printed JSON remains one record.
    rows = spark.read.text(cfg.manifest_path, wholetext=True).collect()
    if len(rows) != 1:
        raise ValueError("Se requiere exactamente un manifiesto")
    return validate_manifest(json.loads(rows[0][0]), cfg.rows)


def gate(spark, cfg):
    audit = read_record(spark, cfg.table("input_audit"))
    if audit.get("status") != "PASS" or audit.get("config_sha256") != cfg.fingerprint:
        raise ValueError("Entrada no autorizada")
    m = load_manifest(spark, cfg)
    if audit["manifest_sha256"] != digest(canonical(m)):
        raise ValueError("Manifiesto cambió tras autorización")
    return m


def verify(spark, cfg):
    from pyspark.sql import functions as F
    start = time.monotonic()
    m = load_manifest(spark, cfg)
    if sum(p["bytes"] for p in m["parts"]) > cfg.max_input_bytes:
        raise ValueError("Hito supera capacidad de entrada revisada; detener antes de cómputo masivo")
    if cfg.rows > 100_000:
        previous = read_record(spark, cfg.predecessor)
        expected = {1_000_000: 100_000, 10_000_000: 1_000_000, 50_000_000: 10_000_000}[cfg.rows]
        if (previous.get("publication_state") != "CONFIRMED" or previous.get("status") != "PASS"
            or previous.get("source_sha256") != m["source_sha256"] or previous.get("rows") != expected):
            raise ValueError("Hito previo no autoriza el siguiente")
    root = cfg.manifest_path.rsplit("/", 1)[0]
    files = spark.read.format("binaryFile").load([f"{root}/{p['file']}" for p in m["parts"]])
    actual = files.select(F.regexp_extract("path", r"([^/]+)$", 1).alias("file"), "length",
        F.sha2("content", 256).alias("sha256")).collect()
    observed = {r["file"]: (r["length"], r["sha256"]) for r in actual}
    wanted = {p["file"]: (p["bytes"], p["sha256"]) for p in m["parts"]}
    if len(actual) != len(wanted) or observed != wanted:
        raise ValueError("Bytes/hash de archivos remotos no coinciden")
    record = {"status": "PASS", "config": asdict(cfg), "config_sha256": cfg.fingerprint,
        "manifest_sha256": digest(canonical(m)), "manifest": m, "remote_files": len(actual)}
    # Duración se registra aparte para que Repair no cambie el contrato inmutable.
    put_record(spark, cfg.table("input_audit"), record)
    return {"status": "PASS", "elapsed_seconds": time.monotonic() - start, "files": len(actual)}


def blocks(m):
    # Preservar 100k iniciales, añadir 900k, después bloques de 1M; evita reescribir prefijos.
    result, current, total = [], [], 0
    for p in m["parts"]:
        current.append(p)
        total += p["rows"]
        if total == 100_000 or total % 1_000_000 == 0:
            result.append(current)
            current = []
    if current:
        raise ValueError("Bloque incompleto")
    return result


def block_table(m, parts):
    return f"{NS}.bronze_{m['source_sha256'][:12]}_{parts[0]['first_row']}_{parts[-1]['last_row']}"


def receipt_table(table):
    return table.replace(".bronze_", ".receipt_", 1)


def current_version(spark, table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def block_frame(spark, cfg, m, parts):
    from pyspark.sql import functions as F
    root = cfg.manifest_path.rsplit("/", 1)[0]
    return (spark.read.schema("source_row LONG, raw_line STRING, raw_sha256 STRING")
        .parquet(*[f"{root}/{p['file']}" for p in parts])
        .withColumn("source_sha256", F.lit(m["source_sha256"]))
        .withColumn("event_id", F.sha2(F.concat_ws("|", "source_sha256", "source_row"), 256)))


def block_stats(df):
    from pyspark.sql import functions as F
    r = df.agg(F.count("*").alias("rows"), F.countDistinct("source_row").alias("distinct_rows"),
        F.min("source_row").alias("first_row"), F.max("source_row").alias("last_row"),
        F.sum(F.when(~F.coalesce(F.col("raw_sha256") == F.sha2("raw_line", 256), F.lit(False)), 1).otherwise(0)).alias("hash_errors")).first()
    return r.asDict()


def bronze(spark, cfg):
    start = time.monotonic()
    m = gate(spark, cfg)
    receipts = []
    for parts in blocks(m):
        table = block_table(m, parts)
        expected = {"source_sha256": m["source_sha256"], "parts": parts,
            "first_row": parts[0]["first_row"], "last_row": parts[-1]["last_row"],
            "rows": sum(p["rows"] for p in parts), "table": table}
        checkpoint = receipt_table(table)
        if spark.catalog.tableExists(checkpoint):
            saved = read_record(spark, checkpoint)
            if saved["input"] != expected or saved["status"] != "PASS" or saved["delta_version"] != current_version(spark, table):
                raise ValueError("Checkpoint lógico no coincide; conservar y detener")
        else:
            frame = block_frame(spark, cfg, m, parts)
            if spark.catalog.tableExists(table):
                stored = spark.table(table)
                if stored.columns != frame.columns or stored.exceptAll(frame).limit(1).count() or frame.exceptAll(stored).limit(1).count():
                    raise ValueError("Bronze parcial distinto; no reparar mediante append")
            else:
                write_new(spark, frame.repartition(2 if expected["rows"] <= 100_000 else 4), table)
            stats = block_stats(spark.table(table))
            if (stats["rows"], stats["distinct_rows"], stats["first_row"], stats["last_row"], stats["hash_errors"]) != (expected["rows"], expected["rows"], expected["first_row"], expected["last_row"], 0):
                raise ValueError("Cobertura Bronze persistida FAIL")
            detail = spark.sql(f"DESCRIBE DETAIL {table}").select("sizeInBytes", "numFiles").first().asDict()
            saved = {"status": "PASS", "input": expected, "stats": stats,
                "delta_version": current_version(spark, table), "delta_detail": detail}
            put_record(spark, checkpoint, saved)
        receipts.append(saved)
    put_record(spark, cfg.table("bronze_ledger"), {"status": "PASS", "config_sha256": cfg.fingerprint,
        "rows": cfg.rows, "source_sha256": m["source_sha256"], "receipts": receipts})
    return {"status": "PASS", "rows": cfg.rows, "blocks": len(receipts), "elapsed_seconds": time.monotonic() - start}


def bronze_union(spark, cfg):
    m = gate(spark, cfg)
    ledger = read_record(spark, cfg.table("bronze_ledger"))
    if ledger["status"] != "PASS" or ledger["config_sha256"] != cfg.fingerprint:
        raise ValueError("Bronze no está conciliada")
    result = None
    for entry in ledger["receipts"]:
        df = spark.read.option("versionAsOf", entry["delta_version"]).table(entry["input"]["table"])
        result = df if result is None else result.unionByName(df)
    return result


def persist_frame(spark, df, table):
    if spark.catalog.tableExists(table):
        previous = spark.table(table)
        if previous.columns != df.columns or previous.exceptAll(df).limit(1).count() or df.exceptAll(previous).limit(1).count():
            raise ValueError("Destino de release parcial/distinto; detener sin sobrescribir")
    else:
        write_new(spark, df, table)


def silver(spark, cfg):
    from twitch_lakehouse.transforms import silver_frames
    start = time.monotonic()
    source = bronze_union(spark, cfg)
    accepted, rejected, valid = silver_frames(source)
    persist_frame(spark, rejected, cfg.table("rejects"))
    persist_frame(spark, accepted, cfg.table("silver"))
    rejected_rows, valid_rows, silver_rows = rejected.count(), valid.count(), spark.table(cfg.table("silver")).count()
    if valid_rows + rejected_rows != cfg.rows or silver_rows > valid_rows:
        raise ValueError("Balance Silver FAIL")
    report = {"status": "PASS", "config_sha256": cfg.fingerprint, "rows_read": cfg.rows,
        "valid_rows": valid_rows, "rejected_rows": rejected_rows, "business_duplicates": valid_rows - silver_rows,
        "silver_rows": silver_rows, "silver_version": current_version(spark, cfg.table("silver")),
        "rejects_version": current_version(spark, cfg.table("rejects"))}
    put_record(spark, cfg.table("silver_audit"), report)
    return {**report, "elapsed_seconds": time.monotonic() - start}


def gold(spark, cfg):
    from twitch_lakehouse.transforms import gold_by_streamer
    start = time.monotonic()
    s = read_record(spark, cfg.table("silver_audit"))
    if s["status"] != "PASS" or s["config_sha256"] != cfg.fingerprint:
        raise ValueError("Silver no autorizada")
    result = gold_by_streamer(spark.table(cfg.table("silver")))
    persist_frame(spark, result, cfg.table("gold"))
    put_record(spark, cfg.table("gold_audit"), {"status": "PASS", "config_sha256": cfg.fingerprint,
        "gold_rows": spark.table(cfg.table("gold")).count(), "gold_version": current_version(spark, cfg.table("gold"))})
    return {"status": "PASS", "elapsed_seconds": time.monotonic() - start}


def publish(spark, cfg):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import KEY, gold_by_streamer
    start = time.monotonic()
    m = gate(spark, cfg)
    s = read_record(spark, cfg.table("silver_audit"))
    g = read_record(spark, cfg.table("gold_audit"))
    if any(r.get("status") != "PASS" or r.get("config_sha256") != cfg.fingerprint for r in (s, g)):
        raise ValueError("Silver/Gold no están autorizadas")
    if (s["silver_version"] != current_version(spark, cfg.table("silver"))
        or s["rejects_version"] != current_version(spark, cfg.table("rejects"))
        or g["gold_version"] != current_version(spark, cfg.table("gold"))):
        raise ValueError("Un destino cambió después de su auditoría")
    accepted, rejected, actual = spark.table(cfg.table("silver")), spark.table(cfg.table("rejects")), spark.table(cfg.table("gold"))
    totals = actual.agg(F.coalesce(F.sum("interactions"), F.lit(0)).alias("interactions"),
        F.coalesce(F.sum("estimated_minutes"), F.lit(0)).alias("minutes")).first()
    minutes = accepted.agg(F.coalesce(F.sum("estimated_minutes"), F.lit(0))).first()[0]
    expected = gold_by_streamer(accepted)
    checks = {"source_balance": s["valid_rows"] + s["rejected_rows"] == cfg.rows,
        "silver_balance": s["silver_rows"] + s["business_duplicates"] == s["valid_rows"],
        "silver_unique_keys": accepted.select(*KEY).distinct().count() == s["silver_rows"],
        "rejects_persisted": rejected.count() == s["rejected_rows"],
        "gold_interactions": totals["interactions"] == s["silver_rows"], "gold_minutes": totals["minutes"] == minutes,
        "gold_exact": not actual.exceptAll(expected).limit(1).count() and not expected.exceptAll(actual).limit(1).count()}
    detail = {name: spark.sql(f"DESCRIBE DETAIL {cfg.table(name)}").select("sizeInBytes", "numFiles").first().asDict()
        for name in ("silver", "rejects", "gold")}
    ledger = read_record(spark, cfg.table("bronze_ledger"))
    detail["bronze"] = {"sizeInBytes": sum(r["delta_detail"]["sizeInBytes"] for r in ledger["receipts"]),
        "numFiles": sum(r["delta_detail"]["numFiles"] for r in ledger["receipts"])}
    report = {"status": "PASS" if all(checks.values()) else "FAIL", "publication_state": "CONFIRMED" if all(checks.values()) else "FAILED",
        "config": asdict(cfg), "config_sha256": cfg.fingerprint, "rows": cfg.rows, "source_sha256": m["source_sha256"],
        "route": "SPARK_BATCH_NOT_KAFKA", "silver": s, "gold_streamers": actual.count(),
        "gold_interactions": totals["interactions"], "estimated_minutes": minutes, "checks": checks,
        "delta_details": detail, "elapsed_publication_seconds": time.monotonic() - start}
    # Tiempo variable queda fuera del artefacto idempotente de publicación.
    elapsed = report.pop("elapsed_publication_seconds")
    put_record(spark, cfg.table("publication"), report)
    if report["status"] != "PASS":
        raise ValueError("Publicación FAIL; evidencia conservada")
    return {**report, "elapsed_publication_seconds": elapsed}


def execute(spark, cfg, stage):
    if stage not in STAGES:
        raise ValueError("Etapa no permitida")
    return {"verify": verify, "bronze": bronze, "silver": silver, "gold": gold, "publish": publish}[stage](spark, cfg)
