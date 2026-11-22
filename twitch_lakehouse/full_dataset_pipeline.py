"""First-million experimental Delta pipeline, isolated from the I1 schema."""
from dataclasses import dataclass
import json
import re
from twitch_lakehouse.config import identifier
from twitch_lakehouse.storage import require_absent, write_new

@dataclass(frozen=True)
class FullSettings:
    catalog: str = "workspace"
    schema: str = "twitch_full_experimental"
    volume: str = "fragments_1m"

    def __post_init__(self):
        for item in (self.catalog, self.schema, self.volume):
            identifier(item)
        if self.schema == "twitch_lakehouse":
            raise ValueError("Never use the I1/I3 schema")

    @property
    def namespace(self):
        return f"{self.catalog}.{self.schema}"

    @property
    def root(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    def table(self, name):
        return f"{self.namespace}.{identifier(name)}"


def validate_manifest(source, expected_rows=1_000_000):
    obj = json.loads(source) if isinstance(source, str) else source
    if not isinstance(obj, dict) or obj.get("schema_version") != "twitch-fragments-v1":
        raise ValueError("Invalid fragment manifest")
    if (type(obj.get("requested_rows")) is not int or
        obj["requested_rows"] != expected_rows or
        obj.get("extracted_rows") != expected_rows):
        raise ValueError("Unexpected source population")
    if not re.fullmatch(r"[0-9a-f]{64}", str(obj.get("source_sha256", ""))):
        raise ValueError("Source checksum missing")
    parts = obj.get("parts")
    if not isinstance(parts, list) or not parts:
        raise ValueError("Missing fragments")
    prior = 0
    for index, part in enumerate(parts):
        if (not isinstance(part, dict) or
            part.get("file") != f"part-{index:06d}.csv" or
            type(part.get("rows")) is not int or part["rows"] <= 0 or
            part.get("first_row") != prior+1 or
            part.get("last_row") != prior+part["rows"] or
            type(part.get("bytes")) is not int or part["bytes"] <= 0 or
            not re.fullmatch(r"[0-9a-f]{64}", str(part.get("sha256", "")))):
            raise ValueError("Invalid fragment metadata/order")
        prior = part["last_row"]
    if prior != expected_rows:
        raise ValueError("Fragment rows do not reconcile")
    return obj


def bootstrap(spark, cfg):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.namespace}").collect()
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.namespace}.{cfg.volume}").collect()
    return cfg.root


def bronze(spark, cfg, manifest):
    from pyspark.sql import functions as F
    manifest = validate_manifest(manifest)
    target = cfg.table("bronze_interactions_1m")
    require_absent(spark, target)
    frames = []
    for part in manifest["parts"]:
        path = f"{cfg.root}/{part['file']}"
        frame = spark.read.text(path).withColumnRenamed("value", "raw_line")
        actual = frame.count()
        if actual != part["rows"]:
            raise ValueError(f"File {part['file']}: expected {part['rows']}, got {actual}")
        frames.append(frame.withColumn("source_file", F.lit(path)).withColumn("ingested_at", F.current_timestamp()))
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    if combined.count() != manifest["extracted_rows"]:
        raise ValueError("Global Bronze count mismatch")
    write_new(spark, combined, target)
    actual = spark.table(target).count()
    if actual != manifest["extracted_rows"]:
        raise RuntimeError("Persisted Bronze count mismatch")
    return {"source_rows": manifest["extracted_rows"], "bronze_rows": actual, "table": target}


def silver(spark, cfg, expected_rows=1_000_000):
    from twitch_lakehouse.transforms import silver_frames
    from twitch_lakehouse.pipeline import silver_metrics
    target = cfg.table("silver_interactions_1m")
    quarantine = cfg.table("silver_rejects_1m")
    require_absent(spark, target, quarantine)
    source = spark.table(cfg.table("bronze_interactions_1m"))
    if source.count() != expected_rows:
        raise ValueError("Bronze source population changed")
    accepted, rejected, valid = silver_frames(source)
    metrics = silver_metrics(source, accepted, rejected, valid)
    write_new(spark, rejected, quarantine)
    write_new(spark, accepted, target)
    if (spark.table(target).count() != metrics["silver_rows"] or
        spark.table(quarantine).count() != metrics["rejected_rows"]):
        raise RuntimeError("Persisted Silver counts mismatch")
    return metrics


def gold(spark, cfg):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import gold_by_streamer
    target = cfg.table("gold_streamer_metrics_1m")
    require_absent(spark, target)
    source = spark.table(cfg.table("silver_interactions_1m"))
    output = gold_by_streamer(source)
    expected = source.agg(F.count("*").alias("n"), F.sum("estimated_minutes").alias("minutes")).first()
    actual = output.agg(F.sum("interactions").alias("n"), F.sum("estimated_minutes").alias("minutes")).first()
    if expected["n"] < 1 or tuple(expected) != tuple(actual):
        raise ValueError("Gold/Silver aggregate mismatch")
    write_new(spark, output, target)
    persisted = spark.table(target).agg(F.sum("interactions").alias("n"), F.sum("estimated_minutes").alias("minutes")).first()
    if tuple(persisted) != tuple(expected):
        raise RuntimeError("Persisted Gold mismatch")
    return {"streamers": spark.table(target).count(), "interactions": expected["n"], "minutes": expected["minutes"]}


def audit(spark, cfg, expected_rows=1_000_000):
    from pyspark.sql import functions as F
    from twitch_lakehouse.transforms import KEY
    b = spark.table(cfg.table("bronze_interactions_1m"))
    s = spark.table(cfg.table("silver_interactions_1m"))
    r = spark.table(cfg.table("silver_rejects_1m"))
    g = spark.table(cfg.table("gold_streamer_metrics_1m"))
    bc, sc, rc = b.count(), s.count(), r.count()
    if bc != expected_rows or sc < 1 or sc + rc > bc:
        raise ValueError("Invalid population")
    if s.select(*KEY).distinct().count() != sc:
        raise ValueError("Silver key duplication")
    minutes = s.agg(F.sum("estimated_minutes").alias("m")).first()["m"]
    gs = g.agg(F.sum("interactions").alias("n"), F.sum("estimated_minutes").alias("m")).first()
    if (gs["n"], gs["m"]) != (sc, minutes):
        raise ValueError("Gold mismatch")
    return {"status":"PASS", "bronze":bc, "silver":sc, "rejected":rc,
            "duplicates_inferred":bc-sc-rc, "gold_interactions":gs["n"],
            "gold_streamers":g.count(), "estimated_minutes":minutes}
