"""DAG batch con evidencia inmutable, versiones explícitas y Repair fail-closed."""
from dataclasses import dataclass, asdict
import json
import re
from kafka_ingest.bronze import NAMESPACE
from kafka_ingest.contracts import canonical, digest, validate_manifest
from kafka_ingest.medallion import BRONZE, Release, STAGES, frames, reconcile, validate_authorization
from kafka_ingest.native_audit import audit_native
from twitch_lakehouse.storage import require_absent, write_new

STEPS = ("verify_bronze", "authorize", "transform", "reconcile", "publish")
PARAMETERS = ("release_id", "bronze_version", "manifest_table", "manifest_version", "population", "code_commit")


@dataclass(frozen=True)
class Context:
    release_id: str
    bronze_version: int
    manifest_table: str
    manifest_version: int
    population: str
    code_commit: str
    job_id: str
    job_run_id: str

    def __post_init__(self):
        Release(self.release_id)
        if self.release_id == "kafka_b7_r001":
            raise ValueError("Release manual confirmada protegida")
        if type(self.bronze_version) is not int or type(self.manifest_version) is not int or min(self.bronze_version, self.manifest_version) < 0:
            raise ValueError("Versiones Delta explícitas no negativas")
        if self.release_id.split("_")[1] != f"b{self.bronze_version}":
            raise ValueError("Release y versión Bronze no concuerdan")
        if not re.fullmatch(re.escape(NAMESPACE) + r"\.[a-z][a-z0-9_]*", self.manifest_table):
            raise ValueError("Manifiesto fuera del esquema experimental")
        if self.population not in ("whole_snapshot", "load_id"):
            raise ValueError("Población explícita requerida")
        if not re.fullmatch(r"[0-9a-f]{40}", self.code_commit):
            raise ValueError("Se requiere commit Git inmutable")
        if not all(re.fullmatch(r"[1-9][0-9]*", v) for v in (self.job_id, self.job_run_id)):
            raise ValueError("Solo ejecutar etapas con contexto real de Job")

    @property
    def release(self):
        return Release(self.release_id)

    @property
    def fingerprint(self):
        return digest(canonical(asdict(self)))

    def artifact(self, kind):
        if kind not in ("bronze_audit", "authorization", "publication"):
            raise ValueError("Artefacto no permitido")
        return f"{NAMESPACE}.job_{kind}_{self.release_id}"


def record(spark, table):
    version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
    rows = spark.read.option("versionAsOf", version).table(table).select("report_json").limit(2).collect()
    if len(rows) != 1:
        raise ValueError("Artefacto debe contener exactamente un reporte")
    return json.loads(rows[0][0]), version


def put_record(spark, table, value):
    if spark.catalog.tableExists(table):
        previous, _ = record(spark, table)
        if previous != value:
            raise ValueError("Artefacto existente distinto: conservar y detener")
    else:
        write_new(spark, spark.createDataFrame([(canonical(value),)], "report_json STRING"), table)


def manifest(spark, ctx):
    rows = (spark.read.option("versionAsOf", ctx.manifest_version).table(ctx.manifest_table)
        .select("binding_json").limit(2).collect())
    if len(rows) != 1:
        raise ValueError("Manifiesto/binding debe tener una fila")
    binding = json.loads(rows[0][0])
    validate_manifest(binding["manifest"])
    return binding


def population(spark, ctx, m):
    from pyspark.sql import functions as F
    snapshot = spark.read.option("versionAsOf", ctx.bronze_version).table(BRONZE)
    if ctx.population == "whole_snapshot":
        return snapshot
    # Las filas fuera de esta carga NO se consideran auditadas; alcance persistido.
    return snapshot.filter(F.get_json_object(F.col("value").cast("string"), "$.twitch_load_id") == m["twitch_load_id"])


def require_pass(value, ctx):
    if value.get("context") != asdict(ctx) or value.get("context_sha256") != ctx.fingerprint or value.get("status") != "PASS":
        raise ValueError("Evidencia previa no autoriza esta ejecución/versión/manifiesto")


def verified(spark, ctx):
    value, version = record(spark, ctx.artifact("bronze_audit"))
    require_pass(value, ctx)
    validate_authorization(value["source_audit"], value["binding"], ctx.bronze_version)
    if manifest(spark, ctx) != value["binding"]:
        raise ValueError("El binding no reproduce la referencia autorizada")
    return value, version


def authorized(spark, ctx):
    value, version = record(spark, ctx.artifact("authorization"))
    require_pass(value, ctx)
    audit, audit_version = verified(spark, ctx)
    if value.get("bronze_audit") != audit or value.get("bronze_audit_delta_version") != audit_version:
        raise ValueError("Autorización no coincide con evidencia Bronze")
    return value, version


def verify_bronze(spark, ctx):
    binding = manifest(spark, ctx)
    m = binding["manifest"]
    source = audit_native(population(spark, ctx, m), m)
    source.update(bronze_table=BRONZE, bronze_delta_version=ctx.bronze_version,
        pipeline_id=binding["pipeline_id"])
    result = {"status": source["status"], "context": asdict(ctx), "context_sha256": ctx.fingerprint,
        "binding": binding, "source_audit": source, "population_scope": ctx.population,
        "snapshot_total_rows": spark.read.option("versionAsOf", ctx.bronze_version).table(BRONZE).count()}
    put_record(spark, ctx.artifact("bronze_audit"), result)
    require_pass(result, ctx)
    return result


def authorize(spark, ctx):
    audit, version = verified(spark, ctx)
    value = {"status": "PASS", "context": asdict(ctx), "context_sha256": ctx.fingerprint,
        "bronze_audit": audit, "bronze_audit_delta_version": version}
    if not spark.catalog.tableExists(ctx.artifact("authorization")):
        # No adoptar resultados preexistentes de otra ejecución o proceso manual.
        require_absent(spark, *(ctx.release.table(s) for s in STAGES), ctx.artifact("publication"))
    put_record(spark, ctx.artifact("authorization"), value)
    return value


def same_frame(left, right):
    return (left.columns == right.columns
        and not left.exceptAll(right).limit(1).count()
        and not right.exceptAll(left).limit(1).count())


def planned(spark, ctx, auth):
    binding = auth["bronze_audit"]["binding"]
    m = binding["manifest"]
    bronze = population(spark, ctx, m)
    current = audit_native(bronze, m)
    before = auth["bronze_audit"]["source_audit"]
    if current["status"] != "PASS" or any(current[k] != before[k] for k in
            ("payload_sha256", "observed_event_ids_sha256", "source_rows", "bronze_rows")):
        raise ValueError("La población fijada ya no reproduce la autorización")
    return frames(bronze, m)


def transform(spark, ctx):
    from pyspark.sql import functions as F
    auth, auth_version = authorized(spark, ctx)
    result = planned(spark, ctx, auth)
    # Marker estable antes de los writes: liga cualquier reparación a esta autorización.
    marker = {"status": "AUTHORIZED_NOT_PUBLISHED", "context": asdict(ctx),
        "context_sha256": ctx.fingerprint, "authorization": auth,
        "authorization_delta_version": auth_version}
    marker_table = ctx.release.table("manifest")
    if spark.catalog.tableExists(marker_table):
        rows = spark.table(marker_table).select("manifest_json").limit(2).collect()
        if len(rows) != 1 or json.loads(rows[0][0]) != marker:
            raise ValueError("Manifest de transformación no coincide")
    else:
        write_new(spark, spark.createDataFrame([(canonical(marker),)], "manifest_json STRING"), marker_table)
    for stage, df in result.items():
        expected = df.withColumn("release_id", F.lit(ctx.release_id))
        table = ctx.release.table(stage)
        if spark.catalog.tableExists(table):
            if not same_frame(spark.table(table), expected):
                raise ValueError("Write parcial/distinto: conservar tablas; usar release nueva tras diagnóstico")
        else:
            write_new(spark, expected, table)
    return {"status": "TRANSFORMED_NOT_PUBLISHED", "release_id": ctx.release_id}


def reconcile_release(spark, ctx):
    auth, auth_version = authorized(spark, ctx)
    expected = planned(spark, ctx, auth)
    stored = {s: spark.table(ctx.release.table(s)).drop("release_id") for s in expected}
    result = reconcile(stored["source_classification"], stored["rejects"], stored["silver"], stored["gold"])
    for stage in expected:
        result["checks"]["persisted_" + stage + "_matches_plan"] = same_frame(stored[stage], expected[stage])
    result["checks"]["persisted_counts_match_authorized_bronze"] = result["bronze_rows"] == auth["bronze_audit"]["source_audit"]["bronze_rows"]
    result.update(status="PASS" if all(result["checks"].values()) else "FAIL",
        context=asdict(ctx), context_sha256=ctx.fingerprint, authorization_delta_version=auth_version,
        offset_audit_status=auth["bronze_audit"]["source_audit"]["offset_audit_status"],
        table_versions={s: spark.sql(f"DESCRIBE HISTORY {ctx.release.table(s)} LIMIT 1").first()["version"] for s in expected})
    put_record(spark, ctx.release.table("reconciliation"), result)
    require_pass(result, ctx)
    return result


def publish(spark, ctx):
    authorized(spark, ctx)
    result, version = record(spark, ctx.release.table("reconciliation"))
    require_pass(result, ctx)
    if len(result.get("checks", {})) != 14 or not all(v is True for v in result["checks"].values()):
        raise ValueError("Conciliación incompleta; no publicar")
    for stage, expected in result["table_versions"].items():
        actual = spark.sql(f"DESCRIBE HISTORY {ctx.release.table(stage)} LIMIT 1").first()["version"]
        if actual != expected:
            raise ValueError("Tabla cambió después de conciliar; no publicar")
    value = {"status": "PASS", "publication_state": "CONFIRMED", "context": asdict(ctx),
        "context_sha256": ctx.fingerprint, "reconciliation_table": ctx.release.table("reconciliation"),
        "reconciliation_delta_version": version, "reconciliation": result,
        "gold_table": ctx.release.table("gold"), "gold_delta_version": result["table_versions"]["gold"],
        "scope": "CONFIRMED_DELTA_RELEASE_NOT_REACT_EXPORT"}
    put_record(spark, ctx.artifact("publication"), value)
    return value


def notebook_context(dbutils):
    defaults = {"release_id": "", "bronze_version": "", "manifest_table": "", "manifest_version": "",
        "population": "whole_snapshot", "code_commit": "", "job_id": "", "job_run_id": "", "job_stage": ""}
    for k, v in defaults.items():
        dbutils.widgets.text(k, v)
    values = {k: dbutils.widgets.get(k) for k in defaults}
    stage = values.pop("job_stage")
    if stage not in STEPS:
        raise ValueError("Etapa Job inválida")
    for k in ("bronze_version", "manifest_version"):
        if not re.fullmatch(r"0|[1-9][0-9]*", values[k]):
            raise ValueError("Versión no resuelta/ausente")
        values[k] = int(values[k])
    return Context(**values), stage


def execute(spark, dbutils, allowed):
    ctx, stage = notebook_context(dbutils)
    if stage not in allowed:
        raise ValueError("Notebook no autorizado para esta etapa")
    return {"verify_bronze": verify_bronze, "authorize": authorize, "transform": transform,
        "reconcile": reconcile_release, "publish": publish}[stage](spark, ctx)
