# Databricks notebook source
# Propuesta Job v1, NO creada remotamente. Todos los stages son RO sobre datos.
import sys
import json
from datetime import datetime, timezone

for name, default in (("project_root", ""), ("catalog", "workspace"),
                      ("schema", "twitch_lakehouse"), ("release", "r01"), ("stage", "preflight")):
    dbutils.widgets.text(name, default)
root = dbutils.widgets.get("project_root").rstrip("/")
if not root.startswith("/Workspace/"):
    raise ValueError("Asignar project_root absoluto del checkout candidato en Workspace")
if root not in sys.path:
    sys.path.insert(0, root)
from twitch_lakehouse.config import Settings
from twitch_lakehouse.release_inventory import inspect_release
from twitch_lakehouse.i3_preflight import verify_release

cfg = Settings(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
release = dbutils.widgets.get("release")
stage = dbutils.widgets.get("stage")
stages = ["preflight", "validate", "pending_processing", "reconcile", "publish", "evidence"]
if stage not in stages:
    raise ValueError("Stage no permitido")
report = {"stage": stage, "status": "PASS", "scope": "READ_ONLY_AUDIT_NOT_PUBLICATION",
          "release": release, "namespace": cfg.namespace,
          "observed_at": datetime.now(timezone.utc).isoformat(), "spark_version": spark.version}
try:
    if stage == "preflight":
        report["inventory"] = inspect_release(spark, cfg, release)
        if report["inventory"]["state"] != "COMPLETE":
            raise ValueError("Release ausente/parcial: decisión manual, ninguna escritura autorizada")
        for name in ("bronze_interactions", "silver_interactions", "silver_rejects", "gold_streamer_metrics"):
            if not spark.catalog.tableExists(cfg.table(name)):
                raise ValueError(f"Falta fuente I1: {name}")
    elif stage == "validate":
        from pyspark.sql import functions as F
        bronze = spark.table(cfg.table("bronze_interactions")).count()
        rejects = spark.table(cfg.table("silver_rejects")).count()
        silver = spark.table(cfg.table("silver_interactions")).agg(
            F.count("*").alias("rows"), F.sum("estimated_minutes").alias("minutes")).first()
        gold = spark.table(cfg.table("gold_streamer_metrics")).agg(
            F.sum("interactions").alias("rows"), F.sum("estimated_minutes").alias("minutes")).first()
        report["counts"] = {"bronze": bronze, "rejects": rejects,
                            "silver": silver.asDict(), "gold": gold.asDict()}
        if silver["rows"] == 0 or tuple(silver) != tuple(gold) or bronze < rejects + silver["rows"]:
            raise ValueError("I1 no concilia conteos/minutos")
    elif stage in ("pending_processing", "publish"):
        report.update(status="SKIP", reason="Deshabilitado en Job v1: requiere decisión manual y precondiciones de ausencia")
    elif stage == "reconcile":
        report["release_check"] = verify_release(spark, cfg, release)
    elif stage == "evidence":
        results = {}
        for task in stages[:-1]:
            try:
                results[task] = dbutils.jobs.taskValues.get(taskKey=task, key="report")
            except Exception:
                results[task] = {"status": "FAIL", "reason": "No hay reporte de la tarea"}
        report["tasks"] = results
        if any(result.get("status") == "FAIL" for result in results.values()):
            raise ValueError("Alguna tarea falló o no produjo evidencia")
except Exception as error:
    report.update(status="FAIL", error=str(error))
print(json.dumps(report, ensure_ascii=False, default=str))
# Metadatos del Job solamente; no se escribe una tabla, archivo ni volumen.
dbutils.jobs.taskValues.set(key="report", value=report)
if report["status"] == "FAIL":
    raise RuntimeError(json.dumps(report, ensure_ascii=False, default=str))
dbutils.notebook.exit(json.dumps(report, ensure_ascii=False, default=str))
