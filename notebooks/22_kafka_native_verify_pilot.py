# Databricks notebook source
# Auditoría batch de la Bronze NATIVA. No inicia un segundo consumidor Kafka.
# Ejecutar después de detener normalmente el pipeline; nunca Full refresh.
# Reiniciar Python después de actualizar Git, antes de ejecutar este notebook.

# COMMAND ----------
import json
import os
import sys
import uuid
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("manifest_json", "")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("update_id", "")
repo = dbutils.widgets.get("repo_root")
if not repo.startswith("/Workspace/") or not os.path.isdir(repo):
    raise ValueError("Configurar Git folder absoluto")
sys.path.insert(0, repo)

from kafka_ingest.contracts import canonical, validate_manifest
from kafka_ingest.bronze import NAMESPACE, project, audit
from twitch_lakehouse.storage import write_new

m = validate_manifest(json.loads(dbutils.widgets.get("manifest_json")))
pipeline_id = str(uuid.UUID(dbutils.widgets.get("pipeline_id")))
update_id = str(uuid.UUID(dbutils.widgets.get("update_id")))
table = NAMESPACE + ".bronze_kafka_native_pilot_v1"
binding_table = NAMESPACE + ".native_pilot_binding_v1"
binding = canonical({"manifest": m, "pipeline_id": pipeline_id})
if spark.catalog.tableExists(binding_table):
    previous = spark.table(binding_table).collect()
    if len(previous) != 1 or previous[0][0] != binding:
        raise RuntimeError("Binding nativo distinto; detener sin modificar estado")
else:
    write_new(spark, spark.createDataFrame([(binding,)], "binding_json STRING"), binding_table)

# COMMAND ----------
native = spark.table(table)
if not {"key", "value", "kafka_source"}.issubset(set(native.columns)):
    raise RuntimeError("Contrato Bronze nativo inesperado: inspeccionar esquema, no inferir metadata")
# Congelar la versión Delta comprobada; nunca auditar una tabla mientras cambia.
version = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]
native = spark.read.option("versionAsOf", version).table(table)
if native.limit(5001).count() > 5000:
    raise RuntimeError("La tabla supera el límite de este auditor de piloto")
metadata = native.select("key", "value", "kafka_source.*")
normalized = project(metadata, m["source_cluster_id"])
view = "native_pilot_" + uuid.uuid4().hex
normalized.createOrReplaceTempView(view)
report = audit(spark, m, table=view)
report.update({"ingestion_mode": "LAKEFLOW_CONNECT_KAFKA_BETA", "bronze_table": table,
    "bronze_delta_version": version, "pipeline_id": pipeline_id, "update_id": update_id})
write_new(spark, spark.createDataFrame([(canonical(report),)], "report_json STRING"), report["audit_table"])
if report["status"] != "PASS":
    raise RuntimeError("Cobertura nativa FAIL; reporte conservado en " + report["audit_table"])
dbutils.notebook.exit(canonical(report))
