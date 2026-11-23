# Databricks notebook source
# Solo Spark batch, Bronze Delta v7 fija; no conecta Kafka ni refresca pipelines.
# Tras actualizar Git, reiniciar Python en celda independiente ANTES de importar.

# COMMAND ----------
import os
import sys
import json
dbutils.widgets.text("execution_mode", "manual")
if dbutils.widgets.get("execution_mode") == "job":
    from kafka_ingest.orchestration import execute
    dbutils.notebook.exit(json.dumps(execute(spark, dbutils, {"transform"})))
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("release_id", "")
repo = dbutils.widgets.get("repo_root")
if not repo.startswith("/Workspace/") or not os.path.isdir(repo):
    raise ValueError("Configurar Git folder absoluto de esta rama")
sys.path.insert(0, repo)
from kafka_ingest.medallion import Release, run

# COMMAND ----------
report = run(spark, Release(dbutils.widgets.get("release_id")))
print(json.dumps(report, sort_keys=True))
dbutils.notebook.exit(json.dumps(report, sort_keys=True))
