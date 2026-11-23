# Databricks notebook source
# Solo Spark batch, Bronze Delta v7 fija; no conecta Kafka ni refresca pipelines.
# Tras actualizar Git, reiniciar Python en celda independiente ANTES de importar.

# COMMAND ----------
import os
import sys
import json
dbutils.widgets.text("repo_root", "/Workspace/Users/dilandlopez10@gmail.com/twitch-analytics-i3")
dbutils.widgets.text("release_id", "kafka_b7_r001")
repo = dbutils.widgets.get("repo_root")
if not repo.startswith("/Workspace/") or not os.path.isdir(repo):
    raise ValueError("Configurar Git folder absoluto de esta rama")
sys.path.insert(0, repo)
from kafka_ingest.medallion import Release, run

# COMMAND ----------
report = run(spark, Release(dbutils.widgets.get("release_id")))
print(json.dumps(report, sort_keys=True))
dbutils.notebook.exit(json.dumps(report, sort_keys=True))
