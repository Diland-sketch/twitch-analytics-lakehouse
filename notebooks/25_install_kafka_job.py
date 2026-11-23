# Databricks notebook source
# Instalación explícita, una vez. No ejecutar Run all automáticamente.
# Usar autenticación de notebook del SDK; no crear ni imprimir tokens.
import json
import os
import sys
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("tested_commit", "")
repo = dbutils.widgets.get("repo_root")
if not repo.startswith("/Workspace/") or not os.path.isdir(repo):
    raise ValueError("Configurar el Git folder de esta rama")
sys.path.insert(0, repo)
from kafka_ingest.job_definition import build_job
settings = build_job(dbutils.widgets.get("tested_commit"))
print(json.dumps(settings, sort_keys=True))

# COMMAND ----------
# Ejecutar esta celda solo tras revisar settings. Crea Job; NO dispara carga.
from databricks.sdk import WorkspaceClient
client = WorkspaceClient()
created = client.api_client.do("POST", "/api/2.2/jobs/create", body=settings)
print({"job_id": created["job_id"], "state": "CREATED_NOT_STARTED"})
