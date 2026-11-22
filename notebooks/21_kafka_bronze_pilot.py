# Databricks notebook source
# Actualizar Git y REINICIAR Python antes de ejecutar (dbutils.library.restartPython()).
# Importar este repositorio como Git folder; no instalar pyspark ni JAR/Maven.

# COMMAND ----------
import json
import os
import sys

dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("manifest_json", "")
dbutils.widgets.text("secret_scope", "twitch-kafka")
dbutils.widgets.text("sasl_mechanism", "SCRAM-SHA-512")
dbutils.widgets.text("ca_secret_key", "")
repo_root = dbutils.widgets.get("repo_root")
if not repo_root.startswith("/Workspace/") or not os.path.isdir(repo_root):
    raise ValueError("Configurar repo_root absoluto del Git folder")
sys.path.insert(0, repo_root)

from kafka_ingest.bronze import ROOT, CHECKPOINT, options, run
from kafka_ingest.contracts import validate_manifest

manifest = validate_manifest(json.loads(dbutils.widgets.get("manifest_json")))
scope = dbutils.widgets.get("secret_scope")

# COMMAND ----------
def checkpoint_present():
    # ROOT existe después del bootstrap. No ocultar errores de permisos.
    children = dbutils.fs.ls(ROOT)
    checkpoints = [p for p in children if p.path.rstrip("/").endswith("/pilot_v1")]
    if not checkpoints:
        return False
    children = dbutils.fs.ls(CHECKPOINT)
    offsets = [p for p in children if p.path.rstrip("/").endswith("/offsets")]
    return bool(offsets and dbutils.fs.ls(CHECKPOINT + "/offsets"))

try:
    ca_key = dbutils.widgets.get("ca_secret_key")
    kafka_options = options(
        dbutils.secrets.get(scope, "bootstrap-servers"), manifest["topic"],
        dbutils.secrets.get(scope, "username"), dbutils.secrets.get(scope, "password"),
        dbutils.widgets.get("sasl_mechanism"),
        dbutils.secrets.get(scope, ca_key) if ca_key else None)
    report = run(spark, manifest, kafka_options, checkpoint_present)
except Exception as error:
    # No publicar el texto de excepciones Kafka/JAAS. Conservar clase y logs seguros.
    raise RuntimeError("Piloto no confirmado: " + type(error).__name__) from None

dbutils.notebook.exit(json.dumps(report))
