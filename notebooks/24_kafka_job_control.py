# Databricks notebook source
# Control de autorización, conciliación y publicación; solo contexto de Job válido.
import json
from kafka_ingest.orchestration import execute
result = execute(spark, dbutils, {"authorize", "reconcile", "publish"})
print(json.dumps(result, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
