# Databricks notebook source
# Job batch separado del piloto Kafka. Sin latest, overwrite, RDD ni Kafka.
import json
import time
from bulk_lakehouse.pipeline import Config, execute, STAGES
for name, value in {"rows": "", "release": "", "manifest_path": "", "predecessor": "",
                    "max_input_bytes": "134217728", "stage": ""}.items():
    dbutils.widgets.text(name, value)
stage = dbutils.widgets.get("stage")
cfg = Config(int(dbutils.widgets.get("rows")), dbutils.widgets.get("release"),
    dbutils.widgets.get("manifest_path"), dbutils.widgets.get("predecessor"), int(dbutils.widgets.get("max_input_bytes")))
started = time.monotonic()
try:
    report = execute(spark, cfg, stage)
except Exception as error:
    print(json.dumps({"status": "FAIL", "release": cfg.release, "stage": stage,
        "error_type": type(error).__name__, "elapsed_seconds": time.monotonic() - started}))
    raise
print(json.dumps(report, sort_keys=True))
dbutils.notebook.exit(json.dumps(report, sort_keys=True))
