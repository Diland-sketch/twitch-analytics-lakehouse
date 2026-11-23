# Databricks notebook source
import json
from bulk_lakehouse.compact import Config,execute
dbutils.widgets.text("release","bulk_50m_c001")
dbutils.widgets.text("stage","")
cfg=Config(dbutils.widgets.get("release"))
stage=dbutils.widgets.get("stage")
try:
    result=execute(spark,cfg,stage)
except Exception as error:
    print(json.dumps({"status":"FAIL","stage":stage,"release":cfg.release,"error_type":type(error).__name__}),flush=True)
    raise
print(json.dumps(result,sort_keys=True),flush=True)
dbutils.notebook.exit(json.dumps(result,sort_keys=True))
