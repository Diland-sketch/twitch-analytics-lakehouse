# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
# No lee Twitch ni escribe tablas. Datos exclusivamente ARTIFICIALES.
from tests.spark_checks import run_spark_checks
print(run_spark_checks(spark))
