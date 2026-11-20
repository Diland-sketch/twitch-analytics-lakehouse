# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
from tests.analytics_checks import run_analytics_checks
print(run_analytics_checks(spark))  # Solo datos ARTIFICIALES, sin escrituras.
