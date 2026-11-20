# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_config

# COMMAND ----------


import inspect
from twitch_lakehouse import pipeline

print("=== FUNCIONES DE RECONSTRUCCIÓN ===")

for nombre in dir(pipeline):
    if "rebuild" in nombre.lower() or "candidate" in nombre.lower():
        obj = getattr(pipeline, nombre)
        if inspect.isfunction(obj):
            print(f"\n--- {nombre} ---")
            print(inspect.getsource(obj))

print("\n=== TABLAS EXISTENTES ===")

spark.sql("""
SHOW TABLES IN workspace.twitch_lakehouse
""").show(truncate=False)


# COMMAND ----------

# Nunca reemplaza ni borra Silver. No forma parte del recorrido normal I1.
from twitch_lakehouse.pipeline import build_silver, silver_metrics
from twitch_lakehouse.transforms import silver_frames
bronze = spark.table(cfg.table("bronze_interactions"))
candidate, rejects, valid = silver_frames(bronze)
print(silver_metrics(bronze, candidate, rejects, valid))
display(candidate.limit(10))

# COMMAND ----------

# Vista previa por defecto. Para guardar, especificar un sufijo nuevo, por ejemplo revision_01.
dbutils.widgets.text("candidate_suffix", "")
suffix = dbutils.widgets.get("candidate_suffix")
if suffix:
    print(build_silver(spark, cfg, candidate_suffix=suffix))
else:
    print("Solo vista previa. Ninguna tabla candidata escrita.")

# COMMAND ----------


original = spark.table(
    cfg.table("silver_interactions")
)

reconstruida = spark.table(
    cfg.table("silver_interactions_candidate_suffix")
)

columnas = original.columns

faltantes = (
    original.select(*columnas)
    .exceptAll(reconstruida.select(*columnas))
    .count()
)

sobrantes = (
    reconstruida.select(*columnas)
    .exceptAll(original.select(*columnas))
    .count()
)

resultado = {
    "filas_originales": original.count(),
    "filas_reconstruidas": reconstruida.count(),
    "faltantes": faltantes,
    "sobrantes": sobrantes,
    "equivalencia": faltantes == 0 and sobrantes == 0
}

print(resultado)
