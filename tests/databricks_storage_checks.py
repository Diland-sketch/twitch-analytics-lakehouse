"""Prueba de integración optativa: crea dos tablas ARTIFICIALES y las conserva.

No se ejecuta con unittest. Invocar manualmente en Databricks tras 00_config.
"""
from datetime import datetime, timezone
from uuid import uuid4
from twitch_lakehouse.storage import write_new


def run_storage_checks(spark, cfg):
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    schema = StructType([StructField("raw_line", StringType(), True),
                         StructField("source_file", StringType(), False),
                         StructField("ingested_at", TimestampType(), False)])
    source = spark.createDataFrame(
        [("ARTIFICIAL: storage regression", "ARTIFICIAL",
          datetime(2026, 11, 17, tzinfo=timezone.utc))], schema)
    prefix = f"i1_storage_check_{uuid4().hex}"
    tables = []
    for name, frame in ((f"{prefix}_populated", source),
                        (f"{prefix}_empty", source.limit(0))):
        target = cfg.table(name)
        tables.append(target)
        print({"fixture": "ARTIFICIAL", "table": target})
        write_new(spark, frame, target)
        stored = spark.table(target)
        assert stored.schema.simpleString() == frame.schema.simpleString()
        assert stored.exceptAll(frame).count() == frame.exceptAll(stored).count() == 0
        count = stored.count()
        version = spark.sql(f"DESCRIBE HISTORY {target}").first()["version"]
        try:
            # Siempre intentar agregar la fila de source, incluso al destino vacío.
            write_new(spark, source, target)
        except RuntimeError as error:
            assert "Destino existente" in str(error)
        else:
            raise AssertionError("Se permitió una segunda escritura")
        assert spark.table(target).count() == count
        assert spark.sql(f"DESCRIBE HISTORY {target}").first()["version"] == version
        assert spark.table(target).exceptAll(frame).count() == 0
    return {"fixture": "ARTIFICIAL", "status": "PASS", "preserved_tables": tables}
