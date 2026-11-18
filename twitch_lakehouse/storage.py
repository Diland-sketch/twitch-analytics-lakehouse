"""Protecciones de publicación; no dependen de un motor Spark local."""


def require_absent(spark, *tables):
    for table in tables:
        if spark.catalog.tableExists(table):
            raise RuntimeError(f"Destino existente: {table}. No se modificó esta tabla.")


def write_new(df, table):
    # La comprobación previa ayuda a diagnosticar; este modo también protege al escribir.
    df.write.format("delta").mode("errorifexists").saveAsTable(table)
