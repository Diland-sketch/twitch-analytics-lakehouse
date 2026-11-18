"""Creación Delta protegida, sin SaveMode.errorifexists ni reemplazos."""
from twitch_lakehouse.config import identifier


def require_absent(spark, *tables):
    for table in tables:
        if spark.catalog.tableExists(table):
            raise RuntimeError(f"Destino existente: {table}. No se modificó esta tabla.")


def write_new(spark, df, table):
    """Crear una tabla ausente y cargarla una vez; un fallo no habilita reintentos.

    CREATE sin IF NOT EXISTS arbitra también dos creadores concurrentes. Solo el
    creador exitoso llega al append. Si el append falla, conservar la tabla creada
    y detenerse: la próxima invocación se bloqueará aunque la tabla esté vacía.
    No admite borrado/recreación externa concurrente durante esta operación.
    """
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError("El destino debe incluir catálogo, esquema y tabla")
    qualified = ".".join(f"`{identifier(part)}`" for part in parts)
    require_absent(spark, table)
    # Derivar orden, nombres, tipos y nulabilidad del DataFrame, sin inferir CSV.
    columns = []
    for field in df.schema.fields:
        name = field.name.replace("`", "``")
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f"`{name}` {field.dataType.simpleString()}{nullable}")
    if not columns:
        raise ValueError("No se crea una tabla sin columnas")
    # No usar IF NOT EXISTS: si otro proceso creó el destino, NO debemos anexar.
    spark.sql(f"CREATE TABLE {qualified} ({', '.join(columns)}) USING DELTA").collect()
    df.write.format("delta").mode("append").saveAsTable(table)
