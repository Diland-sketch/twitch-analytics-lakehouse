"""Fixtures pequeños tipados vía VALUES, sin ejecutar trabajadores Python.

Solo constantes de pruebas. Se prueban transformaciones DataFrame/SQL, no UDF;
el transporte de listas Python al ejecutor no forma parte de su contrato.
"""
def fixture_frame(spark, rows, schema):
    fields = [field.strip().split() for field in schema.split(",")]
    def literal(value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return str(value)
    values = ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in rows)
    aliases = ", ".join(f"c{i}" for i in range(len(fields)))
    columns = ", ".join(f"CAST(c{i} AS {kind}) AS {name}"
                        for i, (name, kind) in enumerate(fields))
    return spark.sql(f"SELECT {columns} FROM VALUES {values} AS fixture({aliases})")
