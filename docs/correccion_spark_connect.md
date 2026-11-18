# Corrección de escritura I1: Spark Connect / Free Edition

## Incidencia y alcance

El equipo reportó `[UNSUPPORTED_OPERATION] errorifexists is not supported` al ejecutar
03 en su entorno. El único uso productivo estaba en `twitch_lakehouse/storage.py`,
función `write_new`: `.mode("errorifexists").saveAsTable(table)`.
La antigua prueba de escritura también esperaba ese modo, pero no lo ejecutaba en Spark.

Las cuatro llamadas en `pipeline.py` compartían el problema:

| Llamada | Destinos |
| --- | --- |
| `ingest_bronze` | `bronze_interactions` |
| `build_silver` (dos escrituras) | `silver_rejects`, `silver_interactions` |
| `build_gold` | `gold_streamer_metrics` |
| `build_silver(candidate_suffix=...)`, reutilizada por 06 | Ambos destinos Silver con sufijo de candidata |

`require_absent` se conserva y se invoca también dentro de `write_new`, que ahora
recibe `spark` explícitamente. No se modifican transformaciones, contrato ni reglas analíticas.

## Estrategia

1. Validar nombre de tres componentes y comprobar ausencia del destino.
2. Derivar nombres, orden, tipos y nulabilidad de `df.schema`.
3. Ejecutar `CREATE TABLE <destino> (<columnas>) USING DELTA` y esperar finalización.
   Sin `IF NOT EXISTS`, `REPLACE` ni ruta externa. Se crea una tabla administrada.
4. Solo tras éxito de CREATE, ejecutar `df.write.format("delta").mode("append").saveAsTable(destino)`.

Si el destino existe, incluso vacío, no hay append. Si otro creador gana después
de la comprobación, CREATE falla y no se llega al append. No se captura ese error
para continuar. Las invocaciones repetidas no anexan filas.

DDL y append son dos operaciones, no una transacción conjunta. Un fallo después de
crear conserva la tabla y bloquea la próxima invocación; no borra ni reanuda datos.
No ejecutar borrado/recreación externa ni escritores ajenos al pipeline concurrentemente.
No hay atomicidad entre cuarentena y Silver; un fallo parcial exige revisión humana.

La [referencia general de saveAsTable](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframewriter/saveAsTable)
enumera `errorifexists`; esto no demuestra su soporte en el runtime concreto que
rechazó la operación. No se atribuye una prohibición universal a Spark Connect.
La [referencia de CREATE TABLE](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using)
documenta DDL Delta y que `IF NOT EXISTS` ignora destinos existentes; por eso se omite.
**DDL + append aún requiere validación en el workspace del equipo.**

## Actualizar la carpeta Git en Databricks

1. Detener cualquier ejecución en curso y anotar el valor local de `project_root`
   de `notebooks/00_config`. No mover ni volver a cargar `100k_a.csv`.
2. En Workspace abrir la Git folder existente → menú Git / nombre de rama.
   Comprobar repositorio `Diland-sketch/twitch-analytics-lakehouse` y rama
   `feat/i1-spark-medallion`; no cambiar a main.
3. Revisar Changes. Si el único cambio local es la ruta de 00_config, copiar ese
   valor fuera de la carpeta y revertir únicamente ese cambio respaldado si el
   diálogo lo exige para Pull. Conservar y resolver otros cambios antes de actualizar;
   no usar descarte global ni reset forzado.
4. Pulsar **Pull**. Verificar en el historial el commit de esta corrección y abrir
   `twitch_lakehouse/storage.py`: firma `write_new(spark, df, table)`, DDL sin
   `IF NOT EXISTS` y modo `append`. Los cuatro sitios del pipeline deben pasar `spark`.
5. Reponer el valor respaldado de `project_root` en 00_config si corresponde.
6. En cada sesión Python que haya importado estos módulos ejecutar en una celda
   separada `dbutils.library.restartPython()`. Al terminar, ejecutar 00_config y 01.
   Reiniciar cada notebook/sesión que se vaya a usar; actualizar archivos no recarga
   automáticamente módulos ya importados.
7. Antes de 03, comprobar en una celda:

```python
target = cfg.table("bronze_interactions")
print({"target": target, "exists_now": spark.catalog.tableExists(target)})
```

8. Si no existe, ejecutar 03 y guardar conteos. Si existe, detener y revisar
   `spark.table(target).count()` y `DESCRIBE HISTORY workspace.twitch_lakehouse.bronze_interactions`.
   No borrarla aunque esté vacía. Para una nueva demostración aislada usar otro
   esquema siguiendo ejecución I1, preservando la fuente y las tablas anteriores.
9. Tras éxito de 03 ejecutar 04 y después 05. Repetir una etapa completada debe dar
   `Destino existente` sin cambiar conteo ni versión Delta. Revisar cuarentena.
10. 06 permanece opcional: vista previa por defecto y escritura solo con sufijo nuevo.

Para la regresión de almacenamiento aislada, después de 00_config ejecutar:

```python
from tests.databricks_storage_checks import run_storage_checks
print(run_storage_checks(spark, cfg))
```

Esta comprobación crea dos tablas pequeñas con nombres únicos y datos explícitamente
ARTIFICIALES. No toca las tablas del pipeline ni elimina las tablas de prueba. Su
PASS no sustituye la ejecución y conciliación real de 03–05.
