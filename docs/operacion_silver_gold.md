# Operación segura: Silver y Gold después de Bronze

## Estado y alcance de esta actualización

Bronze confirmada por el equipo con `5994433`: 3.051.733 filas de origen y de
`workspace.twitch_lakehouse.bronze_interactions`, Delta, 2 archivos físicos y
43.938.329 bytes según DESCRIBE DETAIL. Ver [evidencia](validacion.md).
La igualdad de conteos no equivale a validación de contenido; Silver hará la calidad.

Esta actualización es exclusivamente documental. No cambia módulos, notebooks,
contrato ni reglas analíticas; no modifica tablas. **No hace falta Pull ni reinicio
por estos cambios documentales para continuar la demostración con 5994433.** Si la
sesión ya usa la función corregida, continuar desde 04, sin repetir 03.

## Regla para próximos incrementos: Pull de Python → reinicio → 00_config

Cuando un Pull cambie módulos Python, esperar a que termine la etapa en curso,
conservar la ruta local de configuración, actualizar y ejecutar en una celda separada:

```python
dbutils.library.restartPython()
```

Después ejecutar `00_config` de nuevo en cada sesión afectada y, ante cambios de
transformaciones, `01_artificial_checks`. Las etapas 04/05 también incluyen 00_config.
El reinicio pierde variables, imports y DataFrames Python; conserva tablas Delta.
No repetir escrituras completadas para restaurar variables.

La [documentación Databricks sobre reinicio](https://docs.databricks.com/aws/en/libraries/restart-python-process)
describe el reinicio y la pérdida del estado Python. La aplicación tras Pull es una
regla de este proyecto basada en la incidencia real del equipo.

Abrir el archivo o usar `inspect.getsource` puede mostrar texto nuevo en disco y no
probar qué función compilada sigue cargada. Python conserva módulos importados y
referencias de `from ... import ...`; recargar solo storage puede dejar la referencia
anterior en pipeline. No basar la demostración en recargas parciales ni autoreload.
Ver [advertencias oficiales de importlib.reload](https://docs.python.org/3/library/importlib.html#importlib.reload).

## Identificar la función realmente cargada, sin escribir tablas

Celda opcional después de 00_config, válida para la implementación de `5994433`:

```python
import inspect
import twitch_lakehouse.storage as storage
import twitch_lakehouse.pipeline as pipeline

for module in (storage, pipeline):
    fn = module.write_new
    modes = [value for value in fn.__code__.co_consts
             if isinstance(value, str) and value in {"append", "errorifexists", "overwrite"}]
    print({"module": module.__name__, "file": module.__file__,
           "signature": str(inspect.signature(fn)), "loaded_modes": modes})
    assert tuple(inspect.signature(fn).parameters) == ("spark", "df", "table")
    assert "append" in modes and "errorifexists" not in modes and "overwrite" not in modes
assert pipeline.write_new is storage.write_new
```

Esperado: rutas en la carpeta Workspace del proyecto, firma `(spark, df, table)`,
constante `append` e identidad compartida. La firma antigua de dos parámetros o la
constante `errorifexists` identifica esta versión anterior; identidad distinta indica
referencias mezcladas. Si falla, detener, reiniciar Python y ejecutar 00_config.
Para inspección adicional, `import dis; dis.dis(storage.write_new)` muestra bytecode
del objeto en memoria. El diagnóstico no es una huella criptográfica de todo el
repositorio: registrar también el commit de la Git folder. No se añadió una variable
de versión al runtime para evitar modificar código justo antes de la demostración.

## Escritores y tipos revisados

Las cuatro llamadas productivas pasan `spark` a la misma `write_new`: Bronze,
rechazos, Silver y Gold. La candidata reutiliza build_silver para ambos destinos.
No hay escritor independiente que siga usando errorifexists, error, ignore u overwrite.
Se conserva ausencia previa, CREATE estricto sin IF NOT EXISTS y append solo después
de crear. Una tabla existente, incluso vacía, bloquea la ejecución sin anexar filas.
Si falla append tras CREATE, se conserva la tabla y se detiene el reintento.

Tipos previstos por las expresiones del código, por confirmar con printSchema:

| Capa | Columnas / tipos |
| --- | --- |
| Bronze | raw_line STRING, source_file STRING, ingested_at TIMESTAMP |
| Silver | user_id, stream_id, time_start, time_stop, estimated_minutes BIGINT; streamer_username STRING |
| Cuarentena | columnas Bronze + user_id, stream_id, time_start, time_stop BIGINT; streamer_username y quality_errors STRING |
| Gold | streamer_username STRING; interactions, unique_users, unique_streams, estimated_minutes BIGINT |
| Candidatas | mismos tipos que Silver/cuarentena; nombres de tabla diferentes |

El DDL deriva orden, nombres, tipos y nulabilidad del DataFrame, no de CSV inferido.
Se utilizan tipos escalares Spark/Delta habituales; no hay tipos complejos ni
aritmética de enteros de 32 bits para IDs. La confirmación de Bronze no certifica
todavía el DDL y escritura de cada esquema restante.

## Antes de ejecutar 04_silver

1. Confirmar configuración workspace.twitch_lakehouse, Bronze existente y función
   cargada corregida. No volver a ejecutar 02/03 ni subir el CSV.
2. Comprobar ausencia de **ambos** destinos, incluso si se sospecha una tabla vacía:

```python
from twitch_lakehouse.storage import require_absent
require_absent(spark, cfg.table("silver_interactions"), cfg.table("silver_rejects"))
bronze = spark.table(cfg.table("bronze_interactions"))
bronze.printSchema()
```

3. Si se quiere revisar esquema antes de persistir, construir los DataFrames sin
   escritura (el análisis Spark puede consultar el catálogo y no garantiza éxito futuro):

```python
from twitch_lakehouse.transforms import silver_frames, gold_by_streamer
silver_preview, rejects_preview, _ = silver_frames(bronze)
silver_preview.printSchema()
rejects_preview.printSchema()
gold_by_streamer(silver_preview).printSchema()
```

4. Ejecutar 04 una sola vez, sin otros escritores concurrentes. El pipeline calcula
   métricas antes de publicar y compara los conteos persistidos. Guardar las salidas:
   `bronze_rows` debe ser 3.051.733 si la tabla no cambió;
   Bronze = válidas + rechazadas; válidas = Silver + duplicadas. No anticipar los
   conteos Silver/rechazos/duplicados: aún no están reportados.
5. Revisar motivos de cuarentena y clave Silver única. Incluso una cuarentena vacía
   es una tabla creada: su existencia bloquea el reintento. Si se crea cuarentena
   pero falla Silver, detener y conservar ambos estados; no borrar ni reanexar.

## Antes de ejecutar 05_gold

1. Exigir éxito y conciliación de 04, revisar rechazos y verificar ausencia de
   `gold_streamer_metrics`. Ejecutar 00_config/diagnóstico en esa sesión si corresponde.
2. Revisar el esquema Silver persistido; confirmar tipos de la tabla Gold prevista
   si se usa la vista previa anterior. Ejecutar 05 una sola vez.
3. Guardar conteos y comprobar suma de interacciones Gold = filas Silver y suma
   de minutos Gold = suma de minutos Silver. El pipeline verifica estas dos sumas
   antes de escribir. Los minutos siguen siendo estimaciones, sin cambio de semántica.
4. Para revisar lo persistido sin escrituras adicionales, comparar esas mismas sumas
   leyendo ambas tablas y consultar DESCRIBE HISTORY/DETAIL para evidencia.
   No ejecutar reconstrucción ni pruebas que creen tablas adicionales durante la
   demostración salvo necesidad explícita. 06 permanece como vista previa por defecto.

No se identificó un defecto bloqueante adicional en esta revisión estática.
Esto no sustituye ejecutar 04/05 en el runtime real; ante un nuevo error detener
la secuencia y registrar evidencia antes de proponer cambios al pipeline.
