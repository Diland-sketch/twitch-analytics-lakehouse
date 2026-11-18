# Validación de I1 y corrección de escritura

## Evidencia reportada por el equipo en Databricks

- Spark y `00_config` funcionan; `01_artificial_checks` pasó con estado PASS.
- `100k_a.csv` está cargado en `/Volumes/workspace/twitch_lakehouse/raw_data/100k_a.csv`.
- `02_bootstrap` prepara esquema y volumen.
- `03_bronze` falló en la escritura compartida con
  `[UNSUPPORTED_OPERATION] errorifexists is not supported`.
- Después del Pull persistió el error anterior: el archivo en disco estaba corregido,
  pero la función importada en memoria no coincidía. Tras `dbutils.library.restartPython()`
  y nueva ejecución de `00_config`, el equipo confirmó éxito de Bronze con `5994433`.

| Evidencia real reportada de Bronze | Resultado |
| --- | --- |
| Fuente | `100k_a.csv` |
| Registros de origen | 3.051.733 |
| Registros Bronze | 3.051.733 |
| Conciliación de conteos | 100% |
| Tabla | `workspace.twitch_lakehouse.bronze_interactions` |
| Formato | Delta |
| Archivos físicos Delta | 2 |
| Tamaño de DESCRIBE DETAIL | 43.938.329 bytes |

Resultados reportados por el equipo; no ejecución independiente del asistente.
El 100% indica igualdad de conteos, no certifica calidad de contenido ni ausencia
de duplicados. Archivos y tamaño son una observación de esa ejecución, no invariantes.
No se aportaron aquí salidas originales, hash de fuente, versión Spark ni versión
Delta de la tabla: conservarlas como evidencia sin inventarlas.
El equipo confirmó después la ejecución completa de I1 en Databricks Free Edition:

| Resultado real reportado | Valor |
| --- | --- |
| Silver: registros válidos persistidos | 3.051.733 |
| Rechazados según las reglas implementadas | 0 |
| Duplicados según la clave implementada | 0 |
| Gold: streamers | 162.625 |
| Interacciones conciliadas | 3.051.733 |
| Minutos acumulados estimados | 95.887.110 |
| Silver original / reconstruida | 3.051.733 / 3.051.733 |
| exceptAll original → candidata | 0 diferencias |
| exceptAll candidata → original | 0 diferencias |
| Equivalencia reportada | True |

Candidata existente: `workspace.twitch_lakehouse.silver_interactions_candidate_suffix`.
La Silver original permaneció intacta. El literal accidental y el uso correcto del
widget están en [reconstrucción](reconstruccion_silver.md).

Estado: **I1 batch completado y verificado por el equipo**. Cero rechazos/duplicados
se refiere exclusivamente a las reglas actuales, no a calidad universal del dataset.
Los minutos son una estimación por intervalos, no tiempo exacto de atención.
El asistente documenta los resultados reportados; no volvió a ejecutar ni consultó
las tablas. No se han reportado explícitamente la regresión optativa de almacenamiento
ni la prueba de reejecución bloqueada en el servicio: no se marcan como realizadas.

## Comprobaciones locales de esta corrección

Python 3.13.4, sin PySpark instalado. No se instalaron dependencias ni se descargaron datos.

- `python -m unittest discover -s tests -v`: **15 pruebas aprobadas, 1 omitida**
  explícitamente por falta de PySpark. La suite original tenía 7 aprobadas y 1 omitida.
- Regresión con dobles: destino existente intacto; creación antes de append; nombres,
  orden, tipos y nulabilidad en DDL; reejecución sin duplicados en los seis destinos;
  fallo de CREATE o de su confirmación sin append; carrera con catálogo desactualizado; fallo de append
  conservando tabla y bloqueando reintentos; destino vacío también bloqueado;
  identificadores inválidos y esquema sin columnas rechazados.
- `python -m compileall -q twitch_lakehouse tests notebooks`: sin errores de sintaxis.
- `git diff --check`: sin errores de espacios en el diff.

Los dobles comprueban decisiones y orden de llamadas: **no ejecutan SQL, Spark ni
Delta**, ni prueban la compatibilidad real con el servicio Databricks.

## Evidencia y controles para conservar

- Mantener las tablas actuales; no repetir las cargas para documentar resultados.
- Conservar salidas originales de 01 y 03–06, DESCRIBE DETAIL/HISTORY, esquemas,
  commit usado por cada ejecución, versión Spark y hash de la fuente cuando estén disponibles.
  No se inventan fecha/hora exacta, versión Delta ni commit de la reconstrucción.
- Las pruebas locales usan dobles; la ejecución real reportada ahora valida el
  recorrido batch y la equivalencia de la candidata en ese workspace.
- Kafka y el histórico completo siguen fuera de I1. La rúbrica oficial no fue aportada.
