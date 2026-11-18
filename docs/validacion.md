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
La estrategia DDL + append queda confirmada para Bronze en ese entorno; la ejecución
real de Silver, cuarentena, Gold y candidatas sigue pendiente.

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

## Verificaciones pendientes en Databricks

1. Actualizar rama y reiniciar Python como indica
   [corrección Spark Connect](correccion_spark_connect.md).
2. Repetir 01 tras actualizar para confirmar que las transformaciones siguen pasando.
3. Opcional: ejecutar `tests.databricks_storage_checks.run_storage_checks(spark, cfg)`
   desde una celda tras 00_config. Crea dos tablas pequeñas ARTIFICIALES con nombres
   únicos; verifica contenido, tipos y bloqueo de repetición; **no las elimina**.
   Pendiente de ejecutar; no se confunde con 01, que sigue sin persistencia.
4. Bronze ya existe y fue conciliada: no reejecutar 03 ni volver a cargar el archivo.
5. Seguir [operación Silver y Gold](operacion_silver_gold.md) para comprobar módulo
   cargado, esquemas y destinos; ejecutar 04 y 05 por orden y revisar conciliaciones.
6. Comprobar bloqueo al repetir y que no cambian filas ni versión Delta.
7. Vista previa de 06 y candidata con sufijo nuevo si se desea comprobar persistencia.

Guardar commit, versión Spark, salidas, esquemas, conteos y errores. Cuotas, permisos,
DDL, append y comportamiento del runtime requieren esta comprobación real.
