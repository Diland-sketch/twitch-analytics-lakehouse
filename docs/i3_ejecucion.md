# Implementación y verificación I3 en Databricks Free Edition

## Preparación

1. Revisar PR I3 basado en I1. Abrir carpeta Git de Twitch en Databricks y
   seleccionar `feat/i3-analytics-dashboard` o commit revisado. No repetir las
   cargas I1 ni los notebooks 02–06. Kafka permanece independiente.
2. Tras Pull ejecutar `dbutils.library.restartPython()` en celda separada.
   Después ejecutar `notebooks/00_config`: editar `project_root` a la ruta real
   `/Workspace/.../twitch-analytics-lakehouse` y configurar Settings con el
   catálogo/esquema existentes. No instalar pyspark en Databricks.
3. Registrar commit, `spark.version` y ruta del módulo cargado:
   `import twitch_lakehouse.analytics; print(twitch_lakehouse.analytics.__file__)`.
   Reiniciar evita repetir el incidente de módulo antiguo/errorifexists.
4. Ejecutar notebook 07, fixture **ARTIFICIAL**, sin escrituras Delta. Debe dar
   PASS; incluye las seis consultas sobre vistas temporales del fixture.
   Si falla, detener. No extrapolar sus valores al dataset real.

## Publicación

5. Ejecutar configuración e inspección de notebook 08. Comparar printSchema con
   inventario; confirmar DESCRIBE HISTORY y permisos SELECT/CREATE TABLE en el
   esquema. Usar compute ya disponible en Free Edition, sin crear servicios.
6. Elegir release `r01` solo si no existe ninguno de sus cuatro destinos.
   Ejecutar última celda una vez: fija versiones, valida contrato y claves,
   equivalencia Gold I1/Silver; crea tres agregados, concilia contenido persistido
   y escribe quality al final. Ninguna fuente se modifica.
7. Si falla, conservar stack trace y candidatos. No reintentar con igual sufijo
   ni usar overwrite, DROP, replace o append de recuperación. Diagnosticar y
   escoger otra release para repetir el proceso completo. No consumir candidatos.
8. Conservar salida de notebook 08, versiones de entrada y DESCRIBE HISTORY/DETAIL
   de destinos. Quality debe tener exactamente una fila PASS con scope correcto.
   Mantener las tablas de la release inmutables; no permitir escritores externos.

## Consultas y dashboard

9. En SQL Editor seleccionar compute existente de Free Edition. Ejecutar cada
   archivo `sql/i3/*.sql` por separado. Si cambió configuración, sustituir
   namespace y sufijo `r01` en TODOS los archivos antes de usarlos.
10. Ejecutar 01 y 06 primero: exactamente una fila PASS y todos los deltas cero;
    NULL o ausencia de filas también es fallo. Para el snapshot I1 reportado,
    comparar con 3.051.733 conexiones, 162.625 streamers y 95.887.110 minutos.
    Son referencias reportadas, nunca constantes insertadas por el pipeline.
    Si el snapshot es diferente, investigar antes de exigir esos mismos números.
11. Ejecutar 02–05 con parámetros nombrados según esta tabla:

| Parámetro | Tipo | Valor inicial | Alcance |
| --- | --- | --- | --- |
| streamer | STRING | cadena vacía | coincidencia exacta; vacío=todos; 02,04,05 |
| top_n | INTEGER | 10 | 02; rango efectivo 1–1000 |
| step_from | INTEGER | 0 | 05, primer paso incluido |
| step_to | INTEGER | 2147483647 | 05, último paso incluido |
| bucket_steps | INTEGER | 6 | 05; pasos de diez minutos, mínimo efectivo 1 |

12. Crear dashboard AI/BI **borrador**, añadir seis datasets en Data pegando SQL,
    y dos páginas en Canvas según `i3_dashboard.md`. Conectar controles a los
    parámetros de cada dataset. No usar filtros de fecha ni sumar distintos.
13. Probar todos/tipo exacto/inexistente; Top 10/100; rango temporal completo y
    parcial; tamaños 1/6. Confirmar distribución e inicios completos con ranking
    del mismo streamer. Rango afecta solo R05. Formatear tasas como porcentajes.
14. Registrar parámetros, resultados, release, commit y capturas de ambas páginas.
    Solo entonces declarar «validado en Databricks». Guardar borrador hasta esa
    validación; publicación a usuarios no forma parte de esta entrega.

## Verificación local

`python -m unittest discover -s tests -v` usa omisiones explícitas si falta
PySpark. `python -m compileall -q twitch_lakehouse tests notebooks` revisa sintaxis;
`git diff --check` revisa espacios. Para fixtures locales usar entorno aislado
con PySpark 4.0.1 y JDK compatible con Spark 4 (17/21). Databricks aporta Spark:
no instalar otra versión allí. SQL local no demuestra UI, permisos ni Delta.

Referencias oficiales: [parámetros AI/BI](https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters),
[filtros](https://docs.databricks.com/aws/en/dashboards/manage/filters),
[saveAsTable / Spark Connect](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html).
