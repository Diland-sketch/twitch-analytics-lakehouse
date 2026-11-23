# Bronze Kafka v7 → Silver/Gold experimental

**Ejecución remota completada con PASS** en Databricks Free Edition para
`kafka_b7_r001`: 1.000 fuentes, 1.000 filas Silver, 397 streamers Gold, 1.000
interacciones y 41.600 minutos estimados; 0 rechazos, 0 duplicados fuente y 0
duplicados de negocio. Los 14 controles pasaron leyendo los destinos persistidos.
Duración mostrada por UI: 1,8 minutos (valor redondeado, no latencia exacta).
[Evidencia JSON](evidence/kafka_b7_r001_remote.json).
Resultado persistido: `workspace.twitch_kafka_experimental.reconciliation_kafka_b7_r001`.
Offsets continúa **NO VERIFICABLE**. Estas cifras corresponden solo al piloto.

El nombre `kafka_b7_r001` ya está ocupado por esta ejecución exitosa: no volver a
ejecutar el notebook con ese release. Consultar sus tablas para verificar; para
otra ejecución revisada usar un nombre nuevo, sin borrar la versión confirmada.

Entrada autorizada: `workspace.twitch_kafka_experimental.bronze_kafka_native_pilot_v1`
**versionAsOf=7**, respaldada por
`workspace.twitch_kafka_experimental.audit_9663046b15734333a35d395af3486d99`.
No lee Kafka ni inicia/actualiza Lakeflow. Es Spark batch sobre Delta existente.

## Contrato y autorización

`kafka_ingest.medallion.run` exige la auditoría PASS de identidad/contenido y el
binding original `native_pilot_binding_v1`. Fija también versiones Delta de esas dos
tablas, conserva su JSON íntegro y vuelve a verificar contenido e identidades de
Bronze v7 contra el manifiesto original. Solo después permite crear destinos.
El control de offsets se conserva como **NO VERIFICABLE**, sin deducirlo de ordinales.
Un nuevo Bronze v8 no cambia esta población. No se usa el manifiesto local de 100k_a.csv.

JSON binario: esquema `ENVELOPE` explícito (`from_json`, sin inferencia), IDs de carga
y fuente, ordinal BIGINT, event_id, indicador histórico, raw_line y tiempo técnico.
JSON/identidad/key inválidos o conflicto de payload bloquean la ejecución completa,
porque contradicen la autorización; no se cuentan como rechazos CSV ordinarios.

CSV: se reutiliza `classify` de I1: cinco campos separados por coma sin comillas,
IDs numéricos positivos, streamer no vacío normalizado con trim, intervalos relativos
no negativos y límites enteros, stop >= start. Cada razón queda en `quality_errors`.
Minutos estimados = `(time_stop - time_start) * 10`. No se inventan fechas históricas.

Deduplicación ordenada:

1. Identidad fuente `(source_sha256, twitch_source_row)`, contando `source_occurrences`.
   Contenido divergente para la misma identidad causa fallo. Representante ordenado
   por payload original para que sea reproducible incluso con reenvíos.
2. Tras calidad, clave de negocio
   `(user_id, stream_id, streamer_username, time_start, time_stop)`. Conservar menor
   ordinal/event_id como representante, sin sumar tiempo de duplicados.

## Destinos nuevos y recuperación

Release inicial `kafka_b7_r001`; namespace fijo `workspace.twitch_kafka_experimental`.

| Tabla (prefijo + `_kafka_b7_r001`) | Contenido |
|---|---|
| manifest | Auditoría autorizante completa, versiones, binding y revalidación fuente; estado AUTHORIZED_NOT_PUBLISHED |
| source_classification | Una fila por identidad fuente, payload original, campos normalizados, ocurrencias, calidad y disposición |
| rejects | Filas fuente rechazadas con sus razones y procedencia |
| silver | Una fila por KEY válida, representante fuente y minutos estimados |
| gold | Interacciones, usuarios únicos, transmisiones únicas y minutos por streamer |
| reconciliation | Resultado PASS/FAIL, ecuaciones, conteos y referencias a tablas |

`disposition` es SILVER, REJECTED o BUSINESS_DUPLICATE. Las razones múltiples de
rechazo no se deben sumar como si representaran fuentes diferentes. Silver conserva
source_sha256, twitch_source_row, twitch_load_id, event_id y raw_line del representante;
classification conserva las fuentes excluidas por deduplicación de negocio.

Se usa `storage.write_new` (CREATE estricto + append), no overwrite/errorifexists.
Si cualquier destino existe, la ejecución se detiene antes de leer/procesar. No
adopta tablas antiguas, ni las vacía ni añade filas en reintentos. Un fallo tras crear
destinos deja datos parciales y una tabla nueva `failure_<uuid>_kafka_b7_r001` cuando
Spark permite registrarla. Conservar todo; diagnosticar y elegir `kafka_b7_r002` para
otro intento revisado. Nunca borrar un release parcial para reutilizar su nombre.
No ejecutar dos escritores del mismo release ni modificar manualmente sus tablas.

No hay transacción multi-tabla: únicamente `reconciliation` con PASS autoriza usar
esta versión para visualización/publicación. Manifest AUTHORIZED no equivale a éxito.

## Conciliación

Se comprueba antes de escribir y nuevamente leyendo las tablas persistidas:

* Bronze = fuentes distintas + duplicados fuente.
* Fuentes distintas = rechazadas + duplicados de negocio + filas Silver.
* Silver tiene KEY únicas y coincide mediante `exceptAll` bidireccional con las KEY
  válidas distintas de classification.
* Rechazos coinciden exactamente con classification REJECTED.
* SUM(Gold.interactions) = COUNT(Silver).
* SUM(Gold.estimated_minutes) = SUM(Silver.estimated_minutes).
* Gold coincide por `exceptAll` bidireccional con una agregación recalculada de Silver,
  incluidos usuarios/transmisiones distintos, no solo las sumas.
* Conteo persistido de Bronze concilia con las 1.000 filas de la autorización.
* Cada tabla persistida coincide con el plan de transformación mediante exceptAll
  bidireccional, incluyendo payload y procedencia, no solo conteos de negocio.

No se presupone que las 1.000 fuentes sean válidas ni que Silver tenga 1.000 filas.
Usuarios/transmisiones únicos se calculan sobre Silver global del piloto; no se suman
distinct parciales. No se presenta como espectadores activos ni retención real.

## Ejecución reproducible Free Edition

1. Actualizar esta rama en el Git folder experimental sin descartar cambios locales.
   Reiniciar módulos Python en una celda independiente antes de importar:
   `dbutils.library.restartPython()`.
2. Abrir `notebooks/23_kafka_silver_gold_pilot`. Ajustar `repo_root` al Git folder
   absoluto y `release_id=kafka_b7_r001` (o siguiente nombre nuevo tras diagnóstico).
3. Usar el Serverless gratuito ya disponible. No crear clústeres, Jobs ni recursos
   externos. No instalar PySpark ni conectores Kafka; no hay lectura streaming.
4. Ejecutar ambas celdas Python. El notebook valida autorización y destinos antes
   de escribir. Guardar el JSON final y la referencia reconciliation con PASS.
5. Si falla, conservar la salida/error y los destinos parciales. No Full refresh,
   reset de offsets, limpieza de checkpoints ni ejecución del productor.

```sql
SELECT report_json
FROM workspace.twitch_kafka_experimental.reconciliation_kafka_b7_r001;
SELECT disposition, COUNT(*) AS source_rows, SUM(source_occurrences) AS bronze_rows
FROM workspace.twitch_kafka_experimental.source_classification_kafka_b7_r001
GROUP BY disposition;
SELECT quality_errors, COUNT(*) AS rejected_source_rows
FROM workspace.twitch_kafka_experimental.rejects_kafka_b7_r001 GROUP BY quality_errors;
SELECT * FROM workspace.twitch_kafka_experimental.gold_kafka_b7_r001
ORDER BY interactions DESC, streamer_username;
```

Pruebas: `python -m unittest tests.test_kafka_medallion -v` (autorización/destinos)
y `python -m unittest tests.test_kafka_medallion_spark -v` (Spark real con fixtures
artificiales: rechazos, duplicado fuente, duplicado de negocio, sumas, distinct,
conflictos, JSON inválido y alteración de agregados). No equivalen a ejecución remota.

React queda para un incremento posterior: solo consumir una versión Gold cuya
reconciliation sea PASS, sin mezclarla con r01 ni con el laboratorio SQLite.
