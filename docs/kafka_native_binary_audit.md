# Auditoría urgente: Bronze nativa con key/value BINARY

Evidencia remota **reportada por el usuario**: fuente `full_a.csv.gz`, 1.000 ACK en
103,52 s y 1.000 filas en
`workspace.twitch_kafka_experimental.bronze_kafka_native_pilot_v1`, esquema únicamente
`key BINARY, value BINARY`. Esta actualización no vuelve a publicar ni a consumir Kafka.
El conteo confirma filas Bronze; el PASS de identidad/contenido debe obtenerse al
ejecutar el notebook actualizado con el manifiesto original de esa carga.

## Cambio y semántica del resultado

El notebook 22 conserva su nombre, binding, lectura de versión Delta fija y escritura
de un reporte nuevo `workspace.twitch_kafka_experimental.audit_<uuid>` por ejecución.
Usa `kafka_ingest.native_audit`: lectura batch acotada a 5.000 filas y decodificación
estricta UTF-8/JSON de los bytes persistidos. Sigue limitado al piloto de 1.000 fuentes.
No modifica Bronze ni tablas r01, checkpoints, productor, consumidor ni Bundle.

Comprueba las 1.000 identidades esperadas calculadas desde el manifiesto:
`SHA256(source_sha256 + '|' + twitch_source_row)`, ordinales enteros 1..1000, SHA de
fuente y load_id exactos, `historical_replay=true`, raw_line tipo string y key igual
al event_id UTF-8. Compara SHA256 de la lista JSON canónica ordenada de líneas con
`payload_sha256` del manifiesto. No basta tener COUNT(*)=1000.

| Resultado | Significado |
|---|---|
| source_coverage_status | PASS solo con todo el conjunto esperado, identidades válidas y contenido conciliado |
| identity_integrity_status | FAIL si hay identidades inválidas, claves incongruentes, duplicados de event_id o payloads en conflicto |
| status / status_scope | PASS de fuente e integridad, con alcance explícito SOURCE_IDENTITY_AND_CONTENT_ONLY; no es PASS de offsets |
| offset_audit_status | NO VERIFICABLE sin metadata completa; PASS/FAIL solo si existe topic/partition/offset real en todas las filas |
| offsets / transport_duplicates | null si no pueden verificarse; nunca se reemplazan con ordinales ni con ACK |

Un reenvío idéntico puede conservar cobertura fuente PASS y fallar la integridad
estricta del primer ensayo por duplicados. Un conflicto de payload hace fallar
cobertura y contenido. Incluso si offsets falla, el control de fuente se conserva
independientemente. El controlador Spark original sigue exigiendo evidencia de
transporte; no se debilita para aceptar este PASS parcial ni avanza su cursor.
El auditor no presupone que las filas Bronze sean mensajes físicos Kafka distintos.

## Ejecución inmediata en Databricks

1. Actualizar esta rama en el Git folder separado. En una celda independiente,
   ejecutar `dbutils.library.restartPython()` después de Pull; después abrir notebook
   `22_kafka_native_verify_pilot`. No volver a ejecutar el productor ni notebook 21.
2. Configurar `repo_root` absoluto, `pipeline_id`, `update_id` de la ingesta real y
   `manifest_json` con el JSON **original de full_a.csv.gz** de `work/<bloque>/manifest.json`.
   No usar el manifiesto de las pruebas locales de `100k_a.csv`, no regenerarlo y
   no cambiar hashes/load_id para hacer que coincida con Bronze.
3. Ejecutar notebook 22. No necesita credenciales Kafka, columna kafka_source ni
   otro consumidor. Verifica la versión Delta fijada y conserva el reporte antes
   de elevar FAIL. Un binding previo distinto detiene la ejecución sin sobrescribirlo.
4. Guardar tabla de auditoría, versión Delta y salida JSON. Resultado esperado si
   el contenido realmente concilia (ejemplo de forma, **no ejecución real**):

```json
{
  "status": "PASS",
  "status_scope": "SOURCE_IDENTITY_AND_CONTENT_ONLY",
  "source_coverage_status": "PASS",
  "identity_integrity_status": "PASS",
  "bronze_rows": 1000,
  "source_rows": 1000,
  "distinct_event_ids": 1000,
  "duplicate_event_id_rows": 0,
  "identity_errors": 0,
  "payload_conflicts": 0,
  "offset_audit_status": "NO VERIFICABLE",
  "offsets": null,
  "transport_duplicates": null
}
```

Para comprobar el reporte persistido, sustituir UUID por el nombre devuelto:

```sql
SELECT report_json FROM workspace.twitch_kafka_experimental.audit_UUID;
```

No eliminar auditorías anteriores. Si ya existe un PASS de cobertura, conservarlo
con su manifiesto/versión: la ausencia de offsets se registra como otro control.

## Metadata Kafka: configuración documentada, sin reconstrucción retroactiva

La documentación confirma que key/value es el esquema predeterminado; la metadata
es opt-in. En el objeto de tabla de `ingestion_definition.objects`, como hermano
de `connector_options`, el ajuste es:

```yaml
table_configuration:
  source_metadata_column: kafka_source
```

Este ajuste **ya figura en el Bundle versionado**. Inspeccionar la definición del
pipeline remoto actual y compararla con lo desplegado: que exista en Git no significa
que se haya aplicado al pipeline efectivo. Verificar también el nivel de anidación,
destino exacto y canal PREVIEW. No añadir columnas manualmente con ALTER TABLE para
aparentar que Lakeflow las produce, ni unir ACK a filas por posición.

Procedimiento propuesto, no ejecutado automáticamente:

1. Guardar el JSON de configuración actual, pipeline ID, versión/esquema Delta y
   reporte de cobertura fuente PASS antes de cambiar configuración.
2. Si el pipeline fue creado fuera de este Bundle, no ejecutar deploy sin vincular
   primero el recurso existente. Revisar `databricks bundle deployment bind
   kafka_bronze_native PIPELINE_ID -t experimental` con el mismo workspace/target y
   variables. No crear otro pipeline ni cambiar el nombre de tabla/conexión/topic.
3. Incorporar únicamente `table_configuration.source_metadata_column` a la definición
   efectiva del mismo recurso, validar y revisar el cambio antes de una actualización
   normal. No Full refresh, reset ni limpieza de estado.
4. **La documentación consultada describe la opción, pero no garantiza habilitación
   incremental sobre una tabla ya creada ni backfill de las 1.000 filas existentes.**
   Si el API exige Full refresh o reemplazar la tabla, detenerse y conservar el piloto;
   la auditoría fuente sigue siendo útil y offsets permanece NO VERIFICABLE.
5. Si el workspace acepta el cambio sin reiniciar estado, inspeccionar el esquema y
   filas nuevas en una prueba posterior controlada. No prometer metadata para filas
   antiguas. Si estas quedan con metadata null, el control global seguirá NO
   VERIFICABLE; un futuro control por versión/rango con metadata debe informar su alcance.

Referencias: [opciones del conector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-reference),
[limitaciones y metadata opt-in](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-limits),
[ejemplo de metadata](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-pipeline),
[vincular recurso existente sin recrearlo](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands).

## Pruebas y límites

Pruebas unitarias cubren: key/value sin metadata, duplicado sustituyendo una fila
faltante aunque COUNT sea 1000, duplicado idéntico, conflicto de payload, hash alterado,
SHA/load/ordinal/event_id incorrectos, key incongruente, JSON inválido, metadata completa,
metadata parcial, offsets duplicados, entrada vacía y límite de tamaño. Un fixture Spark
comprueba bytes BINARY reales sin kafka_source. No equivalen al PASS remoto pendiente.

Validación local realizada: 34 pruebas de contratos/regresión PASS y 5 pruebas con
Spark 3.5.8/Java 17 PASS (37,730 s), sin red externa. No se ejecutó el notebook contra
el workspace ni se actualizó el pipeline remoto durante este cambio.

```powershell
python -m unittest tests.test_kafka_native_audit -v
```
