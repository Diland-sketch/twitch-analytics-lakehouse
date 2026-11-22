# 03. Diccionario, granularidad, relaciones y verificaciones

## Fuente UCSD

Archivo `100k_a.csv`, **sin cabecera**, cinco columnas en orden: `user_id,stream_id,streamer_username,time_start,time_stop`. Cada fila registra un intervalo observado de conexión de usuario al chat. Se valida que los IDs sean positivos, intervalos no invertidos, streamer no vacío, enteros válidos y cinco campos sin comillas. `estimated_minutes=(time_stop-time_start)*10`; admite cero. El archivo no contiene `message_text`, seguidores, espectadores concurrentes, calendario de emisiones ni visitas web.

## Databricks/Delta

| Tabla (prefijo `workspace.twitch_lakehouse.`) | Grano / clave lógica | Campos relevantes | Justificación |
| --- | --- | --- | --- |
| `bronze_interactions` | 1 línea de CSV (sin PK adicional) | `raw_line`, `source_file`, `ingested_at` | Preservación literal y fuente |
| `silver_interactions` | Tupla única (user_id, stream_id, streamer_username, time_start, time_stop) | Cinco campos normalizados + `estimated_minutes` | Datos válidos, deduplicados |
| `silver_rejects` | 1 fila inválida original | `raw_line`, `quality_errors`, procedencia | Auditoría de rechazo |
| `gold_streamer_metrics` | 1 fila / streamer | `interactions`, `unique_users`, `unique_streams`, `estimated_minutes` | Indicadores I1 |
| `gold_i3_streamer_r01` | 1 fila / streamer | + `recurring_users`, `recurrence_rate` | R01/R02/R03/R07 |
| `gold_i3_duration_r01` | streamer × `duration_bucket` 0..4 | `interactions`, `estimated_minutes` | R04 |
| `gold_i3_activity_r01` | streamer × `time_start` | `connection_starts`, `started_estimated_minutes` | R05 |
| `gold_i3_quality_r01` | 1 fila / release | status, scope, versiones, conteos, minutos, duplicados inferidos | R08, barrera de publicación |

Cambiar `r01` por sufijo real si el usuario generó otra release. Las claves son **lógicas**, no constraints Delta declarados; comprobar con SQL.

**Buckets:** 0 exactamente cero minutos; 1 (0,10]; 2 (10,30]; 3 (30,60]; 4 >60. El bucket no indica género de audiencia.

## SQLite local `monitor.sqlite`

| Tabla | PK declarada / campos | Lectura |
| --- | --- | --- |
| `events` | `event_key` PK; run_id, valid, streamer, estimated_minutes, batch_id | Ledger de eventos únicos |
| `transport` | (topic, partition, offset) PK, event_key | Confirmación de transporte; puede exceder `events` por reenvíos |
| `runs` | run_id PK; manifest, producer_status, acknowledged | Estado productor y origen |
| `streamer_metrics` | (run_id,streamer) PK; interactions, estimated_minutes | Agregados por ejecución |
| `batches` | (pipeline_id,batch_id) PK; input_rows, new_events, offsets | Micro-lotes confirmados |
| `consumer_status` | id=1; state, heartbeat, detail | Estado y última señal |
| `metadata` | key PK | Metadatos del consumidor |

`valid+invalid=total` en los eventos seleccionados. `SUM(streamer_metrics.interactions)=SUM(events.valid)` por `run_id` una vez asentado el lote. `transport` puede tener más filas que el ledger por reintentos; **no** sumar offsets a eventos únicos. SQL SQLite exacto está en `05_pruebas_evidencias.md`.

## API pública local

- `GET /api/v1/historical`: JSON estático validado: `schema_version=i3-historical-v1`, `release`, `exported_at`, `source_namespace`, `metrics`, `ranking` Top 20 y `duration` cinco buckets. **Sin registros individuales**.
- `GET /api/v1/snapshot?run_id=UUID`: `metrics(total, valid, invalid, streamer_interactions_total, estimated_minutes_total)`, `streamers`, `activity`, `runs`, `consumer`, `last_batch`, `transport_total`, `scopes` y marca temporal.
- `GET /api/v1/health`: disponibilidad de proceso API, **no** salud de Spark o Kafka.
- `WS /api/v1/stream?run_id=UUID`: snapshots completos, no incrementos sumados por frontend.

## Medidas aditivas / no aditivas

- `interactions` y `estimated_minutes` sí suman dentro de una sola familia y población de publicación.
- `unique_users` y `unique_streams` por streamer **no** representan distintos globales sumando filas.
- `recurrence_rate` y `top10_share` son razones, **no** sumar ni promediar por streamer.
- Nunca unir Gold I3 duration y activity solo por streamer para calcular totales (fanout).
- Los minutos de I4 y Gold I3 pertenecen a flujos diferentes; no sumar ambas cifras.

## Controles reproducibles

Ver `sql/01_inventario.sql` a `sql/06_quality.sql`, y `05_pruebas_evidencias.md`. Los SQL no modifican datos y dan conteos observables para el esquema configurado.
