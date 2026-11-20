# Contrato I5 v1

Todos los endpoints son de lectura. Same-origin en localhost:5173; API interna
8000 sin puerto host. No rutas POST/PUT/PATCH/DELETE de negocio. UUID inválido
produce 422. Las rutas automáticas de documentación no controlan I4.

## GET /api/v1/health

Devuelve service, status=ok, read_only=true: salud del proceso API, no de Spark.

## GET /api/v1/snapshot?run_id=UUID

Sin run_id devuelve todas las ejecuciones; con UUID prepara/filtra una ejecución.
HTTP 200, Cache-Control=no-store. Modelo OpenAPI `SnapshotResponse`:

| Campo | Semántica |
| --- | --- |
| schema_version | `1.0` |
| revision | hash de estado para identificar snapshot; no contador de eventos |
| observed_at | UTC real de lectura, no fecha del dataset |
| status | ready si hay eventos en selección, empty si hay cero filas |
| selection.run_id | UUID elegido o null (todas) |
| metrics.total/valid/invalid | eventos únicos en ledger I4 y clasificación Spark |
| metrics.streamer_interactions_total | suma completa de agregados válidos Spark, no solo Top 20 |
| metrics.estimated_minutes_total | minutos estimados válidos; no atención probada |
| streamers[] | streamer, interactions, estimated_minutes, rank; Top 20 |
| activity[] | completed_at, events y cumulative_events; últimos 300 puntos de selección |
| runs[] | run_id, producer_status, acknowledged (último intento), requested, source_name |
| consumer | estado/heartbeat/stale/edad de señal; null si no hay estado; GLOBAL |
| last_batch | pipeline_id, batch_id, completed_at, input/new/valid/invalid y offsets decodificados; GLOBAL |
| transport_total | offsets únicos registrados; GLOBAL, incluye reenvíos |
| scopes | alcance de cada familia para evitar filtros o denominadores ambiguos |

`activity.cumulative_events` se calcula sobre todos los puntos antes de truncar.
Un run_id desconocido devuelve cero real y no crea una fila de manifiesto.
Selección distinta no cambia estado global del consumidor ni último lote.

Si el lector no puede abrir la DB/WAL: HTTP 503,
`detail.code=storage_unavailable`. No se devuelve un snapshot cero sustituto.
Un cliente con dato previo lo conserva con advertencia. No se expone path ni
stack trace al cliente. Se respetan transacciones y locks SQLite originales.

## WS /api/v1/stream?run_id=UUID

Mismo filtro y contrato. Mensajes:

```json
{"type":"snapshot","data":{"schema_version":"1.0","revision":"…","metrics":{"total":100}}}
```

El ejemplo está abreviado para documentación, no es un resultado nuevo.
Fallo temporal: `{"type":"unavailable","code":"storage_unavailable"}`.
Origin ajeno al Host del servicio se rechaza con código 1008. Sin CORS abierto.
Relectura cada 2s; cliente sustituye snapshot entero, no acumula mensajes.

El frontend usa HTTP al abrir y cuando falla WS; reconecta con espera acotada.
API o WS conectados no indican que Spark reciba eventos actuales: se muestra
por separado estado y antigüedad del heartbeat persistido. UTC de procesamiento
se presenta en hora local del navegador. El origen siempre se etiqueta histórico.
