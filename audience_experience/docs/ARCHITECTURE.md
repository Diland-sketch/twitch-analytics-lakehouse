# I5 — Auditoría y propuesta previa

Base auditada: I4 `5afba7e`, PR #3 abierto en borrador, sin merge. Rama nueva
`feat/i5-react-live-experience`. I1/I2/I3/I4 quedan byte por byte intactos.

I4 ya dispone de ledger idempotente, agregados Spark, estados y lector
`live_monitor.storage.snapshot(path, run_id)`: SQLite mode=ro, BEGIN/rollback
para snapshot coherente. Al pasar path explícito no llama state_dir ni crea
directorios. Se reutiliza sin modificar. Actividad/usuarios por ejecución;
micro-lote, consumidor y offsets globales: el contrato I5 conserva ese alcance.

## Arquitectura mínima

SQLite I4 persistente → FastAPI solo lectura → WebSocket / HTTP → React+ECharts.
Nginx sirve frontend y proxy same-origin a API, solo 127.0.0.1:5173. API sin
puerto publicado, conexión SQLite mode=ro. No endpoints de control
ni publicación. Productor y consumidor originales se operan externamente como I4.
Streamlit conserva su puerto 8501 como respaldo.

Carpetas: `api/` (contrato/lector/rutas), `web/` (React, UI shadcn, ECharts,
conexión y pruebas), `tests/` (API), `docs/` (arquitectura/evidencia), `e2e/`
(navegador real + productor I4). Compose y Dockerfiles separados, sin editar I4.

## Contratos v1

GET `/api/v1/health`: disponibilidad del proceso API; no afirma salud Spark.
GET `/api/v1/snapshot?run_id=<UUID opcional>`: JSON con schema_version,
revision, observed_at, selection, metrics, streamers, activity, runs, consumer,
last_batch, transport_total y status. Sin selección usa todas las ejecuciones.
Una ejecución preparada sin filas devuelve vacío real (0), no una simulación.
GET inválido: 422; almacenamiento ausente/ilegible: 503 con código estable.
WS `/api/v1/stream?run_id=…`: mensajes type=snapshot o type=unavailable.
Relectura cada 2s; revisión calcula hash de estado, no genera observaciones.
Consumo stale tras 15s sin heartbeat, aunque el socket API esté conectado.

El frontend recibe snapshots completos, nunca deltas sumados localmente.
Al fallar WS activa polling cada 2s y reconexión con espera acotada; mantiene
último dato y etiqueta desconexión. Se cancela cualquier respuesta vieja al
cambiar run_id. Sin datos carga/vacío/indisponible tienen estados diferentes.
Actividad limitada a últimos 300 puntos para render; acumulado calculado sobre
todo el resultado; ranking Top 20 de I4 no se presenta como población completa.

## Límites y verificación

Lector I4 no está diseñado para 124M y materializa actividad; el MVP de 100
eventos lo reutiliza antes de introducir otra consulta redundante. No se añade
esquema ni índices. Contador de eventos es ledger I4, no espectadores activos.
Fechas del gráfico son procesamiento real del replay, no calendario histórico.
Métricas son selección; último lote/offsets/heartbeat globales, etiquetados.
ECharts interpola gráficos entre snapshots; KPI muestra exactamente el entero
persistido, sin conteo artificial. Respeta reduced-motion y conserva tabla textual.

Hallazgo en integración real: montar el volumen completo con read_only impedía
abrir SQLite WAL cuando no existían -wal/-shm. Una propuesta de permitir
coordinación auxiliar mediante montaje RW fue rechazada por la revisión
automática: ampliaría el acceso del contenedor al estado I4. El montaje se
conserva RO. Alternativa segura: conexión persistente mode=ro sin transacción de
lectura abierta, que mantiene el ciclo de vida WAL mientras la API está activa;
los snapshots siguen usando sus transacciones breves originales. Solo puede
adjuntarse cuando el escritor I4 haya creado auxiliares legibles; de faltar,
devuelve 503 y reintenta. No crea archivos en el volumen RO. No se usa immutable=1
(ignoraría cambios WAL), no se copian ficheros activos ni se cambia journal_mode.
Después de un arranque completamente frío puede requerirse que I4 procese un
lote para que la API se adjunte. Esta condición se prueba y se documenta.
[Condiciones oficiales de lectura WAL](https://www.sqlite.org/wal.html#read_only_databases).

Aceptación: API/WS mismos conteos; React 0→parcial→100 en ejecución nueva con
CSV real, 100 acks y conciliación SQLite; recuperación polling/reconexión; UI
responsive y screenshots. No declarar éxito sin observarlo. No publicar cloud.
