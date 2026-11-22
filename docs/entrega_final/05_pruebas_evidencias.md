# 05. Pruebas, comprobaciones exactas y custodia de evidencias

## Regla de aceptación

Una prueba es **PASS** solo con comando, origen, resultados observados, identidad/versión y evidencia conservados; un indicador verde aislado no basta. **FAIL/NO EJECUTADO** no se convierte en PASS por un resultado previo del compañero. Capturar fecha local y UTC.

## Matriz

| Código | Precondición | Prueba/artefacto | Condición de aprobación | Estado fuente |
| --- | --- | --- | --- | --- |
| P01 | `100k_a.csv` cargado | `03_bronze`, SQL 01/02 | Fuente=Bronze>0 | Ejecutada según equipo |
| P02 | Bronze | `04_silver`, SQL 02/03 | Bronze=Silver+Rejects+Duplicados; clave única | Ejecutada según equipo |
| P03 | Silver | `05_gold`, SQL 02/04 | SUM(interacciones/minutos) Gold = Silver | Ejecutada según equipo |
| P04 | Gold I1 | `07,08,09`, SQL 05/06 | 1 quality PASS, 7 deltas cero, versiones no nulas | Usuario reportó notebooks |
| P05 | I3 publicada | `10`, `GET /historical` | release, KPIs, Top 20, 5 buckets válidos | Usuario reportó React histórico |
| P06 | Docker activo | `docker compose ps`, `/health`, logs | Kafka healthy, Spark y API activos | Usuario reportó prueba local |
| P07 | Run nuevo | Productor / API / JSON | Acks=N, 0→parcial→N, valid+invalid=N | 100 E2E documentados |
| P08 | Run completo | `restart spark`, misma API | Total antes=después=N | 100 E2E documentados; 1.000 reportados |
| P09 | Run completo | Reenviar mismo `run_id`/CSV/rango | offsets aumentan, eventos únicos constantes | 100 E2E documentados; nueva prueba opcional |
| P10 | Servicios activos | CI + `npm test`, `npm run build`, pytest | Suites PASS sin omisiones inesperadas | Consultar commit exacto |
| P11 | Dos áreas React | Navegador de escritorio/móvil | Análisis sin controles Kafka; laboratorio mantiene filtros/WS | Usuario reportó funcional |
| P12 | Fuentes protegidas | `docker inspect`, Git status, catálogo | API RO y rutas sin DML/DDL, sin CSV/DB/secretos en Git | Verificar en cada entrega |

## Auditoría de Databricks por sentencias SQL

Abrir Databricks SQL Editor con Serverless; usar los archivos **separados** de `sql/`: 01 catálogo/inventario, 02 conteos y minutos, 03 claves de Silver, 04 comparación Gold I1, 05 dominios y poblaciones I3, 06 quality/deltas. **No son sentencias de escritura**. Si una consulta no devuelve filas o da NULL donde se espera cero, eso es **FAIL**. Revisión de historia Delta (sustituir ruta según release):

```sql
DESCRIBE DETAIL workspace.twitch_lakehouse.bronze_interactions;
DESCRIBE HISTORY workspace.twitch_lakehouse.bronze_interactions LIMIT 5;
DESCRIBE DETAIL workspace.twitch_lakehouse.silver_interactions;
DESCRIBE HISTORY workspace.twitch_lakehouse.silver_interactions LIMIT 5;
DESCRIBE DETAIL workspace.twitch_lakehouse.gold_streamer_metrics;
DESCRIBE HISTORY workspace.twitch_lakehouse.gold_streamer_metrics LIMIT 5;
DESCRIBE DETAIL workspace.twitch_lakehouse.gold_i3_quality_r01;
DESCRIBE HISTORY workspace.twitch_lakehouse.gold_i3_quality_r01 LIMIT 5;
```

El marcador quality es acotado a I1/I3 y sus reglas, no auditoría universal. Para reporte más estricto Python: `notebooks/09_i3_release_preflight.py` (solo lectura).

## HTTP y comprobación de ejecución local

```powershell
$runId = "<PEGAR_UUID_REAL>"
$base = "http://localhost:5173"
$health = Invoke-RestMethod "$base/api/v1/health"
$gold = Invoke-RestMethod "$base/api/v1/historical"
$live = Invoke-RestMethod "$base/api/v1/snapshot?run_id=$runId"
$health
$gold.release; $gold.metrics
$live.metrics
if ($live.metrics.total -ne ($live.metrics.valid + $live.metrics.invalid)) { throw "FAIL: total != valid+invalid" }
if ($live.metrics.streamer_interactions_total -ne $live.metrics.valid) { throw "FAIL: agregados distintos a valid" }
```

- `GET /health` confirma proceso API solamente.
- `GET /historical` confirma contenido JSON validado local; no asegura que Databricks esté disponible ahora. Comparar su `release`, `exported_at` y métricas con el notebook 10 y SQL.
- `GET /snapshot?run_id=...` confirma persistencia por run; `transport_total`, batches y heartbeat son globales. Para UI en vivo observar WebSocket en DevTools/red con mensajes `type=snapshot` y evolución de contadores sin manipular.
- `Invoke-RestMethod` en caso 503: registrar falla real y diagnosticar Spark/WAL; no afirmar cero eventos.

## SQLite: SELECT de auditoría, sin modificar estado

Para ejecutar las consultas con el contenedor **tools** que ya incluye Python y montaje de estado, no copiar `monitor.sqlite` con Spark activo (WAL): usar el lector `live_monitor.control status` y snapshots API consistentes. Si se requiere SQL SQLite directo, primero **detener escritor Spark** y productor, luego abrir la BD con un cliente SQLite RO en el mismo volumen, manteniendo sus archivos auxiliares. Consultas:

```sql
-- :run_id se sustituye por UUID real en el cliente SQLite.
SELECT run_id, COUNT(*) AS total, SUM(valid) AS validos,
       COUNT(*)-SUM(valid) AS invalidos,
       SUM(CASE WHEN valid=1 THEN estimated_minutes ELSE 0 END) AS minutos
FROM events WHERE run_id = :run_id GROUP BY run_id;

SELECT run_id, SUM(interactions) AS interacciones,
       SUM(estimated_minutes) AS minutos
FROM streamer_metrics WHERE run_id=:run_id GROUP BY run_id;

SELECT COUNT(*) AS total_transportes FROM transport;
SELECT COUNT(*) AS total_eventos_unicos FROM events;
SELECT pipeline_id,batch_id,new_events,valid_events,invalid_events,completed_at
FROM batches ORDER BY completed_at DESC LIMIT 10;
```

No se exige que el transporte coincida con los eventos únicos; duplicados de reenvío generan transporte adicional. Comparar minutos solo de eventos válidos contra los agregados del **mismo run**.

## Evidencia ya reportada (no generada en esta auditoría)

- I1: 3.051.733 Bronze/Silver, 162.625 Gold, 95.887.110 min estimados, cero rechazos/duplicados actuales (`docs/validacion.md`).
- I4: primer E2E 100 válidos, 0 inválidos, 2070 min; reenvío conserva 100 únicos (`live_monitor/VALIDATION.md`).
- I5: E2E 100, WebSocket/React 0→parcial→100, SHA256 y capturas locales documentadas (`audience_experience/docs/VALIDATION.md`).
- Equipo/usuario: otra ejecución local con 1.000 únicos, 1.000 válidos, cero inválidos y 1.000 después de `restart spark` (sin logs compartidos). Registrar al repetir mediante script.

## Carpeta de pruebas y manifiesto

Guardar `work/entrega_final/<run_id>/` (ignorada por Git): `manifest.json` (commit, CSV hash, ruta, run, conteo, timestamps), `antes.json`, `despues.json`, `posterior_reinicio.json` si procede, `producer.log`, resultados `test`, screenshots React 0/intermedio/final y Gold, `sql_results/` con CSV/JSON exportados del SQL Editor, `versions.txt`. Generar hashes:

```powershell
Get-ChildItem "work/entrega_final/<run_id>" -File |
  Get-FileHash -Algorithm SHA256 | Format-Table Path,Hash -AutoSize
```

No subir datos ni capturas reales al repositorio. Ante fallo, conservar salidas, no limpiar volúmenes ni adulterar evidencia.
