# 08. Guion y secuencia de demostración reproducible

**Duración orientativa de exposición:** 12–18 minutos, adaptable al profesor; las duraciones no son métricas del sistema. Preparar previamente Docker y Databricks con resultados cargados. El productor de 100 registros a rate 5 tarda por el intervalo de envío más procesamiento variable; no prometer tiempo fijo de pantalla.

## Escena 1 — Problema y dato (2 min)

«No estamos midiendo espectadores concurrentes, sino registros de presencia observada en el chat de Twitch. Queremos transformar una fuente cruda en indicadores auditables y demostrar cómo se mueve por una arquitectura de datos». Mostrar CSV de cinco campos y fórmula de minutos estimados. Mostrar hash SHA256 del CSV, commit y rama `desarollo`.

## Escena 2 — Medallion Databricks (3 min)

Mostrar arquitectura `02_arquitectura.md` y el esquema `workspace.twitch_lakehouse`. Abrir SQL editor, ejecutar **por separado**:
- `sql/01_inventario.sql`: existencia y tipo.
- `sql/02_conciliacion_i1.sql`: conteos Bronze/Silver/Gold/minutos/rechazos.
- `sql/03_clave_silver.sql`: cero claves repetidas.
- `sql/04_gold_i1.sql`: cero diferencias frente a Silver.

Decir: «Bronze conserva, Silver valida/deduplica y Gold agrega. La igualdad de conteos y de sumas muestra que no perdimos interacciones válidas sin explicación». Mostrar DESCRIBE HISTORY y versiones, no reejecutar notebooks de escritura.

## Escena 3 — Analítica avanzada (2 min)

Abrir `notebooks/09_i3_release_preflight.py` (solo lectura) y **`sql/06_quality_i3.sql`** (quality PASS, siete deltas cero). Mostrar ranking y recurrencia en Databricks o React histórico. Explicar que recurrencia = usuarios con al menos dos filas, NO retención por fecha. Mostrar duración y concentración; no sumar usuarios únicos ni tasas. Mostrar el documento `docs/i3_requerimientos.md` para defender las 8 preguntas R01–R08.

## Escena 4 — Frontera Databricks–React (1 min)

En React, abrir **Análisis de audiencia** y mostrar release/exported_at; explicar: «Una exportación JSON generada con notebook 10 conserva métricas Gold verificadas, sin credenciales Databricks en navegador. Es un snapshot y se actualiza manualmente». Corroborar `GET /api/v1/historical` y comparar KPIs contra SQL/quality. No llamarlo integración automática en vivo.

## Escena 5 — Replay por Kafka y Spark (4–6 min)

Desde PowerShell elegir `$runId=[guid]::NewGuid().ToString()`. Abrir `http://localhost:5173/?run_id=$runId`: evidencia de **0** antes de publicar. Ejecutar productor con `--count 100 --rate 5` o `scripts/validar_demo.ps1` que captura evidencias. Mostrar **Laboratorio → Resumen** con evolución 0→parcial→100; **Audiencia del replay** con streamers; **Procesamiento** con Kafka, Spark, micro-lotes y offsets.

Corroborar `GET /api/v1/snapshot?run_id=UUID` y comprobar `total=valid+invalid` y `valid=streamer_interactions_total`. Explicar: el productor confirmó el transporte, Spark confirmó después el procesamiento, SQLite guarda ledger; el reloj solo refresca UI.

## Escena 6 — Recuperación/idempotencia (2 min)

Una vez terminado el lote y guardados conteos, reiniciar solo Spark: `docker compose -f live_monitor/compose.yaml restart spark`. Volver a consultar **mismo run** y comprobar mismo `total`. Si queda tiempo, reenviar **mismo run/rango con la misma fuente**, esperar a que Spark drene y demostrar `total` constante aun cuando `transport_total` global aumenta. No reiniciar el broker Kafka ni borrar volúmenes.

## Escena 7 — Cierre (1 min)

Resumen de comprobaciones: batch 3.051.733 Silver / 162.625 streamers / 95.887.110 minutos estimados (valores a corroborar en ese workspace), calidad I3 PASS, replay 100 o 1.000 únicos por run, recuperación conservada. Reconocer límites: sin API Twitch en vivo, sin alta disponibilidad ni SLA ni 124M procesados.

## Respuestas a preguntas probables

- **¿Por qué Kafka si ya tenemos Databricks?** Demuestra transporte/asimilación de eventos incremental y resiliencia local; no es canal directo a Gold actual.
- **¿Por qué SQLite y no Delta para el streaming?** Es un sink pequeño transaccional para el MVP en Docker; no una arquitectura de producción para 124M.
- **¿Cómo prueban calidad?** Contratos por campo, cuarentena, clave Silver, conteos, sumas, quality I3, SQL de deltas cero, run ledger y API.
- **¿Cómo saben que no duplican?** Clave I1 de cinco campos; I4 `event_id` estable por run+fuente+ordinal y PK SQLite, más checkpoint. Se valida con reenvío.
- **¿Qué significa “tiempo real”?** Kafka/Spark procesan conforme se emiten registros históricos; no llegan eventos de Twitch hoy ni existe latencia garantizada.
- **¿Cuál es la fuente de verdad?** Para batch, tablas Delta versionadas I1/I3; para laboratorio, ledger SQLite confirmado por Spark. React es solo lector.

## Checklist previo

- [ ] Git en `desarollo`, commit registrado, sin cambios inesperados.
- [ ] JSON Gold legítimo válido y `GET /historical` 200.
- [ ] Kafka healthy, Spark RUNNING, React accesible.
- [ ] Nuevo UUID, snapshot inicial en 0 (captura).
- [ ] SQL 01–06 y preflight 09 en PASS, sin escrituras.
- [ ] Proyección dual monitor y SQL lista; sin secretos visibles en terminal.
- [ ] Prueba 100 o 1.000 y reinicio planeado; capturas y logs locales, no en Git.
- [ ] Plan alternativo: capturas/evidencia anterior, marcadas como **grabación previa**, si el entorno falla. Nunca declarar ejecución en directo si no ocurrió.
