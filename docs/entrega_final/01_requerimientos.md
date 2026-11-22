# 01. Requerimientos y matriz de aceptación

## Propósito y alcance

Analizar **interacciones de presencia en chat** de Twitch usando un subconjunto histórico UCSD `100k_a.csv` y demostrar un flujo de eventos Kafka→Spark→SQLite→React a velocidad configurable. La arquitectura Medallion de Databricks es batch; el monitor local es streaming sobre un replay. Las estadísticas **no** acreditan espectadores activos, mensaje de chat, seguidores, dinero o audiencia concurrente.

## Funcionales

| ID | Requerimiento verificable | Implementación | Aceptación/evidencia |
| --- | --- | --- | --- |
| RF01 | Cargar CSV de cinco campos preservando línea/procedencia | `03_bronze`, `ingest_bronze` | Conteo origen = Bronze; tabla Delta y origen identificados |
| RF02 | Validar campos y registrar rechazados | `04_silver`, `classify` | `bronze = valid_raw + rejects`; motivos conservados |
| RF03 | Deduplicar clave I1 y estimar minutos | `silver_frames` | Clave de cinco campos sin duplicados; minutos = diferencia ×10 |
| RF04 | Agregar interacciones, usuarios, transmisiones, minutos por streamer | `05_gold` | SUM(Gold.interactions)=Silver; minutos concilian |
| RF05 | Consultar recurrencia, duración, inicios relativos, concentración | `08_i3_analytics`, SQL I3 | R01–R08 de `docs/i3_requerimientos.md`, `09_i3_release_preflight` PASS |
| RF06 | Publicar I3 por release sin destruir fuentes | `analytics_pipeline.py` | Cuatro tablas `gold_i3_*_rXX`, una quality PASS, versiones Delta |
| RF07 | Exportar agregados verificados para React, sin credenciales | `10_i3_export_snapshot.py` y `api/historical.py` | JSON `i3-historical-v1`, GET historical 200, poblaciones coinciden |
| RF08 | Reproducir rango controlado de CSV por Kafka | `live_monitor/producer.py` | `run_id` UUID, manifest SHA256/rango, `acknowledged=count` |
| RF09 | Procesar Kafka con Spark Structured Streaming y calidad | `live_monitor/consumer.py` | Persistencia en SQLite; `valid + invalid = total` por run |
| RF10 | Mantener identidad única tras reenvío y reinicio controlado | Ledger events, checkpoint y `foreachBatch` | Repetir mismo run/rango sin aumento de `total` y reinicio no pierde registros |
| RF11 | Presentar datos de replay progresivamente, con estado honesto | API `/snapshot`, WS `/stream`, React | Resultados 0→parcial→N; fallo WS→polling; no inventar valores |
| RF12 | Separar análisis histórico de laboratorio | React `App.tsx`, `HistoricalPage.tsx` | Exactamente dos áreas principales; sin `run_id` ni WebSocket visible en histórico |

## Analítica I3 con definiciones formales

Referirse a **`docs/i3_requerimientos.md`** para definición, filtros y denominadores completos: R01 conexiones por streamer; R02 usuarios únicos por streamer; R03 usuarios con ≥2 conexiones por streamer (no días de retención); R04 5 intervalos de duración; R05 inicios por paso relativo (no fecha); R06 concentración Top 10/100 sobre población global; R07 streams distintos por streamer; R08 reconciliación exacta. Tasa `recurrence_rate=recurring_users/unique_users` por streamer; `unique_users` **no aditivo** entre streamers. No promediar tasas.

## No funcionales (contratos comprobables, no promesas)

| ID | Característica | Criterio de aceptación / prueba |
| --- | --- | --- |
| RNF01 | Reproducibilidad | Rama, commit, versiones, fuente SHA256, release y run_id registrables |
| RNF02 | Integridad | Bronze/Silver/Gold/I3 conciliados; streaming ledger único; errores visibles |
| RNF03 | Seguridad local | Solo puertos `127.0.0.1:5173`, `127.0.0.1:8501`; FastAPI sin exposición host; volumen I4 `:ro` en API |
| RNF04 | Protección de datos | Sin registros crudos/credenciales en React ni Git; exportación I3 exclusivamente agregada |
| RNF05 | Recuperación controlada | Reinicio Spark conserva identificadores con estado Kafka, SQLite, checkpoint intactos |
| RNF06 | Observabilidad | Estado Kafka/Compose, `run_id`, heartbeat, offsets globales, lote y métricas seleccionadas |
| RNF07 | Accesibilidad básica | Navegación por teclado, móvil sin desbordamiento, estados offline/empty visibles |
| RNF08 | Coste y escala honestos | Free Edition y Docker local, sin SLA ni afirmación de 124M procesados |

## Restricciones y exclusiones

- El conjunto de 100 mil usuarios y ~3 millones de filas se carga **manualmente**; no descargamos automáticamente datos del navegador ni procesamos `full_a.csv.gz`.
- Kafka utiliza broker local único y Compose; no se comunica con Databricks en producción.
- El dashboard AI/BI Databricks es producto separado, elaborado según reporte del equipo; se requieren capturas de la cuenta correspondiente para certificar su revisión.
- No existe sensor en vivo de Twitch ni sistema de autenticación/operación pública o despliegue SaaS.
- Los rechazos cero documentados no significan calidad universal, solo conformidad con reglas implementadas.

## Trazabilidad de pruebas

RF01–06: `notebooks/01,03,04,05,07,08,09`, `sql/i3/*.sql`, auditoría SQL de este dossier. RF07/12: `10_i3_export_snapshot.py`, `tests/test_historical.py`, `web/src/test/App.test.tsx`. RF08–11: `live_monitor/README.md`, `live_monitor/e2e.py`, `audience_experience/web/e2e/`, `scripts/validar_demo.ps1`. RNF03: `docker compose ... ps`, `docker inspect` del contenedor API y `compose.yaml`.
