# Twitch Analytics Lakehouse — dossier de entrega y reproducibilidad

**Rama de referencia:** `desarollo` (ortografía literal). **Ámbito:** archivo `100k_a.csv`, I1–I5; no histórico completo `full_a.csv.gz`.

## Empezar aquí

| Documento | Para qué sirve | Verificación principal |
| --- | --- | --- |
| [01_requerimientos.md](01_requerimientos.md) | Alcance, contratos, RF/RNF, criterios y trazabilidad | Casos observables RF01–RF12 |
| [02_arquitectura.md](02_arquitectura.md) | Arquitectura física y lógica, mermaid, fuentes y límites | Correspondencia con código y Compose |
| [03_modelo_datos.md](03_modelo_datos.md) | Granularidades, tablas Delta, SQLite, semántica | PK lógicas, cardinalidad, no aditividad |
| [04_procesamiento.md](04_procesamiento.md) | Instalación y ejecución reproducible Databricks + Docker | Scripts y orden de notebooks |
| [05_pruebas_evidencias.md](05_pruebas_evidencias.md) | Matriz PASS/FAIL y carpetas de evidencias | SQL, HTTP, reinicio, hashes |
| [06_manual_usuario.md](06_manual_usuario.md) | Interpretación de las dos pantallas | No confundir batch con replay |
| [07_manual_instalacion.md](07_manual_instalacion.md) | Requisitos, puesta en marcha, diagnóstico, recuperación | Health, estado, dependencias |
| [08_guion_sustentacion.md](08_guion_sustentacion.md) | Demostración repetible y defensa técnica | Observación 0→N y reconciliaciones |
| [sql/](sql/) | Sentencias de auditoría **de solo lectura**, ejecutar por separado | Comparar con valores esperados |
| [scripts/validar_demo.ps1](scripts/validar_demo.ps1) | Automatiza una reproducción local verificada, sin eliminar estado | Evidencias en `work/entrega_final/` |

## Qué está demostrado, qué no

1. **I1:** ejecución batch Bronze/Silver/Gold sobre `100k_a.csv` reportada por el equipo: 3.051.733 conexiones observadas, 162.625 streamers, 95.887.110 minutos **estimados**, 0 rechazados y 0 duplicados **bajo las reglas implementadas**. Reproducir y guardar pruebas en cada workspace.
2. **I3:** código de Gold avanzado y su preflight en `notebooks/07–09`; el usuario reportó su ejecución y la visualización de resultados agregados en React tras exportar con notebook 10. El dashboard Databricks existe según informe del equipo, pero sus capturas/configuración no están versionadas aquí.
3. **I4/I5:** un E2E de 100 filas quedó documentado en `live_monitor/VALIDATION.md` y `audience_experience/docs/VALIDATION.md`; adicionalmente el usuario reportó una prueba de **1.000** eventos únicos, todos válidos y conservados tras reinicio controlado de Spark. Esta segunda prueba es evidencia **declarada por usuario**; faltan logs/artefactos firmados de su equipo para auditarla independientemente.
4. **CI:** GitHub Actions comprueba PySpark con fixtures, contratos de I4, API y React; no sustituye Kafka/Databricks reales. No dar por aprobada una nueva ejecución antes de comprobar su resultado.
5. **No realizado/probado:** integración automática continua Databricks→React (la exportación es manual y estática), API de Twitch en vivo, failover multi-broker, pérdida de discos, histórico completo ~124M, SLA de latencia o operación a gran escala.

## Terminología obligatoria

**Conexión observada al chat** = fila de presencia observada por intervalos de diez minutos; **no** mensaje enviado, persona viendo video en ese momento ni espectador concurrente. **Minutos estimados** = `(time_stop-time_start)*10`, no tiempo verificado de visualización. **Recurrencia** = ≥2 filas Silver de mismo usuario/streamer, no retención entre sesiones. **Tiempo real de demostración** = llegada y procesamiento progresivo de un *replay* de CSV histórico; no datos actuales de la API Twitch.

## Reglas de operación y evidencia

- Conservar siempre `run_id`, hash SHA-256 del CSV, commit, timestamps UTC, versión release I3, resultados y capturas. Separar evidencia local de archivos públicos.
- No subir CSV, JSON real, SQLite, checkpoints, secretos, capturas con datos sensibles ni logs crudos a Git. Guardar en `work/entrega_final/<run_id>/` ignorado.
- Nunca ejecutar `docker compose down -v`, borrar volúmenes, reiniciar un broker recreado sobre checkpoint anterior ni sobrescribir tablas Delta. Reejecución batch: esquema aislado; publicación I3 nueva: release distinta.
- Los SQL de `sql/` son **consultas de auditoría, no migraciones**. Úsalos después de confirmar `namespace=workspace.twitch_lakehouse` y `release=r01`; si cambian, editar nombres de manera explícita.
- Ante FAIL, detener y guardar la evidencia tal cual, sin cambiar datos para obtener PASS.

## Accesos

- React: `http://localhost:5173` — **Análisis de audiencia** (Gold I3), **Laboratorio de datos** (Kafka/Spark).
- API HTTP: `/api/v1/health`, `/api/v1/historical`, `/api/v1/snapshot?run_id=UUID`; WebSocket `/api/v1/stream?run_id=UUID`.
- Streamlit respaldo: `http://localhost:8501`.
- Consulta histórica Gold: Git folder `desarollo` y SQL editor de Databricks Free Edition.

Fuentes versionadas: `docs/arquitectura.md`, `docs/calidad.md`, `docs/i3_requerimientos.md`, `docs/i3_modelo_gold.md`, `docs/i3_ejecucion.md`, `docs/integracion_gold_react.md`, `live_monitor/README.md`, `audience_experience/README.md`, archivos `twitch_lakehouse/`, `live_monitor/` y `audience_experience/`.
