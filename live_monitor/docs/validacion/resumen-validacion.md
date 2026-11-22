# Validación Kafka + Spark Streaming — Anghela

## Versión probada

- Rama probada: feat/i4-twitch-live-monitor.
- Commit: 5afba7ee9af8d51a858fc7cf7610e70e39a89afb.
- Entorno: Windows, Docker Desktop y contenedores Linux.
- Fuente: primeras 100 filas reales de 100k_a.csv.
- run_id: a8dc6f69-7f83-4efb-978b-e5263d16881c.

## Flujo comprobado

CSV histórico → productor Python → Kafka → Spark Structured Streaming → SQLite → dashboard Streamlit.

Los eventos son una reproducción histórica. No representan actividad actual de Twitch.

## Resultados

| Comprobación | Resultado |
|---|---|
| Construcción de imagen Spark | Exitosa, código 0 |
| Estado inicial del dashboard | 0 eventos |
| Primer envío confirmado por Kafka | 100 mensajes |
| Eventos únicos procesados por Spark | 100 |
| Registros válidos / inválidos | 100 / 0 |
| Minutos estimados acumulados | 2070 |
| Partición | 0 |
| Offsets del primer envío | 0–99 |
| Estado después de reiniciar Spark | 100 eventos; 2070 minutos |
| Reenvío del mismo lote | 200 offsets registrados; 100 eventos únicos |
| Último offset después del reenvío | 199 |
| Minutos después del reenvío | 2070 |
| Estado final del consumidor | RUNNING |

La deduplicación se comprobó para el mismo run_id, archivo y rango. No se afirma una garantía universal de procesamiento exactamente una vez.

## Evidencias conservadas

- Commit y estado de servicios.
- Confirmaciones del primer envío.
- Estado inicial después del procesamiento.
- Estado después del reinicio.
- Confirmaciones del reenvío.
- Estado después del reenvío.
- Capturas del dashboard antes y después.

Ubicación local de las evidencias: `work/evidencias-a/`.

Los archivos originales de evidencia se conservan localmente; esta documentación resume sus resultados.

## Límites

- Prueba de 100 filas; no demuestra rendimiento para 124 millones.
- No se midió una garantía de latencia.
- No se probó pérdida de disco ni alta disponibilidad.
- No se modificaron tablas de Databricks.
- Esta ejecución demuestra integración local, no conexión con Databricks.
