# Validación I4: evidencia real y límites

## Resultado observado

**Demostración E2E PASS**, ejecutada en este equipo con Docker Linux el
20 de noviembre de 2026. Intervalo técnico UTC del test:
18:41:20–18:44:04 (13:41:20–13:44:04 Bogotá). No representa fechas del dataset.

| Criterio | Evidencia real |
| --- | --- |
| Lote identificado | run_id `44ffc3a5-77b3-4880-89e2-6dd1eb1520a7` |
| Fuente | primeras 100 líneas del CSV local 100k_a.csv, 114.131.306 bytes |
| Kafka confirmó primer envío | 100 identidades esperadas, partición 0, offsets 0–99 |
| Consumidor efectivo | Spark Structured Streaming readStream Kafka; progreso y logs conservados |
| Eventos únicos persistidos | 100 |
| Válidos / inválidos | 100 / 0 según reglas I4/Silver |
| Agregados por streamer | suma completa de interacciones=100, minutos estimados=2.070; conciliados con CSV |
| Actualización progresiva | snapshots 0, 4, 8, 16, 28, 38, 46, 55, 63, 72, 88, 95, 100 |
| Streamlit sobre SQLite real | AppTest: métricas 0→100; captura de navegador confirma 100/100/0 y RUNNING |
| Reinicio real del consumidor | docker compose restart spark; mismo pipeline_id y checkpoint; 100 únicos |
| Reenvío del mismo run/rango | otros 100 mensajes, offsets 100–199; 200 offsets registrados, 100 únicos |
| Último micro-lote del test | batch_id 33, offset 199; new_events=0; sin inflación por reenvío |

pipeline_id observado: `1c19758e-8d3e-4b38-94a2-310fe9ab00ab`, idéntico antes y
después de reiniciar. Fuente SHA256:
`11d3bdb49683f651eb6b0ba806bf317db68646efc0c7de2d02d750201fd8ef69`.

## Plataforma verificada

Docker Engine 29.7.2 / Compose v5.4.0. Contenedor: Spark 3.5.8, Java 17.0.17,
Scala 2.12.18 y Python 3.10. Conector resuelto en build:
`spark-sql-kafka-0-10_2.12:3.5.8` (cliente Kafka JVM transitivo 3.4.1).
Broker apache/kafka:3.9.1; kafka-python 3.0.13; Streamlit 1.50.0.
Imagen local del test:
`sha256:3edc1815cf6e63c4597e877ba65d1e9d0b6607b63f087230d0ecf91a4532a6e6`.

Docker no estaba iniciado al auditar; se inició el motor instalado. Se corrigió
PYTHONPATH para herramientas Python de la imagen oficial antes de ejecutar la
suite. No se modificaron instalaciones Java/Spark del host ni tablas Databricks.

## Pruebas automatizadas

- Imagen fijada: **11 pruebas aprobadas, cero omisiones**, 14,270 s. Incluye
  transformación JSON/Silver en Spark real y foreachBatch con SQLite temporal;
  negativos artificiales, duración cero, BIGINT overflow, payload malformado,
  reintento de micro-lote y reenvío de identidades con offsets nuevos.
- Host sin PySpark: 10 pruebas I4 aprobadas, 1 omitida explícitamente; regresión
  heredada I1: 15 aprobadas, 1 omitida por falta de PySpark en Python host.
- E2E real adicional: `python -m live_monitor.e2e --csv <ruta real> --count 100
  --rate 5`: PASS. No reemplaza Kafka/Spark por dobles.
- compileall y git diff --check correctos. Advertencias de Hadoop nativo y
  sockets de recogida PySpark no impidieron finalizar suite ni demostración.

## Evidencia conservada localmente

Raíz relativa al checkout I4:
`work/i4-evidence/44ffc3a5-77b3-4880-89e2-6dd1eb1520a7/`.
Ignorada por Git, sin subir CSV, DB, logs ni captura originales al PR.

| Archivo | SHA256 |
| --- | --- |
| report.json | 47712660c6fda16df1c246b7cd56e8098c876901f23a606e242c0c5ef97f8c36 |
| snapshots.jsonl | 000b53a935afac8efc374c0d8d591726bb6f6c94132cb430eca154cc6f62019e |
| producer.log | 0324382de41dc5cf67ba377fa9fe06b089de6f4366a9b36298101f981098bcd8 |
| producer_acknowledgements.jsonl | e941df009e0b7955ed108f03db0592876a2dd9ca6d6c2b0f8c3e6b65b856010a |
| spark_progress.jsonl | 3c9e3657410b7c87fa04e8f234825ef10f3f7b27fa867a4c2155d5d9e6590e38 |
| commands.log | 4101e30ec63d63db73734c2600c5d2ed6ec9a037dd91891e84bc275389dba2b3 |
| dashboard-after-restart.jpg | c3a078ff480a041be65b8bc8aff983878e9fc35598576281269c622a7edbf67f |

El volumen mantiene la DB y checkpoint originales; los ficheros de progreso/acks
son copias puntuales. Los hashes permiten identificar evidencia, no sustituyen
repetir la prueba en otro equipo. La captura es del navegador real; AppTest
conserva el antes/después de valores y los snapshots prueban progresión.

## Límites explícitos

No se midió SLA de latencia de un segundo ni rendimiento a gran volumen.
Duración de la prueba incluye arranque, UI, dos publicaciones y reinicio; no es
latencia evento→pantalla. El prototipo monobroker/SQLite no garantiza recuperación
ante pérdida conjunta de disco ni está dimensionado para 124M. No se probó caída
abrupta del host, pérdida del broker o cambio de particionado; las transacciones
se probaron con rollback y lectura concurrente. Controles de parada están
implementados y documentados; su prueba operativa separada no forma parte del
PASS E2E registrado aquí. No hay actividad actual de Twitch ni atención probada.
I3/PR #2 e I1 permanecen sin cambios; no hubo merge automático.
