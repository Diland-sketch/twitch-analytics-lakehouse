# I4: auditoría previa y propuesta mínima viable

Auditoría GitHub: PR #1 abierto, no integrado; Kafka vive en
`feat/i2-kafka-integration`, commit `37d85f0`. `main` solo tiene README.
I3/PR #2 sigue en borrador (`11dbc81`), separado: no se cambia.
I4 parte de I2 en `feat/i4-twitch-live-monitor`, worktree independiente.

## Hallazgos reales

- I2: apache/kafka:3.9.1 en KRaft, volumen persistente y puerto loopback 9092.
  Productor Python publica 10 filas; consumidor Python, sin Spark, filtra run_id.
- Evidencia I2 reportada por Anghela: 10 mensajes confirmados/recibidos; no prueba
  Kafka→Spark. Reinicio de broker documentado con límites de evidencia.
- UUID nuevo en cada ejecución, sin event_id estable: reenvíos no son idempotentes.
  Parámetros fijos, sin control de calidad JSON, checkpoint ni dashboard.
- CSV real encontrado localmente: Downloads/100k_a.csv, 114.131.306 bytes.
  No hace falta descargarlo ni copiarlo al repositorio.
- Docker CLI/Compose instalados; al inicio motor Linux no accesible (pipe ausente).
  Java host 22 y Python 3.13 no serán la plataforma objetivo de Spark I4.

## MVP decidido antes de implementación

Compose I4 aislado: broker sin puertos publicados, Spark local[2] dentro de Linux
con Java 17 / Scala 2.12 / Spark 3.5.8 y conector Kafka **3.5.8**, productor
controlado, SQLite en volumen Linux y Streamlit solo en 127.0.0.1:8501.
I2 y tablas Databricks permanecen intactos; topic I4 distinto.

1. Cada ejecución elige run_id estable, huella SHA256 de archivo y rango de filas.
   event_id = SHA256(run_id + huella + ordinal). Reenviar mismo run_id/rango
   conserva identidad; otra ejecución explícita cuenta como otra reproducción.
2. Spark readStream Kafka con JSON explícito y reglas equivalentes a Silver.
   Tiempo histórico relativo separado de tiempo real de recepción/procesamiento.
3. foreachBatch escribe ledger de offsets, eventos deduplicados, agregados Spark
   y micro-lote en una transacción SQLite. PK de evento y transporte más ledger
   de micro-lote; un reintento tras commit no incrementa métricas.
4. Checkpoint persistente con identidad de pipeline; solo un consumidor, bloqueo
   exclusivo de proceso. SQLite transaccional; dashboard solo lectura en snapshot.
5. Heartbeat y estado se publican aparte de conteos. Sin consumidor/no heartbeat
   reciente mostrar estado desactualizado, nunca fingir procesamiento.
6. Streamlit relee SQLite periódicamente; ningún contador aleatorio ni incremento
   basado en reloj. Control externo productor configurable y señal de parada.
7. Prueba E2E: 100 filas reales, acks, progreso Spark y SQL concilian, reinicio
   con checkpoint, reenvío mismo lote sin inflación y evidencia UI antes/después.

## Riesgos / puertas de aceptación

Dependencias: motor Docker Linux, imágenes/Maven disponibles durante build y
recursos locales. No exponer Kafka a internet ni desplegar cloud. SQLite es sink
de demostración, no solución para 124M: micro-lotes acotados, recogida al driver.
Conservar juntos broker/DB/checkpoint: no reutilizar offsets de un broker borrado.
No borrar volúmenes para resolver fallos. Un sink foreachBatch no es exactamente
una vez por sí solo: la transacción y claves únicas aportan idempotencia.
La prueba completa queda bloqueada si no arranca Docker, faltan dependencias o
no puede verificarse UI; documentar cada tramo por separado sin declarar éxito.

Fuentes: [Kafka/Spark 3.5.8](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html),
[foreachBatch/checkpoints](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html),
[fragmentos Streamlit](https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment).
