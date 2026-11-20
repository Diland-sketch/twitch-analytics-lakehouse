# Twitch Live Monitor — I4

Demostración local **de reproducción histórica**, no actividad actual de Twitch.
Cada métrica del dashboard se lee del sink de Spark. El reloj solo actualiza
la lectura/estado; no genera eventos ni incrementa cifras.

Auditoría previa: [AUDIT.md](AUDIT.md). Arquitectura y garantías:
[ARCHITECTURE.md](ARCHITECTURE.md). Resultados y límites: [VALIDATION.md](VALIDATION.md).
Rama I4 basada en `feat/i2-kafka-integration`; no presupone merge de I2 ni cambia
I1/I3. El despliegue Kafka I2 permanece independiente.

## Requisitos y arranque

Docker Desktop con motor Linux activo y Compose. CSV `100k_a.csv` ya disponible
localmente; no descargar histórico completo. Memoria suficiente para Spark
local[2], Kafka y Streamlit (el uso real depende del equipo). Internet solo para
descargar imágenes, paquetes Python y conector durante build.

Desde la raíz del repositorio, PowerShell:

```powershell
$env:TWITCH_CSV = 'C:/Users/TU_USUARIO/Downloads/100k_a.csv'
docker compose -f live_monitor/compose.yaml config --quiet
docker compose -f live_monitor/compose.yaml build spark
docker compose -f live_monitor/compose.yaml up -d kafka spark dashboard
docker compose -f live_monitor/compose.yaml logs -f spark
```

Abrir [dashboard local](http://localhost:8501). Esperar estado RUNNING.
Kafka no publica puertos al host. Solo Streamlit se vincula a 127.0.0.1:8501;
no hay Kafka público ni Spark UI expuesto. No se usa Databricks ni servicios de
pago. Spark 3.5.8/Scala 2.12/Java 17 están en la imagen, no depende del Java host.
Conector exacto `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`.

## Reproducción controlada inicial: 100 eventos

En otra terminal, sin detener el consumidor:

```powershell
$runId = [guid]::NewGuid().ToString()
$runId
docker compose -f live_monitor/compose.yaml run --rm --no-deps producer python3 -m live_monitor.producer --csv /input/100k_a.csv --run-id $runId --count 100 --rate 5
docker compose -f live_monitor/compose.yaml run --rm --no-deps tools python3 -m live_monitor.control status --run-id $runId
```

`rate` es velocidad objetivo de publicación, no garantía de latencia de dashboard.
`--start-row` inicia un rango de filas (1 por defecto); count se limita a 10.000
para evitar replay masivo accidental. La aceptación inicial exige 100. Se lee
el archivo completo para calcular SHA256, pero solo se envía el rango solicitado.
Se conservan las líneas originales, incluso si resultan inválidas; Spark las
clasifica. No se presentan fixtures como datos reales.

El monitor permite elegir run_id, ver válidos/invalidos, Top 20 streamers válidos,
evolución por finalización de micro-lotes, último micro-lote, heartbeat y offsets.
La última actualización de datos es la del lote completado; «lectura del monitor»
solo indica consulta del UI. El selector de ejecución afecta métricas, streamers
y evolución; último lote, heartbeat y offsets se etiquetan como globales.

## Parada segura y reintentos

En otra terminal, usando el UUID real (no el literal UUID):

```powershell
docker compose -f live_monitor/compose.yaml run --rm --no-deps tools python3 -m live_monitor.control stop --run-id $runId
```

Finaliza el envío en curso, confirma lo enviado y conserva el progreso. Ctrl+C
en productor también solicita parada. No apaga Spark: permite drenar Kafka.
Antes de reanudar un run detenido retirar solo la señal de control:

```powershell
docker compose -f live_monitor/compose.yaml run --rm --no-deps tools python3 -m live_monitor.control resume --run-id $runId
```

Después repetir el comando productor con **el mismo run_id, archivo y rango**.
Se reenvía desde el inicio del rango, con identidades estables: Kafka recibe
mensajes adicionales, pero los eventos únicos no aumentan. Un run_id nuevo es
otra reproducción explícita y sí incrementa el total. El manifiesto impide cambiar
fuente/rango para un run_id. Confirmaciones del productor muestran el último
intento; no equivalen por sí solas a procesamiento Spark.

Reiniciar consumidor conservando DB/checkpoint:

```powershell
docker compose -f live_monitor/compose.yaml restart spark
```

Apagar solo servicios I4, conservando volúmenes:

```powershell
docker compose -f live_monitor/compose.yaml stop producer spark dashboard kafka
```

No usar `down -v`, limpiar volúmenes, borrar checkpoint ni reutilizar un broker
recreado sobre una DB vieja. No escalar spark o producer: los locks impiden dos
procesos por rol. Restaurar broker, DB y checkpoint como conjunto ante pérdida.

## Pruebas reproducibles

Sin Docker/Spark: `python -m unittest discover -s live_monitor/tests -v` (el caso
Spark se omite explícitamente). Suite completa en imagen fijada:

```powershell
docker compose -f live_monitor/compose.yaml run --rm --no-deps -e I4_SPARK_TESTS=1 tools python3 -m unittest discover -s live_monitor/tests -v
```

Prueba E2E real, desde raíz, con Python estándar local:

```powershell
python -m live_monitor.e2e --csv $env:TWITCH_CSV --count 100 --rate 5 --out work/i4-evidence
```

Arranca servicios, verifica UI con AppTest sobre SQLite real, publica 100 filas,
observa un estado intermedio, concilia acks/identidades/conteos/minutos, reinicia
Spark, reenvía mismo run_id y exige 100 únicos aunque crezcan offsets. No usa
consumidor Python de eventos. Conservar además capturas del navegador antes y
después: AppTest comprueba render del código, no sustituye revisión visual.
Si falla, report.json registra FAILED y los servicios/datos se conservan para
diagnóstico; no se marcan tramos no ejecutados como exitosos.

## Evidencia local (no subir CSV, DB ni logs originales a Git)

`work/i4-evidence/<run_id>/`: report.json, snapshots.jsonl, producer.log,
commands.log (comandos y logs de servicios). El volumen monitor_state guarda
monitor.sqlite, checkpoint, spark_progress.jsonl y producer-<run_id>.jsonl.
Conservar capturas y hashes localmente. Para copiar evidencia al host, sin
copiar DB activa como backup incoherente, usar exportación SQLite o parar Spark
y productor primero. No cambiar el estado para «hacer pasar» una conciliación.

Si el motor no está activo, no arranca la imagen, falta Maven/conector, falla
Spark o el UI no muestra incremento, registrar el bloqueo y no afirmar demo
exitosa. No hay objetivo de latencia de un segundo: trigger y refresco a 2s son
configuración, no medición ni SLA.
