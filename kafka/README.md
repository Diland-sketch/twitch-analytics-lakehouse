# Kafka local — integración I2 de Anghela

Implementación aislada del pipeline batch de Diland. Publica las primeras diez
filas reales de `100k_a.csv` como JSON y comprueba su recepción por `run_id`.
Es reproducción histórica, no datos nuevos de Twitch. Las diez filas corresponden
al usuario 1; no son una muestra estadística del dataset.

## Estado verificado

- Kafka 3.9.1 KRaft en Docker Compose; un broker y una partición.
- Lote nuevo `18d8eb41-5659-49be-acd4-dd2a9c794d65`: diez confirmaciones,
  partición 0, offsets 10–19; consumidor recibió las diez filas distintas.
- Lote anterior de Compose `05c02821-199f-4ae9-90cb-295955f6eb3d`:
  diez filas, offsets 0–9; nueva lectura después del paso de reinicio reportado.
- [Evidencias y límites](docs/validacion.md).
- Kafka → Spark y Kafka → Databricks **todavía no están demostrados**.

## Preparación desde un clon del repositorio

En PowerShell, desde la raíz del repositorio:

```powershell
python -m venv kafka/.venv
./kafka/.venv/Scripts/python.exe -m pip install -r kafka/requirements.txt
```

Descargar [100k_a.csv](https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/100k_a.csv)
y guardarlo en `Downloads` del usuario. No subir el CSV, entornos ni logs a Git.
Python probado por Anghela: 3.14.7. No hace falta activar el entorno.

## Utilizar el despliegue actual de Anghela

Si el Kafka de `C:\Users\anghe\twitch-analytics-lakehouse` sigue activo, ejecutar
solo los scripts Python contra `127.0.0.1:9092`. No iniciar un segundo broker
en ese puerto ni cambiar el proyecto Compose. Se pueden operar sus servicios
desde su carpeta original; no hay que copiar ni borrar sus volúmenes.

## Despliegue nuevo e independiente

Para otro equipo, con el puerto 9092 libre, desde la raíz del repositorio:

```powershell
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml config --quiet
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml run --rm --no-deps --user 0:0 --entrypoint /bin/bash kafka -c "chown -R 1000:1000 /tmp/kraft-combined-logs"
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml up -d
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml ps -a
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml logs --tail 20 kafka
```

El paso de propiedad corresponde a la incidencia real del volumen con usuario
1000 de esta imagen. Mantener el mismo `-p` en todas las operaciones: cambiarlo
selecciona otro volumen. `twitch-kafka-i2` crea un despliegue nuevo, no recupera
mensajes de los contenedores de Anghela. No usar estos comandos sobre dos
despliegues simultáneos en el mismo puerto.

Esperar `Kafka Server started`, después:

```powershell
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic twitch.interactions.raw --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic twitch.interactions.raw --bootstrap-server 127.0.0.1:9092
```

No exponer Kafka a otras interfaces. PLAINTEXT sin autenticación es solo para
esta demostración local. `127.0.0.1` no permite acceso desde Databricks ni desde
otro contenedor con espacio de red propio.

## Publicar exactamente un lote y consumirlo

```powershell
New-Item -ItemType Directory -Force kafka/evidencias
./kafka/.venv/Scripts/python.exe kafka/productor.py 2>&1 | Tee-Object -FilePath kafka/evidencias/productor.txt
```

Revisar diez confirmaciones. Extraer el identificador real, sin escribir `RUN_ID`:

```powershell
$texto = Get-Content kafka/evidencias/productor.txt -Raw
$runId = [regex]::Match($texto, 'Ejecucion:\s+([0-9a-f-]{36})').Groups[1].Value
if (-not $runId) { throw 'El productor no registro un run_id; revisar errores.' }
./kafka/.venv/Scripts/python.exe kafka/consumidor.py $runId 2>&1 | Tee-Object -FilePath kafka/evidencias/consumidor.txt
```

Exigir las diez filas y ausencia de traceback. En Windows PowerShell,
`Tee-Object` no convierte un fallo del proceso en un éxito: revisar la salida
y `$LASTEXITCODE` justo después de cada proceso Python.

Cada ejecución del productor publica diez mensajes adicionales; no garantiza
deduplicación entre ejecuciones. No publicar el dataset completo.

## Persistencia y parada

Después de un lote validado, reiniciar solo el servicio, esperar el arranque y
repetir el consumidor con **el mismo run_id**, sin volver a publicar:

```powershell
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml restart kafka
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml logs --tail 20 kafka
./kafka/.venv/Scripts/python.exe kafka/consumidor.py $runId
```

Esto comprueba supervivencia al reinicio, no recuperación ante pérdida del disco.
No borrar volúmenes ni checkpoints. Para detener:

```powershell
docker compose -p twitch-kafka-i2 -f kafka/compose.yaml stop
```

## Contrato y límites

Los campos de negocio coinciden con [I1](../docs/contrato_datos.md).
`source_file`, `source_row`, `run_id`, `historical_replay` y `produced_at`
son metadatos añadidos. `produced_at` es hora de publicación UTC, no tiempo
histórico. Los cuatro campos numéricos deben leerse como LongType en Spark.

Se preservan los scripts que funcionaron, sin refactorizar durante la integración.
Broker, tópico, ruta y diez filas están fijos; no hay parámetros de entorno
configurables ni se necesita `.env`. El consumidor valida run_id y filas, pero
no comparación completa automática, ni todos los duplicados posteriores, ni
JSON inválido. Falta event_id estable y política de reenvíos. `acks=all` sobre
una réplica no demuestra exactamente una vez ni alta disponibilidad.

No ejecutar estos scripts como notebook Databricks. No escriben ni modifican
`workspace.twitch_lakehouse`. La prueba real de Structured Streaming será un
incremento aislado después de esta incorporación; no un cambio al consumidor
Python presentado como Spark.
