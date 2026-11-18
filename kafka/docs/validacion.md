# Validación I2: Kafka local en Compose

## Evidencia revisada

El asistente leyó los archivos reales generados por Anghela en PowerShell.
No pudo acceder al Docker del equipo desde su sesión; las pruebas operativas
fueron ejecutadas por Anghela, no simuladas por el asistente.

| Instancia / lote | Confirmados | Recibidos | Partición | Offsets |
|---|---:|---:|---:|---|
| Contenedor anterior, cfbbc4e0-a9f2-4a40-a6d3-8df1a11591d3 | 10 | 10 | 0 | 0–9 |
| Compose, 05c02821-199f-4ae9-90cb-295955f6eb3d | 10 | 10 | 0 | 0–9 |
| Compose, lote nuevo 18d8eb41-5659-49be-acd4-dd2a9c794d65 | 10 | 10 | 0 | 10–19 |

El primer intento de consumo en Compose usó el texto literal RUN_ID y obtuvo
cero coincidencias. Se conservó consumidor_compose.txt; el UUID real permitió
recuperar la lectura en consumidor_compose_recuperado.txt.

consumo_tras_reinicio.txt contiene las diez filas del lote Compose inicial,
con los mismos offsets, tras el paso de reinicio indicado y reportado por
Anghela. No se adjuntó la salida original del comando restart ni timestamps
del contenedor antes/después: la recepción fue leída del archivo, la ejecución
del reinicio depende del procedimiento reportado. No afirmar recreación del
contenedor ni recuperación ante pérdida del volumen.

## Comprobaciones de la copia preparada

- Análisis AST de ambos scripts: correcto.
- Scripts preservados respecto a los originales (texto UTF-8 normalizado).
- pip check del entorno original: sin dependencias incompatibles.
- Compose original: config --quiet exitoso; este cambio copia su configuración.
- Se compararon automáticamente los diez JSON del lote nuevo con las primeras
  diez filas del CSV local: cinco campos coinciden en cada fila.
- UUID común, diez eventos y offsets exactamente 10 a 19 en la salida revisada.
- Las diez filas pertenecen al usuario 1: prueba de transporte, no muestreo.

No se ejecutaron módulos batch, pruebas de escritura Databricks ni tablas.
No se suben logs originales ni CSV al repositorio: se conservan localmente.
Estos hashes identifican las evidencias originales; no sustituyen repetir
los comandos del README en el entorno receptor.

| Evidencia local | SHA-256 |
|---|---|
| productor_compose.txt | d8209389ae30278d691cd8a21ae526e5d30609a6a1c0b69fc99487822f22f1d6 |
| consumidor_compose.txt | 705465c17f82642082f09526a2dedb09085f11d5b1bda813730f2063a6255519 |
| consumidor_compose_recuperado.txt | eb5eca56b575d8dd7f4e27bda1ca032f2200c5afa6a4c4766eeb9faa5c8458b0 |
| consumo_tras_reinicio.txt | eb5eca56b575d8dd7f4e27bda1ca032f2200c5afa6a4c4766eeb9faa5c8458b0 |
| productor_lote_nuevo.txt | fb4118d634cbb9db52155f30782d8f2538aa5dceba81c17a9224b02c0e89413a |
| consumidor_lote_nuevo.txt | ecbd231d8545acd2d14eb6bf8d8c9d032c6814d004040550f9238c5d06ccbe9b |

## Pendiente

Prueba real Spark Structured Streaming y cualquier transferencia a Databricks.
Esta entrega demuestra CSV → Python → Kafka → Python local con evidencia de
Compose, no una conexión Kafka → Spark ni streaming cloud.
