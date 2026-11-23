# Escalamiento real, aislado del PR #7

Rama `codex/twitch-scale-50m`, basada en 754f0dc. No modifica r01, kafka_b7_r001,
kafka_b7_r002, sus reportes, el tópico piloto ni sus checkpoints. Los hitos nuevos
se publican exclusivamente en `workspace.twitch_bulk_experimental`.

Estado actualizado: 100k, 1M y 10M tienen publicación remota CONFIRMED. Los 50M
están preparados y validados LOCALMENTE; transferencia y Job esperan autorización
explícita. Ver [plan de aprobación 50M](APPROVAL_50M.md) para bytes, 88 destinos,
reutilización certificada, límites no observables y recuperación.

## Diagnóstico verificable

Se recorrió COMPLETO el GZIP local: **474.676.930 filas físicas**, 6.943.679.726 bytes
comprimidos y 18.895.365.102 bytes expandidos. SHA256 comprimido
`2475bcbf0a8fdf1864c78bd62aee35384f9ae371788348fa0b8831c5d827e11e`, coincidente con
la fuente del piloto. El escaneo tardó 184,697 s usando bloques de 8 MiB; es inspección
local de bytes/líneas, **no procesamiento Databricks de 474M**. Ver
[perfil persistido](evidence/full_source_profile.json).

La página académica publica 124M «interactions»; esto no coincide con el conteo
físico del archivo actualmente servido. No se asume que ambas métricas tengan la
misma definición ni que todas las filas físicas sean válidas/distintas. Los hitos
deben conciliar con la fuente física y las reglas Silver, no completar una cifra de
la publicación. El índice público inspeccionado solo ofrece 100k_a.csv y full_a.csv.gz.
En Downloads se encontraron full_a.csv.gz y dos copias nominales de 100k_a.csv, sin
fragmentos históricos adicionales identificados. No se descargó otro histórico.

Código existente reutilizado: SHA streaming del preparador full_dataset_prepare,
classify/silver_frames/gold_by_streamer y write_new. Los experimentos 1M y 10M ya
tienen aislamiento, hashes por fragmento, lotes y agregación global. Su read.text
no persiste ordinal global dentro del archivo: la nueva entrada Parquet incorpora
ese ordinal explícito. No se han encontrado evidencias remotas 1M/10M en esos scripts.
Evidencias reales previas: r01/I3, piloto Kafka 1k y las releases kafka_b7_r001/r002;
la confirmación del Job 714370176386673 y r002 fue aportada por el usuario y la tabla
de publicación existe en el workspace. No se suman poblaciones que puedan solaparse.

## Cuotas y coste

Databricks Free Edition: solo serverless, egress restringido, cinco tasks concurrentes
y un pipeline activo por tipo. Agotar fair usage puede detener cómputo el resto del
día/mes. No se conoce una cuota numérica restante desde este entorno; por eso se
ejecuta un hito a la vez y se observa capacidad/estado antes del siguiente.

**Aiven real NO es Free tier:** la consola mostró `kafka-professional-1`, prueba
activa con $50 USD y 30 días, 487 KB ingress / 523 KB egress y tópico piloto único
con 1 partición, replicación 3, min ISR 1, retención 168 h, retention.bytes unlimited.
Los límites Free tier documentados NO se aplican automáticamente a ese servicio.
Capacidad física disponible y presupuesto efectivo restante deben revisarse antes
de publicar. No se cambia plan ni se habilitan cargos. Tras la indicación posterior
del usuario, **NO publicar Kafka todavía** hasta revisar origen, cuotas, tópico,
manifiesto, recuperación y rendimiento.

## Productor: diagnóstico y evolución

El archivo real `acked.json` del piloto registra 1.000 ACK en 103,5221837 s (~9,66/s).
El código hace send().get() serial por evento, fsync por ACK y sleep(1/rate). Esto
impide batching efectivo y suma latencia, disco y throttling. No se midió por separado
RTT/fsync ni se conoce el rate exacto de esa ejecución: no se atribuye todo a Kafka.

`bulk_lakehouse.kafka_async` prepara publicación asíncrona con máximo 256 futuros,
batch 64 KiB, linger 20 ms, gzip, buffer 8 MiB e idempotencia de productor; SQLite WAL
FULL guarda ACK por ordinal en grupos de 64. Reanuda el mismo load_id y omite ACK
durables. Tras caída, ACK no confirmados localmente pueden reenviarse: event_id estable
permite deduplicar fuente, pero NO promete exactly-once entre procesos. Un archivo
STOP o señal detiene nuevas publicaciones y drena la ventana pendiente.

El productor NO crea tópicos ni cambia particiones. Exige un review APPROVED con
fuente, 100k, tópico `twitch.bulk.*` distinto del piloto, particiones existentes,
créditos/almacenamiento y recuperación revisados, sin cambios de plan/cargos extra.
La primera capacidad estimada con las 100k filas reales es **51.112.589 bytes** de
value+key antes de compresión, réplica y overhead (~511,126 bytes/fila, máximo 528).
Es una estimación, no un envío. Ver JSON de capacidad. El límite conservador de
64 KiB/s implica ~780 s solo para ese volumen lógico; medir antes de prometer mejoras.

Inyección segura preparada en `scripts/run_bulk_kafka_secure.ps1`: Read-Host
-AsSecureString, variables de proceso temporales, contraseña fuera de argumentos,
restauración del entorno y liberación del BSTR en finally. No se pide contraseña al
chat, no se crea .env y no se imprimen opciones/errores de autenticación. Ejecutarlo
solo tras revisión explícita; actualmente no se lanzó. La memoria del proceso y
variables de entorno no equivalen a un almacén cifrado permanente.

## Ruta batch distribuida

Preparación secuencial bounded del GZIP (inevitable para un único gzip no splittable)
→ fragmentos Parquet/Snappy de 100k con row groups de hasta 10k → volumen UC → Job
Spark distribuido. Schema físico explícito: source_row LONG, raw_line STRING,
raw_sha256 STRING. Manifiesto conserva SHA del GZIP original, rangos contiguos,
bytes/SHA por Parquet y digest secuencial de sus filas. No usa zipWithIndex, RDD ni
monotonically_increasing_id para inventar procedencia. Se retienen errores parciales.

El Job separado `twitch-bulk-batch-milestones` tiene cinco tasks ALL_SUCCESS, cero
retries y Git fijado por commit: verify → bronze → silver → gold → publish.
Verify compara bytes/SHA de archivos remotos con binaryFile distribuido (solo recoge
metadatos, no eventos). El límite max_input_bytes es explícito y bloquea etapas que
excedan la capacidad revisada. Hitos posteriores requieren publicación CONFIRMED
del anterior con la misma fuente: 100k → 1M → 10M → 50M.

Bronze usa checkpoints lógicos inmutables por rango: 1..100k, 100001..1M y después
bloques de 1M, hasta 51 bloques para 50M. Un hito nuevo reutiliza prefijos certificados
sin anexar duplicados. Receipt incluye rangos, partes, conteos, hashes de filas,
versión y tamaño Delta. Si hay tabla sin receipt por caída, solo se certifica si
coincide exactamente con el plan; un contenido parcial/distinto se conserva y falla.

Silver aplica reglas I1 y deduplica KEY globalmente sobre la unión de rangos
disjuntos certificados; rechazos conservan ordinal y payload. Gold hace distinct
globales sobre Silver, nunca suma distinct de bloques. Tablas de hito nuevas,
sin partitionBy por user/streamer (alta cardinalidad). Bronze escribe 2 particiones
para 100k y 4 por bloque mayor; se medirán archivos/tamaños antes de ajustar.

Publish confirma balances, KEY únicas, rechazos persistidos, sumas y Gold exacta
mediante exceptAll bidireccional. Guarda métricas y DESCRIBE DETAIL de Bronze,
Silver, rechazos y Gold. Un fallo no se presenta como CONFIRMED; los resultados
parciales y el Job permanecen visibles. No hay transacción multi-tabla.

## Ejecución reproducible

```powershell
python -m bulk_lakehouse.source inspect --source C:\ruta\full_a.csv.gz --output work\profile.json
python -m bulk_lakehouse.source prepare --source C:\ruta\full_a.csv.gz --profile work\profile.json --rows 100000 --output work\parts-100k
```

No repetir sobre destinos existentes. El perfil es reutilizable solo si bytes,
mtime y SHA completo de la fuente concuerdan; la fragmentación vuelve a verificar
ese SHA sin leer el archivo entero en RAM. El primer fragmento real ocupa 9.949.256
bytes y cubre exactamente 1..100000; preparación no demuestra ingesta remota.

En Databricks, crear únicamente esquema/volumen experimental nuevos, subir manifest
y Parquet del hito a `/Volumes/workspace/twitch_bulk_experimental/source_parts/m100k/`.
Crear Job desde `bulk_lakehouse.job.settings(SHA_COMMIT)` mediante SDK/Jobs API.
Run parameters: rows=100000, release=bulk_100k_r001, manifest_path=<ruta>/manifest.json,
predecessor vacío y max_input_bytes=134217728. No instalar Spark ni Maven.

Tras PASS y revisión de capacidad: preparar/subir 1M a m1m, release bulk_1m_r001 y
predecessor `workspace.twitch_bulk_experimental.publication_bulk_100k_r001`.
10M/50M exigen sus predecesores y un límite de bytes revisado; no se disparan por
un loop automático. Cada manifest describe una población prefijo, no eventos extra
que puedan sumarse a r01 o a los otros hitos.

```sql
SELECT report_json FROM workspace.twitch_bulk_experimental.publication_bulk_100k_r001;
SELECT report_json FROM workspace.twitch_bulk_experimental.silver_audit_bulk_100k_r001;
SELECT * FROM workspace.twitch_bulk_experimental.gold_bulk_100k_r001
ORDER BY interactions DESC, streamer_username;
```

Pruebas artificiales: `python -m unittest tests.test_bulk tests.test_bulk_kafka -v`
y `tests.test_bulk_spark` con Spark real y storage fake versionado. No completar
cifras de los hitos con fixtures. Duraciones por task y fallos se conservan en Jobs;
las evidencias remotas de cada hito deben añadirse solo al observar su resultado.

Fuentes: [dataset y métricas publicadas](https://mcauleylab.ucsd.edu/public_datasets/data/datasets.html),
[índice de archivos](https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/),
[GzipCodec](https://hadoop.apache.org/docs/current3/api/org/apache/hadoop/io/compress/GzipCodec.html),
[Parquet/Snappy](https://arrow.apache.org/docs/python/parquet/parquet.html),
[Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations),
[Aiven Free tier (no confundir con el plan observado)](https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier).

## Primera ejecución remota y corrección

Job batch creado: `685945819660775`. Primera ejecución: `148404169252947`.
La transferencia de `manifest.json` (760 bytes) y Parquet (9.949.256 bytes)
fue confirmada remotamente. La tarea verify falló, bloqueando todas las demás:
`text()` aplicaba su argumento predeterminado wholetext=False después de las
opciones del reader. El manifiesto formateado se dividía en 22 registros.
La corrección pasa `wholetext=True` directamente a `text()` y el fixture Spark
utiliza JSON multilínea como el real. Se conserva el fallo en evidence/.

Serverless realizó además un reintento de auto-optimización pese a max_retries=0.
Los nuevos settings incluyen `disable_auto_optimization=true` y
`retry_on_timeout=false` en cada tarea. Verificar el valor remoto después de
actualizar el Job. No cambiar los Jobs previos del piloto Kafka.
Referencia: [reintentos Serverless](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs).

La ejecución corregida `37327879921671` terminó SUCCESS en 117.849 ms,
con las cinco tareas exitosas y publicación `bulk_100k_r001` CONFIRMED.
Siete controles PASS; 100.000 filas Silver válidas, cero rechazos y duplicados
de negocio; 17.702 streamers y 3.182.310 minutos estimados. Bronze Delta ocupa
7.895.148 bytes en dos archivos; Silver 1.287.872 y Gold 154.841 bytes.
Estos tamaños excluyen logs Delta y almacenamiento de artefactos auxiliares.
La ruta es SPARK_BATCH_NOT_KAFKA y la población no debe sumarse al histórico r01.
La evidencia persistida se copia en `evidence/batch_100k_publication.json`.

El prefijo de 1M se preparó con 10 fragmentos/99.152.273 bytes, dentro del límite
de entrada de 128 MiB. Su primer fragmento tiene el mismo SHA que el hito 100k.
El manifiesto local es preparación de fuente, no prueba de procesamiento remoto.

El Job de 1M `697148119262366` terminó SUCCESS en 112.889 ms y publicó
`bulk_1m_r001` CONFIRMED con siete controles PASS: 1.000.000 válidas/Silver,
cero rechazos/duplicados de negocio, 78.197 streamers y 30.805.650 minutos
estimados. Bronze ocupa 79.477.380 bytes, Silver 12.176.166 y Gold 706.225.
El total Bronze incluye el bloque 100k reutilizado; no representa otro millón
adicional de filas transportadas por Kafka. Evidencia: `batch_1m_publication.json`.

Preparación de 10M: 100 fragmentos, 992.487.125 bytes, 76,436 s de extracción
(excluye rehash previo de fuente); manifest 33.518 bytes. Límite de entrada
revisado: 1.342.177.280 bytes. El archivo de capacidad documenta las condiciones;
preparar/subir no acredita ejecución remota. La cuota fair-use restante sigue sin
ser cuantificable y un fallo impide avanzar a 50M.

Para carpetas grandes, subir la carpeta local `work/m10m` desde la raíz del volumen
con **Select folder**; verificar que el destino final es `source_parts/m10m/`.
El selector nativo de Windows puede requerir pulsar manualmente **Cargar** y aceptar
la confirmación de Chrome de los 101 archivos. No aceptar sustituciones de archivos.
Antes de lanzar, comprobar archivos remotos y bytes; el Job repite la validación de
SHA distribuida. Parámetros 10M: rows=10000000, release=bulk_10m_r001,
manifest_path=/Volumes/workspace/twitch_bulk_experimental/source_parts/m10m/manifest.json,
predecessor=workspace.twitch_bulk_experimental.publication_bulk_1m_r001,
max_input_bytes=1342177280. No lanzar si no existe el manifiesto remoto completo.
