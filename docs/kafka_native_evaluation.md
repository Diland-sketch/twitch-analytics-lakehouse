# Lakeflow Kafka Beta: decisión y piloto de 1.000 filas

> Actualización posterior: el usuario confirmó ingesta remota de 1.000 filas desde
> full_a.csv.gz, Bronze solo key/value. Para auditar esa ejecución usar la
> [adaptación binaria del notebook 22](kafka_native_binary_audit.md). Los estados
> pendientes de esta evaluación inicial describen la entrega anterior.

Esta guía es la decisión vigente; [la guía Spark](kafka_databricks_pilot.md) conserva
el respaldo implementado. **Ninguna ruta está validada remotamente en esta entrega.**

## Recomendación

Evaluar primero Lakeflow Connect Kafka nativo con Bronze binaria y metadata explícita.
Encaja con UC y reduce el código que opera el consumidor. Mantener Spark directo
con AvailableNow como respaldo, sin desplegar ambos. Abrir el formulario confirma
visibilidad, pero no demuestra CREATE/USE CONNECTION, habilitación Beta, egress,
permisos de pipeline ni escritura Delta. No es correcto afirmar todavía compatibilidad
efectiva con este Free Edition.

**Diferencia decisiva:** la FAQ oficial indica que el managed connector omite offsets
eliminados por retención y continúa. Su referencia no ofrece `failOnDataLoss=true`.
No trasladar esa opción Spark al Bundle. Por ello, usar manifiesto, ACK, conservación
de la fuente y auditoría de cobertura como barrera externa: un pipeline que sigue
activo o termina sin error no equivale a una carga completa. Si se exige detener el
consumidor ante cualquier pérdida Kafka, elegir el respaldo Spark, que sí configura
ese contrato. Lakeflow solo se promoverá a escala con cobertura y recuperación probadas.

| Aspecto | Lakeflow Connect Kafka | Spark directo conservado |
|---|---|---|
| Autenticación | Conexión UC existente, credenciales fuera del Bundle | Secret scope leído en memoria por notebook |
| Estado | Gestionado por pipeline; conservar pipeline ID y su estado | Checkpoint explícito nuevo en volumen |
| Ejecución inicial | Pipeline continuo según ejemplo oficial; ventana breve y parada manual | Job finito AvailableNow |
| Pérdidas por retención | Puede saltar offsets: auditoría externa obligatoria | failOnDataLoss=true |
| Código propio | Productor, manifiesto y verificación; ingesta administrada | Añade consumidor Spark y su operación |
| Free Edition | Permisos/Beta/egress/cuota por confirmar | Fuente/SASL/egress/secretos/cuota por confirmar |

No se usa Lakeflow como atajo para habilitar una conexión que la red deniega. La
cuota de cómputo y las restricciones de salida siguen aplicando a ambas rutas.

## Bloqueos y comprobación antes de publicar

| Comprobación manual | Estado actual | Evidencia necesaria |
|---|---|---|
| Formulario Kafka visible | Confirmado por el usuario | Solo prueba visibilidad |
| Beta Kafka habilitada por administrador | Pendiente | Preview habilitada y creación aceptada |
| UC + serverless y permisos de destino | Pendiente en este esquema nuevo | USE CATALOG, USE/CREATE SCHEMA, CREATE TABLE |
| CREATE CONNECTION o conexión existente con USE CONNECTION | Pendiente | Conexión guardada y accesible para la identidad ejecutora |
| Servicio Aiven ya autorizado, SASL_SSL y CA compatible | Sin configuración disponible para este trabajo | Broker/topic/mecanismo y prueba de conexión sin secretos en logs |
| Egress Databricks hacia todos los brokers | Pendiente | DNS/TLS/ACL efectivas desde el pipeline |
| Capacidad de pipeline Free | Pendiente | Cuota disponible sin interrumpir pipelines existentes |
| CLI / autenticación local Databricks | CLI no instalada en el entorno revisado | CLI con Bundles y autenticación autorizada |

Free Edition limita salida a dominios de confianza, solo ofrece serverless y permite
un pipeline activo por tipo. No ofrece configuración de red privada personalizada.
No hay una garantía documental específica de que Aiven esté permitido en este
workspace. La exclusión de conectores CDC con gateway clásico no demuestra por sí
misma que Kafka serverless esté excluido. Verificar cada condición en lugar de inferir.

La conexión administrada admite usuario/contraseña SASL PLAIN o SCRAM y service
credentials; no asumir que una credencial MSK/IAM equivale a Aiven. Crear la conexión
manualmente en Catalog Explorer con bootstrap y autenticación reales. No se crea por
Bundle ni se copia su contraseña al YAML. Si la cadena CA privada del servicio no
puede configurarse mediante el contrato disponible del conector, detener el piloto
y registrar el bloqueo; no desactivar TLS. No crear Aiven ni recursos de pago.

## Archivos preparados

```text
bundles/kafka_native/
  databricks.yml                 # Bundle y target experimental, conexión como referencia
  resources/bronze.pipeline.yml # Un único recurso: ingesta nativa Bronze
notebooks/22_kafka_native_verify_pilot.py # Auditoría batch; NO otro consumidor Kafka
kafka_ingest/                   # Productor/contratos/controlador preservados
notebooks/21_kafka_bronze_pilot.py # Respaldo Spark, sin desplegar
```

Los `.yml` contienen JSON, subconjunto válido de YAML, para verificarlos localmente
sin un parser adicional. El Bundle fija destino
`workspace.twitch_kafka_experimental.bronze_kafka_native_pilot_v1`, conserva key/value
binarios y añade `kafka_source` con topic, partition, offset, timestamp, timestampType
y headers mediante `table_configuration.source_metadata_column`.
`starting_offset=earliest` evita perder el bloque publicado antes del primer inicio;
solo se aplica cuando no existe checkpoint. No se configura un volumen/checkpoint
Spark ni se referencia el del respaldo o I4.

Se utiliza `channel=PREVIEW`, serverless y `lifecycle.prevent_destroy`. El estado
gestionado no se elimina ni se reinicia. No usar Full refresh, reset, destroy, borrar
tablas ni volver a crear el pipeline para reanudar. No cambiar identidad del Bundle,
target, conexión ni topic después de comenzar sin revisar el estado persistido.
`prevent_destroy` protege el recurso frente al Bundle, no frente a acciones manuales.

Se omiten catalog/target al nivel del pipeline porque la referencia general de
`ingestion_definition` los declara incompatibles; los destinos van en cada objeto.
El ejemplo específico de Kafka sí los muestra: hay una discrepancia documental.
**Resolver con `bundle validate` y el API real antes de desplegar**; no afirmar que
la configuración fue aceptada basándose solo en los tests de archivos.

## Prueba mínima propuesta, secuencia segura

1. Mantener el laboratorio I4 y r01 intactos. Crear únicamente el esquema experimental
   nuevo si hay permisos y comprobar que el nombre de tabla nativa está libre.
2. Configurar Aiven/UC manualmente y comprobar requisitos de la tabla anterior.
   Topic dedicado `twitch.pilot.twitch1000`, una partición, sin compactación, sin
   otros productores y retención superior a ventana + recuperación.
3. En el Git folder nuevo, actualizar esta rama y reiniciar Python antes de importar.
   Configurar CLI con autenticación segura; nunca versionar tokens. Desde el Bundle:

```powershell
Set-Location bundles\kafka_native
databricks bundle validate -t experimental --var connection_name=NOMBRE_CONEXION_UC
```

   Guardar salida sanitizada. La validación de configuración no prueba conectividad.
   Si aparecen propiedades desconocidas, permisos denegados o Beta no habilitada,
   registrar el error y no publicar. No está ejecutado: falta CLI/acceso configurado.

4. Tras revisión de la configuración validada, desplegar manualmente en Free Edition:

```powershell
databricks bundle deploy -t experimental --var connection_name=NOMBRE_CONEXION_UC
databricks bundle summary -t experimental --var connection_name=NOMBRE_CONEXION_UC
```

   Registrar pipeline ID; no iniciar automáticamente ni desplegar el respaldo Spark.
   No ejecutar estos pasos si implican un servicio facturable no autorizado.
5. Volver a la raíz del repositorio. Preparar 1.000 líneas reales con el productor
   existente, sin descargar el histórico. Fuente CSV o GZIP ya disponible:

```powershell
python -m kafka_ingest.controller prepare --source C:\ruta\full_a.csv.gz --block-dir work\native-1000 --topic twitch.pilot.twitch1000 --cluster-name IDENTIDAD_ESTABLE_AIVEN
python -m kafka_ingest.controller publish --block-dir work\native-1000 --rate 100
```

   Requiere variables KAFKA_* de la guía Spark, inyectadas de forma segura. `publish`
   solo envía/guarda ACK: **no ejecuta Spark ni avanza cursor**. No usar `run` aquí:
   esa suborden conserva el controlador del respaldo Spark directo.
6. Iniciar una actualización normal del pipeline desde la UI, sin Full refresh.
   El ejemplo oficial usa modo continuo: establecer ventana supervisada de 15 min
   como máximo y detener manualmente si no converge o consume cuota. No dejarlo
   encendido esperando nuevos datos. La configuración de modo triggered se evaluará
   con el API tras el piloto; no se presupone soporte del conector Beta.
7. Esperar a que el topic esté alcanzado y detener normalmente el pipeline. Registrar
   pipeline ID/update ID. Inspeccionar el esquema real de Bronze. Ejecutar notebook 22
   con repo_root absoluto, JSON del manifiesto, pipeline_id y update_id. La auditoría
   fija una versión Delta (`versionAsOf`), normaliza metadata y reutiliza la lógica
   de identidad/cobertura/calidad; no consume Kafka ni modifica Bronze.
8. Comparar sus offsets con TODOS los ACK y conservar tabla `audit_<uuid>`, versión
   Delta y capturas. Primer envío: 1.000 ACK / 1.000 transport_rows / 1.000 source_rows,
   hash exacto y rango fuente 1..1000, sin conflictos de payload/identidad ni duplicados
   de transporte. Reportar rechazos y duplicados de negocio separadamente.
9. Reiniciar el mismo pipeline sin publicar nada; repetir auditoría: mismos totales
   y contenido. Una actualización nueva/commit vacío puede cambiar versión Delta,
   pero no cobertura. Hacer ensayo controlado de reenvío con mismos event_id cuando
   este recorrido base ya pase: transport_rows puede subir, source_rows permanece 1000.

Solo después de estas pruebas se diseñará el adaptador nativo del controlador:
disparar/observar update + Job de verificación + persistir PASS y cursor. Hoy `run`
sigue enlazado únicamente al respaldo Jobs/Spark; no se presenta como controlador
Lakeflow ya operativo. No pasar manualmente un pipeline RUNNING a estado de carga
confirmada. No escalar a 100.000 ni millones antes del PASS remoto y el reinicio.

Si Kafka elimina datos, la auditoría debe fallar aunque Lakeflow continúe. Conservar
el reporte, revisar ACK y volver a publicar de forma controlada desde fuente local
con identidad estable. No ocultar los offsets perdidos ni marcar éxito hasta que
se documente y resuelva la discrepancia. Si la recuperación del managed connector
no cumple integridad exigida, usar el respaldo con un topic/tabla/checkpoint propios.

## Diseño de Silver y Gold con Declarative Pipelines

No se despliega todavía un segundo pipeline completo. Tras Bronze y cobertura PASS,
preparar un pipeline de transformación **triggered**, separado de la ingesta, activado
una vez por carga global aprobada:

* Bronze binaria → vista normalizada con `from_json` explícito del contrato del productor.
* Expectativas de identidad y calidad: registrar rechazos y detalles, no descartar
  silenciosamente filas. Reutilizar reglas de `classify`, incluido intervalo relativo.
* Vista/materialized view fuente única por `(source_sha256, twitch_source_row)`, con
  conflictos de contenido como barrera FAIL, antes de deduplicar clave KEY de negocio.
* Silver materializada global, claves normalizadas y minutos estimados `(stop-start)*10`.
* Gold materializadas por streamer y agregados I3, con distinct globales; no sumar
  usuarios distintos por bloque. No recalcular tras cada bloque Kafka.
* Auditoría posterior: transporte/fuente/rechazos/duplicados de negocio, conteos Silver
  y sumas Gold; publicar release/snapshot exclusivamente tras PASS.

Destinos separados: `silver_kafka_<version>` / `gold_kafka_<version>` en esquema
experimental; nunca adoptar ni refrescar tablas de `workspace.twitch_lakehouse`/r01.
Definir el DAG Bronze administrada → barrera de cobertura → transformación Silver/Gold
→ auditoría → snapshot confirmado FastAPI/React. El primer adaptador React seguirá
siendo publicación de snapshot, no contadores ACK ni lecturas SQLite mezcladas.
Si la cuota impide dos pipelines activos, ejecutarlos secuencialmente con ingesta detenida.
Medir si las materialized views pueden refrescar incrementalmente; no prometerlo para
deduplicación y distinct globales sin comprobar su plan y requisitos.

## Evidencia disponible y límites

* 27 tests locales de contratos/preparación/Bundle pasaron en 3,053 s; 4 tests
  Spark 3.5.8 sobre fixtures, incluido el adaptador de metadata nativa, pasaron en
  33,668 s. Son pruebas de código, no ingesta remota.
* Se prepararon 1.000 líneas REALES del CSV local: SHA fuente
  `11d3bdb49683f651eb6b0ba806bf317db68646efc0c7de2d02d750201fd8ef69` y SHA del bloque
  `1fbb5b2a73afaf25dd975deb69870e039de9642b45f14b933c019873e238ecbd`.
  Preparación no significa publicación ni persistencia Delta.
* Smoke local adicional: **FAILED** al conectar `kafka:9092`; el broker I4 registraba
  SIGTERM/apagado y estaba unhealthy. No se reinició I4. No hay PASS Kafka local nuevo,
  ni ACK/Delta remotos. Logs conservados en `work/local-real-1000.log`.
* Bundle: verificado como archivos/contratos locales; `bundle validate`, conexión UC,
  despliegue y prueba remota **NO EJECUTADOS**. No hay credenciales configuradas para
  operar Aiven/Databricks desde este entorno.

Fuentes: [requisitos y Bundle Kafka Beta](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-pipeline),
[opciones y metadata](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-reference),
[pérdida por retención](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-faq),
[Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations),
[conexión UC](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/kafka-source-setup),
[recursos Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/resources).
