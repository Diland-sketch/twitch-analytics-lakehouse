# Twitch Analytics Lakehouse — resumen técnico I1

Material para la presentación de avances del 18 de noviembre de 2026.
Resultados reales reportados por Diland y Anghela desde Databricks Free Edition;
no son una ejecución independiente del asistente ni cifras del histórico completo.

## Objetivo y alcance alcanzado

Construir un Lakehouse batch reproducible sobre `100k_a.csv` con PySpark,
Delta Lake y arquitectura Medallón en el catálogo `workspace`, esquema
`twitch_lakehouse`, con presupuesto cero y sin servicios facturables nuevos.

Bronze conserva líneas originales y procedencia. Silver aplica tipado explícito,
reglas de calidad y deduplicación por los cinco campos normalizados. Los registros
inválidos se dirigen a cuarentena. Gold agrega interacciones, usuarios/streams
distintos y minutos estimados por streamer. Configuración y transformaciones están separadas.

## Resultados verificados por el equipo

| Indicador | Resultado |
| --- | --- |
| Bronze | 3.051.733 registros |
| Silver válida | 3.051.733 registros |
| Rechazados / duplicados | 0 / 0 según las reglas implementadas |
| Gold | 162.625 streamers |
| Interacciones conciliadas | 3.051.733 |
| Minutos acumulados estimados | 95.887.110 |
| Almacenamiento Bronze | Delta, 2 archivos de datos, 43.938.329 bytes |

La reconstrucción desde Bronze produjo una candidata independiente con 3.051.733
filas. exceptAll original→candidata y candidata→original dieron 0 diferencias:
`equivalencia=True`. La Silver original permaneció intacta.

## Lecciones técnicas demostradas

- El workspace rechazó SaveMode.errorifexists. La corrección `5994433` usa validación
  de ausencia, CREATE TABLE Delta estricto y append solo tras creación exitosa.
  Los destinos existentes se bloquean; no hay overwrite ni borrado automático.
- Git Pull actualiza archivos, pero puede dejar funciones anteriores importadas en
  memoria. Reiniciar Python y ejecutar 00_config resolvió la incidencia observada.
- La candidata se llamó `silver_interactions_candidate_suffix` por pasar un literal
  en Databricks. El notebook versionado ya usa `candidate_suffix=suffix` correctamente;
  la separación de destinos protegió la tabla original.
- Las pruebas locales distinguen dobles de escritura y datos artificiales de la
  validación real. El equipo confirmó además 01_artificial_checks con PASS.

## Límites y siguiente alcance

I1 batch está completado. Esto no equivale a terminar todo el proyecto: Kafka sigue
pendiente, a cargo de Anghela; no se implementó ni simuló. El histórico completo de
aproximadamente 124 millones de interacciones no se descargó ni procesó en I1.
La rúbrica oficial del curso aún no fue proporcionada.

Cero rechazos y duplicados solo describe las reglas actuales. Los minutos se calculan
como diferencia de pasos por diez: son estimados desde observaciones de chat, no
medición exacta de atención. No se atribuyen fechas calendario a los pasos temporales.
La equivalencia comprueba reproducibilidad del contenido bajo las mismas reglas,
no una validación externa de su semántica.

Diland: ingeniería de datos, Spark y Lakehouse. Anghela: Kafka, calidad y presentación.
La rama `feat/i1-spark-medallion` queda para revisión, sin merge automático.

## Guion breve para exposición

En I1 implementamos y ejecutamos un flujo batch Medallón en Databricks Free Edition.
Partimos del subconjunto 100k de Twitch, que contiene 3.051.733 interacciones.
Bronze preserva los registros originales; Silver aplica las reglas de calidad y
deduplicación; Gold resume el resultado en 162.625 streamers. Conciliamos todas las
interacciones y obtuvimos 95.887.110 minutos estimados, con cero rechazos y duplicados
según nuestras reglas.

También reconstruimos Silver desde Bronze en una tabla independiente. La comparación
en ambos sentidos dio cero diferencias, sin modificar la Silver original. Resolvimos
una incompatibilidad de escritura con Spark Connect y aprendimos a reiniciar Python
después de actualizar módulos. El flujo batch queda demostrado; la integración Kafka
corresponde al siguiente incremento y no se presenta como implementada.

Referencias de evidencia: [validación](validacion.md),
[reconstrucción](reconstruccion_silver.md), [operación](operacion_silver_gold.md).
