# Etapa 2 — carga de un millón en Spark/Delta

Esta implementación es **experimental**, aislada de las tablas de I1/I3. La prueba real en Databricks todavía debe ejecutarse; la suite CI usa archivos sintéticos o dobles, **no** el millón real.

## Operación secuencial en Databricks Free

1. En la Git folder seleccionar `experimental/full-dataset-124m` y Pull. Configurar `notebooks/00_config.py` con `project_root=/Workspace/...` real, solo para Python path.
2. Ejecutar `11_full_bootstrap.py`: crea el esquema separado `workspace.twitch_full_experimental` y su volumen `fragments_1m`.
3. Subir **manualmente** los diez CSV `part-000000.csv` … `part-000009.csv` **y** `manifest.json` de `work/full_dataset/sample_1m` a la raíz del volumen recién creado. No cambiar nombres o contenido.
4. Ejecutar `12_full_bronze.py`: coteja el manifiesto, verifica **conteo por archivo**, conserva ruta de origen por fila, escribe Delta `bronze_interactions_1m`.
5. Ejecutar `13_full_silver.py`: aplica las reglas originales I1, cuarentena, deduplicación y comprueba persistencia. La población Silver puede ser menor al millón.
6. Ejecutar `14_full_gold.py`: genera agregados por streamer y compara totales de Silver antes/después de escribir.
7. Ejecutar `15_full_audit.py` (solo lectura): debe registrar `PASS`, `bronze=1000000`, `bronze=silver+rejected+duplicates_inferred`, `gold_interactions=silver` y minutos coincidentes.

**Solo una vez** cada notebook de escritura 12–14. Los destinos existentes bloquean el reintento para impedir sobrescrituras. Si falla, guardar logs, no borrar tablas para ocultar el error. Otra ejecución usa otro esquema experimental configurado explícitamente.

## SQL de corroboración

```sql
SELECT COUNT(*) AS bronze FROM workspace.twitch_full_experimental.bronze_interactions_1m;
SELECT COUNT(*) AS silver, SUM(estimated_minutes) AS minutes FROM workspace.twitch_full_experimental.silver_interactions_1m;
SELECT COUNT(*) AS rejects FROM workspace.twitch_full_experimental.silver_rejects_1m;
SELECT SUM(interactions) AS connections, SUM(estimated_minutes) AS minutes FROM workspace.twitch_full_experimental.gold_streamer_metrics_1m;
SELECT user_id,stream_id,streamer_username,time_start,time_stop,COUNT(*) AS n
FROM workspace.twitch_full_experimental.silver_interactions_1m
GROUP BY user_id,stream_id,streamer_username,time_start,time_stop
HAVING COUNT(*) > 1 LIMIT 10;
DESCRIBE DETAIL workspace.twitch_full_experimental.bronze_interactions_1m;
DESCRIBE HISTORY workspace.twitch_full_experimental.bronze_interactions_1m LIMIT 5;
```

Las cinco primeras consultas son de solo lectura. La consulta de claves repetidas debe devolver **0 filas**. El volumen estático se debe mantener intacto durante el proceso.

## Limitaciones

El hash SHA256 de partes se valida localmente mediante el manifiesto y el comando de Windows; Databricks solo coteja los **conteos por fragmento**, no el checksum remoto. No se procesa todavía 10M ni 124M; tampoco hay reanudación transaccional entre capas o publicación automática a React. No tocar el esquema `workspace.twitch_lakehouse` ya aprobado.
