# Modelo Gold ampliado

Nombres por defecto `workspace.twitch_lakehouse.gold_i3_<familia>_r01`.
`r01` es identificador de publicación, no fecha del dataset. Cada publicación
es inmutable por contrato operativo. No se altera ningún objeto I1.
Las tablas son hechos agregados con dimensión degenerada `streamer_username`;
no se impone una dimensión artificial de fechas ni se inventan entidades.

| Familia | Clave lógica | Columnas adicionales (tipo Spark esperado) | Origen |
| --- | --- | --- | --- |
| streamer | streamer_username STRING | interactions, unique_users, unique_streams, estimated_minutes, recurring_users BIGINT; recurrence_rate DOUBLE | Gold I1 validada + conteo usuario/streamer de Silver |
| duration | streamer_username STRING, duration_bucket INT | interactions, estimated_minutes BIGINT | Silver; CASE de 5 buckets sin UDF |
| activity | streamer_username STRING, time_start BIGINT | connection_starts, started_estimated_minutes BIGINT | Silver agrupada por paso de inicio |
| quality | release STRING | source_namespace, status, scope STRING; bronze_rows, silver_rows, rejected_rows, duplicates_removed_inferred, silver_duplicate_keys, gold_streamers, estimated_minutes BIGINT; bronze_version, silver_version, rejects_version, gold_version BIGINT | versiones y conteos leídos; controles ejecutados |

Tipos numéricos de quality inferidos de enteros Python como BIGINT. Claves lógicas
no son constraints físicas Delta. La tabla quality debe contener exactamente una
fila. No se añaden filas vacías a duration: el SQL aporta los cinco labels.

No sumar familias entre sí: son representaciones de los mismos hechos. No unir
duration con activity por streamer: produciría fanout. El dashboard usa datasets
independientes con parámetros comunes. `gold_streamer_metrics` permanece como
baseline I1; la copia estrecha extendida fija la población para BI por release.

## Coste y consistencia

Se fijan versiones Delta de Bronze, Silver, rejects y Gold antes de leerlas.
Las versiones no prueban que procedan del mismo batch: la equivalencia Gold/Silver
y conteos detectan inconsistencias dentro del alcance; procedencia I1 se conserva
como dependencia explícita. No hay acceso al CSV ni descarga.

Shuffle exacto por (streamer, usuario), reducción por streamer; otros dos groupBy
por streamer/bucket y streamer/inicio. Activity crece como máximo hasta N filas;
duration hasta 5×streamers; streamer exactamente un registro por streamer.
No se hace explode de intervalos, countDistinct por bucket, join de hechos ni
ordenamiento global del histórico crudo. ROW_NUMBER ordena solo los streamers.
La validación Gold contra Silver y la equivalencia posterior al guardado añaden
lecturas deliberadas; Spark puede recomputar planes al no usar cache (compatible
con restricciones de compute serverless). No se promete una sola lectura ni
tiempos de ejecución. Evaluar EXPLAIN/coste antes de escalar a 124M.

CREATE estricto seguido de append solo al destino recién creado; todas las
ausencias se comprueban al principio, con segunda guarda al escribir cada tabla.
quality se crea después de verificar los tres agregados persistidos. No hay
commit atómico multitabla: si falla un paso, no usar la release, conservar salidas
y elegir un sufijo nuevo tras diagnóstico. Si falla guardar quality, inspeccionar
su contenido y salida del notebook antes de consumir. No hay recuperación que
borre, sustituya o anexe automáticamente. No permitir mutadores externos sobre
los candidatos ni borrado concurrente durante publicación.
