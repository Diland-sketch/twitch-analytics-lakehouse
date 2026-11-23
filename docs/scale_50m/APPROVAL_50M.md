# 50M: revisión terminada, ejecución pendiente de autorización

No se ha transferido m50m ni iniciado el Job 50M. Kafka y React quedan fuera.
Esta preparación conserva los Parquet existentes; no vuelve a extraer la fuente.

## Evidencia disponible

- 10M remotos: run `547592370095797`, SUCCESS, 272,594 s, publicación
  `workspace.twitch_bulk_experimental.publication_bulk_10m_r001` CONFIRMED.
  Siete controles PASS, Silver 10.000.000, rechazos 0, duplicados de negocio 0,
  Gold 278.411 streamers y 306.525.720 minutos estimados.
- Revisión remota nueva: los 11 bloques Bronze conservan su versión certificada,
  ningún run batch activo, consulta SELECT 1 correcta. El volumen tiene 114 archivos
  en m100k/m1m/m10m, con 1.101.626.657 bytes observados. No hay m50m en esa revisión.
- 50M locales: preparación completada en 305,710 s, 500 Parquet/4.961.822.716 bytes.
  Validación independiente PASS en 280,961 s: 50.000.000 ordinales consecutivos,
  SHA del GZIP original, 500 SHA de archivo, hashes de línea y digests secuenciales.
  Los primeros 100 fragmentos coinciden exactamente con el manifiesto de 10M.
  Esto no demuestra procesamiento remoto de 50M.

Los reportes están en `evidence/batch_10m_publication.json`,
`evidence/local_preflight_50m.json` y `evidence/remote_preflight_50m.json`.
El manifiesto definitivo está en `evidence/source_manifest_50m_prepared.json`.

## Operaciones propuestas para autorización

1. Transferir solamente 400 Parquet nuevos y el manifiesto: **401 archivos,
   3.969.502.311 bytes**. Origen preparado:
   `C:\movielens-datawarehouse\twitch-scale-50m\work\transfer_ready\m50m`.
   Son hard links a los originales: no se han duplicado los 3,97 GB en disco.
   No editar los enlaces; compartirían cambios con los originales. El primer intento
   local de crear enlaces fue denegado; se conservó el directorio vacío y el intento
   posterior con permisos de archivo adecuados terminó correctamente en otro destino.
2. Destino: `/Volumes/workspace/twitch_bulk_experimental/source_parts/m50m/`.
   **Overwrite desactivado.** Los 100 archivos anteriores se leen desde m10m;
   no se vuelven a subir ni copiar. El manifiesto original completo no se modifica.
3. Tras comprobar archivos recibidos y SHA, actualizar exclusivamente el código Git
   fijado del Job batch `685945819660775` y ejecutar `bulk_50m_r001`.
   La configuración remota aún no se ha actualizado al código candidato.
4. El Job verifica el predecesor y las 11 versiones Delta/recibos existentes antes
   de autorizar la entrada. Después certifica 40 bloques nuevos de 1M y calcula
   Silver y Gold globales sobre 50M. No suma DISTINCT de agregados parciales.

**88 tablas nuevas previstas**, todas en `workspace.twitch_bulk_experimental`:
40 `bronze_2475bcbf0a8f_<inicio>_<fin>` para 10.000.001..50.000.000,
40 `receipt_2475bcbf0a8f_<inicio>_<fin>` y ocho tablas de release:
`input_audit_bulk_50m_r001`, `bronze_ledger_bulk_50m_r001`,
`rejects_bulk_50m_r001`, `silver_bulk_50m_r001`, `silver_audit_bulk_50m_r001`,
`gold_bulk_50m_r001`, `gold_audit_bulk_50m_r001`, `publication_bulk_50m_r001`.
El inventario exacto figura en `evidence/execution_plan_50m_pending_approval.json`.
Los 11 bloques previos se leen y reutilizan; no se modifican ni recrean.
Los conteos Silver, Gold, rechazos y duplicados de 50M son todavía desconocidos.

## Capacidad y límites

- Disco local libre observado después del staging: 49.342.898.176 bytes.
- Archivo Parquet más grande: 10.010.980 bytes, inferior al límite de UI de 5 GB
  por archivo. El volumen tendría 5.071.128.968 bytes de archivos fuente después
  de esta transferencia, excluyendo tablas Delta, logs y espacio temporal.
- Presupuesto de entrada del Job: 6 GiB (6.442.450.944 bytes), superior a los
  4.961.822.716 bytes Parquet de la población. **Es un guard del código, no saldo
  garantizado de almacenamiento ni cuota restante de Databricks.**
- No se obtuvo una cifra verificable de cuota Serverless restante ni de espacio
  libre del volumen. SELECT 1 correcto solo acredita disponibilidad actual.
  La consulta de system.billing.usage para este Job devolvió cero registros;
  no equivale a coste cero ni permite estimar DBU/importe consumido.
- Free Edition puede detener cómputo al agotar fair usage. No se crean recursos
  pagados ni se cambia de plan. El ensayo requiere aceptar esta incertidumbre;
  cualquier error de cuota/capacidad detiene el avance y queda registrado.
- Un run activo como máximo, cinco tareas seriales, retries=0,
  disable_auto_optimization=true, timeout Job 7.200 s, por tarea 3.600 s.
  No se promete duración ni throughput de 50M con los tiempos de 10M.

## Recuperación y supervisión

Si una subida queda parcial, comparar nombres, bytes y hashes recibidos y preparar
solo los faltantes. No reenviar todo por defecto ni sustituir archivos existentes.
Una discrepancia de hash requiere detenerse y revisar un destino nuevo.

Si el Job falla, conservar run, errores, tablas y recibos. Un Repair run solo
reutiliza bloques cuya versión Delta, identidad de fuente y recibo siguen intactos,
con idénticos parámetros. Una tabla parcial inconsistente bloquea la reparación;
no se completa mediante append indiscriminado ni se elimina automáticamente.
No hay transacción multi-tabla; solamente la publicación CONFIRMED autoriza consumo.

Registrar antes/después: archivos, SHA, run/commit, filas fuente, válidas, rechazos,
duplicados, Silver, Gold, minutos estimados, duraciones, tamaños Delta, fallos y
cuotas/costos realmente observables. No mezclar estos registros con r01 ni Kafka.

Preparación reproducible (local, sin red ni ejecución de Jobs):

```powershell
python -m bulk_lakehouse.preflight --parts work/m50m --rows 50000000 --prefix work/m10m --source C:\Users\dilan\Downloads\full_a.csv.gz --output work/preflight-50m.json
python -m bulk_lakehouse.transfer --parts work/m50m --preflight work/preflight-50m.json --destination work/transfer_ready/m50m --plan work/transfer-50m-plan.json
```

Ambos comandos ya terminaron; no repetir sobre esos destinos existentes.

Fuentes oficiales: [Free Edition y fair usage](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations),
[carga a volúmenes y límite por archivo](https://docs.databricks.com/aws/en/volumes/volume-files).
