# Experimento de 10 millones de registros — procedimiento reproducible

**Rama:** `experimental/full-dataset-124m`.

**Estado:** código implementado, sujeto a prueba en Databricks real. La auditoría de
1 millón ya se reportó PASS en una sesión previa y se preserva sin escrituras.
No afirmar 10M procesados hasta disponer de la salida de `20_full_10m_audit.py`.

## Diseño: lotes Bronze persistidos y Silver/Gold globales

- 100 fragmentos de **100.000 líneas**, extraídos del mismo GZIP original.
- Diez tablas Bronze, cada una con 10 fragmentos/1 millón de filas. Cada tabla
  es un *punto de recuperación duradero*: cuando ya existe, se cotejan su
  población por `source_file` y la huella fuente registrada; **no se escribe
  nuevamente** ni se duplica el lote.
- **Silver global** sobre la unión de las diez tablas: validación I1,
  rechazados y deduplicación por los cinco campos. NO deduplicar lote por lote
  si después se suman conteos.
- **Gold global** por streamer calculado sobre Silver global, para no
  sumar erróneamente usuarios/streams distintos entre lotes.
- Se crea el esquema independiente
  `workspace.twitch_full_10m_experimental` y el volumen `fragments_10m`.
  No se escriben tablas del millón anterior.

La recuperación implementada es **por lote Bronze terminado**. Si falla una
escritura dejando una tabla incompleta, la verificación del siguiente intento
bloquea el avance: se preserva evidencia y se examina el incidente. La
escritura Silver/Gold no es una transacción conjunta ni admite recuperación
incremental automática. No se elimina ni reemplaza una tabla existente.

## A. Windows — preparar 10 millones

```powershell
cd C:\Proyectos\twitch-analytics-lakehouse
git switch experimental/full-dataset-124m
git pull origin experimental/full-dataset-124m
python -m twitch_lakehouse.full_dataset_prepare --source "C:\Datasets\Twitch\full_a.csv.gz" --output "work\full_dataset\sample_10m" --rows 10000000 --rows-per-part 100000
```

La extracción produce `part-000000.csv` ... `part-000099.csv` y
`manifest.json` en un directorio nuevo. Se vuelve a calcular SHA256 del GZIP;
comparar con el hash documentado en la prueba 1M:
`2475bcbf0a8fdf1864c78bd62aee35384f9ae371788348fa0b8831c5d827e11e`.
Si difiere, **detenerse**: no son exactamente los mismos bytes fuente.
No borrar `sample_1m`.

Verificar el manifiesto localmente:

```powershell
$dir="work\full_dataset\sample_10m"
$m=Get-Content "$dir\manifest.json" -Raw | ConvertFrom-Json
if ($m.extracted_rows -ne 10000000 -or $m.parts.Count -ne 100) { throw "Población/partes incorrectas" }
if (($m.parts | Measure-Object -Property rows -Sum).Sum -ne 10000000) { throw "Suma inconsistente" }
foreach ($p in $m.parts) {
  $file=Join-Path $dir $p.file
  if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -ne $p.sha256) {
    throw "SHA256 de parte inválido: $($p.file)"
  }
}
"PASS: 10M filas, 100 partes, SHA256 correctos"
```

## B. Databricks — ejecutar cada notebook en orden

1. En la Git folder seleccionar rama experimental y sincronizar. En
   `00_config.py`, conservar el `project_root` correcto. Si hace falta,
   reiniciar Python con `dbutils.library.restartPython()` y volver a iniciar.
2. Ejecutar `16_full_10m_bootstrap.py`. Crea exclusivamente el esquema/volumen
   experimental de 10M.
3. Subir al volumen **100 archivos CSV + manifest.json** desde `sample_10m`.
   Para evitar inconsistencias: concluir y verificar toda la subida antes de
   ejecutar Bronze. No renombrar ni sobreescribir fragmentos.
4. Ejecutar `17_full_10m_bronze_batches.py`. Imprime por lote:
   `batch`, `action` CREATED/REUSED, `rows=1000000`, `files=10`,
   `elapsed_seconds`. Deben aparecer diez lotes verificados y
   `ALL_BRONZE_VERIFIED` con `bronze_rows=10000000`.
5. Ejecutar **una sola vez** `18_full_10m_silver.py`: valida/deduplica
   globalmente, compara Bronze=valid+rejects y valid=Silver+duplicados;
   verifica las tablas persistidas.
6. Ejecutar **una sola vez** `19_full_10m_gold.py`: agrega globalmente.
7. Ejecutar `20_full_10m_audit.py` (solo lectura). El PASS exige 10M Bronze,
   10 lotes íntegros, clave Silver única y Gold coherente.

## C. SQL de corroboración

```sql
SELECT COUNT(*) FROM workspace.twitch_full_10m_experimental.bronze_batch_000_10m;
SELECT COUNT(*) FROM workspace.twitch_full_10m_experimental.bronze_batch_009_10m;
SELECT COUNT(*) AS silver, SUM(estimated_minutes) AS minutes
  FROM workspace.twitch_full_10m_experimental.silver_interactions_10m;
SELECT COUNT(*) AS rejects FROM workspace.twitch_full_10m_experimental.silver_rejects_10m;
SELECT COUNT(*) AS streamers, SUM(interactions) AS connections,
       SUM(estimated_minutes) AS minutes
  FROM workspace.twitch_full_10m_experimental.gold_streamer_metrics_10m;
SELECT user_id, stream_id, streamer_username, time_start, time_stop, COUNT(*) AS n
  FROM workspace.twitch_full_10m_experimental.silver_interactions_10m
  GROUP BY user_id, stream_id, streamer_username, time_start, time_stop
  HAVING COUNT(*) > 1 LIMIT 10;
DESCRIBE HISTORY workspace.twitch_full_10m_experimental.silver_interactions_10m LIMIT 5;
```

Las diez tablas Bronze se pueden auditar con `20_full_10m_audit.py`.
Para una tabla Gold, la suma de conexiones debe igualar Silver, pero
los streamers distintos del millón inicial **no se suman** entre lotes.

## D. Advertencias y límites honestos

- Fragmentos de entrada inmutables durante todo el proceso.
- Verificación de **hash de archivos** local; en Databricks la auditoría
  comprueba SHA256 fuente *registrado* y conteos por ruta, **no recalcula**
  SHA256 sobre cada fragmento remoto. Registros con igual conteo pero distintos
  bytes podrían superar esta auditoría; conservar evidencias y permisos de volumen.
- Databricks Free Edition puede agotar cuotas, especialmente en la deduplicación
  global de 10M. No ejecutar notebook 18 simultáneamente con otro proceso masivo.
- El script registra tiempos por notebook/lote, **no métricas reales de RAM/CPU
  del servidor**: esas deben recogerse de Databricks si se muestran.
- `full_a.csv.gz` contiene conexiones históricas a chats, no audiencia en
  vivo ni minutos de reproducción verificados.
- La preparación genera el prefijo de 10M, no diez millones *nuevos* además
  del primer millón, y **no equivale a 11M únicos**.
