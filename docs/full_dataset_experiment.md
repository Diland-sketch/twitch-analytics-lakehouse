# Full dataset — experimento 124M, etapa 1

**Rama:** `experimental/full-dataset-124m`. **No modifica** I1/I3, Kafka,
React ni las tablas y checkpoints de `desarollo`.

## Preparación real de un millón de filas

`full_a.csv.gz` es un único GZIP comprimido, leído secuencialmente. El módulo
`twitch_lakehouse.full_dataset_prepare` extrae un prefijo de **exactamente
1.000.000 de líneas físicas originales**, conservando bytes, sin deduplicar ni
descartar líneas malformadas (esa es responsabilidad de Silver). Usa lectura por
bloques para SHA256 del GZIP completo y SHA256 para cada CSV generado.

No hay que convertir a Parquet primero: Spark leerá estos archivos en paralelo
y escribirá Delta Lake en el siguiente incremento. El script **todavía no
procesa las filas con Spark** ni prueba capacidad de Databricks para 124M.

Desde PowerShell:

```powershell
cd C:\Proyectos\twitch-analytics-lakehouse
git fetch origin
git switch experimental/full-dataset-124m
$source = "C:\Datasets\Twitch\full_a.csv.gz"
Test-Path $source
python -m twitch_lakehouse.full_dataset_prepare `
  --source $source `
  --output "work\full_dataset\sample_1m" `
  --rows 1000000 `
  --rows-per-part 100000
```

**Salida:** `part-000000.csv` a `part-000009.csv` más `manifest.json`.
Un manifiesto contiene hash SHA256 y filas por parte, SHA256 del GZIP fuente,
marca UTC y bytes. El directorio se publica de forma atómica tras verificar
los fragmentos; si faltan líneas o ya existe destino, se detiene sin sobrescribir.

## Comprobación PowerShell

```powershell
$dir = "work\full_dataset\sample_1m"
$m = Get-Content "$dir\manifest.json" -Raw | ConvertFrom-Json
if ($m.extracted_rows -ne 1000000) { throw "Filas incorrectas" }
if (($m.parts | Measure-Object rows -Sum).Sum -ne 1000000) { throw "Sumatoria incorrecta" }
foreach ($p in $m.parts) {
  $file = Join-Path $dir $p.file
  if ((Get-FileHash $file -Algorithm SHA256).Hash.ToLowerInvariant() -ne $p.sha256) {
    throw "Hash incorrecto: $($p.file)"
  }
}
"PASS: $($m.extracted_rows) filas en $($m.parts.Count) fragmentos"
```

Pruebas del preparador sobre GZIP sintético: `python -m unittest
tests.test_full_dataset_prepare -v` (no necesita Spark).

**Limitaciones:** el prefijo no es muestra aleatoria y un GZIP monolítico
no se descomprime en paralelo. Crear los fragmentos **no** demuestra 124M
procesados ni garantiza que todos los datos sean válidos.

## Próximo incremento

Crear ingesta Spark/Delta en un **esquema distinto**
`workspace.twitch_full_experimental`, con conciliación Bronze–Silver–Gold,
mediciones por lote y publicación sin sobrescrituras. Después se podrá
subir progresivamente 1M → 10M → totalidad, sujetos a cuotas de Databricks.
