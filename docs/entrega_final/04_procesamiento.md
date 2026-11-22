# 04. Operación reproducible — desde cero y con estado existente

**Entorno:** Windows 11 + PowerShell + Docker Desktop/WSL 2 + Git; workspace Databricks Free Edition independiente. Antes de iniciar, comprobar `git branch --show-current` → `desarollo`, `git rev-parse HEAD`, `docker compose version`, `Test-Path $env:TWITCH_CSV`. No instalar Spark host para I4: está fijado en Docker.

## A. Databricks histórico: **solo al preparar un workspace vacío**

1. Crear Git folder con `https://github.com/Diland-sketch/twitch-analytics-lakehouse`, rama `desarollo`. En `notebooks/00_config.py` configurar `project_root` exacto con prefijo `/Workspace/`; mantener `Settings(catalog='workspace',schema='twitch_lakehouse',volume='raw_data',filename='100k_a.csv')` si coincide con permisos.
2. Conectar Serverless. Ejecutar `00_config`; esperar `namespace` y `raw_path`. Ejecutar `01_artificial_checks` → fixture ARTIFICIAL PASS.
3. Ejecutar `02_bootstrap` para crear esquema y volumen faltantes. Subir solo `100k_a.csv` desde Catalog Explorer → Volumes → `raw_data`. Registrar SHA256 local (`Get-FileHash -Algorithm SHA256 ...`).
4. Ejecutar **una sola vez**, por orden: `03_bronze` → `04_silver` → `05_gold`. Detener ante errores, tablas parciales, destinos existentes, cuotas o no conciliación. Ver `docs/ejecucion_i1.md`.
5. Ejecutar `07_i3_fixture_checks` (datos ficticios), inspeccionar fuentes y release del `08_i3_analytics`. Comprobar que los cuatro destinos `gold_i3_*_r01` estén ausentes. Publicar 08 **una sola vez**. Ejecutar `09_i3_release_preflight` → PASS.
6. Ejecutar sentencias de este dossier (`sql/`) y las seis consultas `sql/i3/` (parámetros `streamer='', top_n=10, step_from=0, step_to=2147483647, bucket_steps=6`). Registrar salidas.
7. Ejecutar `10_i3_export_snapshot.py` sobre la misma release; descargar `historical_r01.json` del volumen `historical_exports`, copiar a `audience_experience/data/historical_snapshot.json` fuera de Git. Es un **snapshot congelado**, no transmisión continua.
8. Si cambian módulos al hacer Git Pull, reiniciar Python con `dbutils.library.restartPython()` y volver a ejecutar `00_config`. **No** reejecutar notebooks 03–05 ni 08 contra destinos existentes.

**Workspace con tablas ya hechas:** comenzar en pasos 5–7 según etapa pendiente. Si 07–09 están terminados y exportación 10 ya existe, **solo comprobar SQL, PASS y el JSON**; no escribir otra vez. Para nueva release I3 usar sufijo nuevo, no overwrite. El dashboard AI/BI se valida en su cuenta por capturas y consultas sin duplicar su construcción.

## B. Docker local: estado persistente

Desde raíz del repositorio en PowerShell:

```powershell
git checkout desarollo
git pull origin desarollo
$env:TWITCH_CSV = "$env:USERPROFILE\Downloads\100k_a.csv"
if (-not (Test-Path $env:TWITCH_CSV)) { throw "Falta CSV" }
docker compose -f live_monitor/compose.yaml config --quiet
docker compose -f live_monitor/compose.yaml up -d --build kafka spark dashboard
docker compose -f audience_experience/compose.yaml up -d --build
docker compose -f live_monitor/compose.yaml ps
docker compose -f audience_experience/compose.yaml ps
```

Verificar `http://localhost:8501`, `http://localhost:5173`, `http://localhost:5173/api/v1/health`, `http://localhost:5173/api/v1/historical`. La API histórica devuelve 503 si el archivo falta o no supera validación; NO sustituir por fixture. Si `/snapshot` devuelve 503 en arranque frío, inspeccionar logs Spark, SQLite WAL y conservar montajes RO.

## C. Demostración de ingestión progresiva

```powershell
$runId = [guid]::NewGuid().ToString()
Start-Process "http://localhost:5173/?run_id=$runId"
docker compose -f live_monitor/compose.yaml run --rm --no-deps producer python3 -m live_monitor.producer --csv /input/100k_a.csv --run-id $runId --count 100 --rate 5
docker compose -f live_monitor/compose.yaml run --rm --no-deps tools python3 -m live_monitor.control status --run-id $runId
(Invoke-RestMethod "http://localhost:5173/api/v1/snapshot?run_id=$runId").metrics
```

**Antes de publicar**, el run nuevo debe estar vacío (0 real). **Durante**, mostrar 0→intermedio→100 en React y WS; después comprobar `valid + invalid = total`, `streamer_interactions_total=valid` y minutos estimados contra fuente. Productor con 100 acks no significa que Spark ya consumió 100: esperar API/SQLite.

Automatización con generación de manifiesto, comprobaciones, reenvío opcional y reinicio controlado: `scripts/validar_demo.ps1` (ver ayuda inicial y `05_pruebas_evidencias.md`).

## D. React/servicios y pruebas sin escrituras externas

```powershell
docker compose -f live_monitor/compose.yaml run --rm --no-deps -e I4_SPARK_TESTS=1 tools python3 -m unittest discover -s live_monitor/tests -v
cd audience_experience/web
npm ci
npm test
npm run build
cd ../..
```

Ejecutar E2E de navegador por separado con servicios activos y `I5_REAL_E2E` **sin configurar**, salvo que se quiera publicar otra tanda real; el test real sí crea un run nuevo. API pytest con entorno Python según `audience_experience/README.md`.

## E. Reinicio no destructivo

```powershell
docker compose -f live_monitor/compose.yaml restart spark
# Consultar el MISMO run_id y comprobar que sigue teniendo N eventos únicos.
docker compose -f live_monitor/compose.yaml logs --tail 80 spark
```

Los datos sobreviven por SQLite y checkpoint mantenidos, no porque React guarde una copia. Evitar `docker compose down -v`, `docker volume rm`, `docker system prune --volumes`, borrar SQLite/CSV/checkpoint o cambiar de broker Kafka reutilizando DB previa.

## F. Diferencia entre reproducción «en tiempo real» y feed vivo

La demostración muestra transporte y actualización **en tiempo de ejecución** de filas históricas; `--rate` es límite objetivo de emisión. Se puede observar cuándo termina cada micro-lote Spark, pero no prometer latencia fija ni calificarla como datos Twitch del presente.
