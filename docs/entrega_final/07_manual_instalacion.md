# 07. Instalación, reproducibilidad, fallos y protección

## Matriz mínima de componentes

- Windows 11, PowerShell, Docker Desktop con contenedores Linux y WSL2 habilitado.
- Git para clonar `https://github.com/Diland-sketch/twitch-analytics-lakehouse` rama **`desarollo`**.
- Node.js 22/npm para desarrollo web local (`npm ci`, `npm test`, `npm run build`). Docker compila frontend sin requerir npm del host.
- Python 3.11 opcional para scripts y pruebas fuera de Docker, no necesario para iniciar I4.
- CSV `100k_a.csv` **real y externo al repositorio**, archivo sin encabezado. Fuente oficial: https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/
- Para batch I1/I3: cuenta Databricks Free Edition con acceso Serverless, Git folder y permisos de esquema/volumen/tablas; **no** es obligatorio para la demo local ya exportada.

## Puesta en marcha en una nueva máquina

```powershell
git clone --branch desarollo https://github.com/Diland-sketch/twitch-analytics-lakehouse.git
cd twitch-analytics-lakehouse
git rev-parse HEAD
$env:TWITCH_CSV = "$env:USERPROFILE\Downloads\100k_a.csv"
Test-Path $env:TWITCH_CSV
Get-FileHash -Algorithm SHA256 $env:TWITCH_CSV
docker info
docker compose -f live_monitor/compose.yaml config --quiet
docker compose -f live_monitor/compose.yaml up -d --build kafka spark dashboard
```

**Orden importante:** el volumen `twitch-live-i4_monitor_state` debe existir antes de iniciar `audience_experience/compose.yaml`; el Compose I5 lo trata como externo y lo monta `:ro`. Para Gold histórico, exportar JSON I3 desde Databricks o copiar un JSON legítimo validado (nunca fixture):

```powershell
New-Item -ItemType Directory -Force audience_experience/data | Out-Null
Copy-Item "$env:USERPROFILE\Downloads\historical_r01.json" "audience_experience/data/historical_snapshot.json"
docker compose -f audience_experience/compose.yaml up -d --build
docker compose -f live_monitor/compose.yaml ps
docker compose -f audience_experience/compose.yaml ps
Invoke-RestMethod http://localhost:5173/api/v1/health
Invoke-RestMethod http://localhost:5173/api/v1/historical
```

**Cuidado:** el archivo exportado desde Databricks se llama `historical_r01.json` y en el host se renombra a `historical_snapshot.json`; ambos deben corresponder a la misma release. Si usaste otra release, actualizar nombre del archivo fuente. API 503 no equivale a Gold vacío.

## Diagnóstico seguro

| Síntoma | Prueba | Acción segura |
| --- | --- | --- |
| Docker CLI existe pero sin Engine | `docker info` | Abrir Docker Desktop/WSL2 |
| Kafka no saludable | `docker compose -f live_monitor/compose.yaml ps` y `logs kafka` | Inspeccionar logs; no eliminar broker |
| Spark no procesa | `logs --tail 100 spark` | Revisar conector, checkpoint, Kafka; no recrear volúmenes |
| Streamlit carga, React no | `docker compose -f audience_experience/compose.yaml ps` | Inspeccionar `logs web api`, puerto 5173, posible `npm run dev` en conflicto |
| `GET /snapshot` 503 en frío | `logs spark api` | Esperar primer micro-lote que cree auxiliares WAL; conservar `:ro` |
| `GET /historical` 503 | `Test-Path audience_experience/data/historical_snapshot.json` | Revisar export notebook 10, schema/release, montaje `:ro` |
| `run_id` no aumenta | `control status --run-id` | Verificar UUID/CSV/rango, acks, logs Spark y filtro |
| Reintento de I1/I3 rechazado | `SHOW TABLES`, `DESCRIBE HISTORY` | Diagnóstico o esquema/release nuevos, **no** overwrite |
| Databricks importa módulo antiguo | `print(twitch_lakehouse.analytics.__file__)` | `dbutils.library.restartPython()` tras Pull y ejecutar 00_config |
| Agotamiento Free Edition | Salida del notebook | Registrar cuota; no pagar para ocultarlo |

## Parada y reinicio sin pérdida

```powershell
docker compose -f audience_experience/compose.yaml stop
docker compose -f live_monitor/compose.yaml stop spark dashboard kafka
# Para reanudar (si topic, estado y broker siguen consistentes):
docker compose -f live_monitor/compose.yaml start kafka spark dashboard
docker compose -f audience_experience/compose.yaml start
```

No ejecutar `down -v`, `docker volume prune`, `docker system prune --volumes` ni tocar manualmente WAL/checkpoint con servicios activos. No construir respaldo copiando solo SQLite mientras su WAL está abierto.

## Versiones y evidencia

En cada repetición registrar: `git rev-parse HEAD`, `git status --short`, `docker version`, `docker compose version`, `Get-FileHash $env:TWITCH_CSV`; en Databricks `spark.version`, release y `DESCRIBE DETAIL/HISTORY`. Los documentos de validación de I4/I5 corresponden a sistemas concretos de noviembre de 2026: no se presupone idéntica imagen/digest en todos los hosts.
