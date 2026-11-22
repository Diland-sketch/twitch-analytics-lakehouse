# Twitch Analytics Lakehouse

Proyecto académico de Ingeniería de Sistemas: arquitectura **Medallion Bronze/Silver/Gold en Databricks**, analítica avanzada I3, reproducción de eventos históricos con **Kafka + Spark Structured Streaming + SQLite** y aplicación **React + FastAPI**.

**Rama de trabajo:** `desarollo` (nombre literal). Este repositorio **no recibe actividad actual de la API Twitch**: demuestra procesamiento incremental de datos históricos.

## Guía oficial para ejecutar, verificar y defender el sistema

**[ENTREGA FINAL — documentación integral y reproducible](docs/entrega_final/README.md)**.

Incluye ocho documentos, siete consultas SQL auditables, script PowerShell con UUID/CSV hash/evidencias, diagramas, criterios de aceptación, manual de instalación/uso y guion de demostración.

### Dos rutas, dos poblaciones

| Ruta | Tecnologías | Resultado | Frecuencia |
| --- | --- | --- | --- |
| **Histórica/batch** | Databricks Free Edition, PySpark, Delta Lake I1/I3 | Gold/quality, dashboard Databricks, JSON agregado consumido por React | Carga y exportación explícitas, **no** sincronización en vivo |
| **Laboratorio/replay** | Docker, Kafka 3.9.1, Spark 3.5.8, SQLite WAL, FastAPI/React | Eventos únicos, válidos, inválidos, lotes, streamers por `run_id` | Procesamiento progresivo al publicar CSV histórico |

La aplicación React (localhost:5173) tiene **dos secciones principales**: Análisis de audiencia y Laboratorio de datos (resumen, audiencia de replay, procesamiento y guía). Streamlit local disponible en localhost:8501.

### Arranque rápido desde Windows

```powershell
git clone --branch desarollo https://github.com/Diland-sketch/twitch-analytics-lakehouse.git
cd twitch-analytics-lakehouse
$env:TWITCH_CSV = "$env:USERPROFILE\Downloads\100k_a.csv"
docker compose -f live_monitor/compose.yaml up -d --build kafka spark dashboard
# Archivo JSON Gold I3 obtenido previamente mediante notebook 10:
New-Item -ItemType Directory -Force audience_experience/data | Out-Null
Copy-Item "$env:USERPROFILE\Downloads\historical_r01.json" "audience_experience/data/historical_snapshot.json"
docker compose -f audience_experience/compose.yaml up -d --build
```

Para probar con evidencia:

```powershell
.\docs\entrega_final\scripts\validar_demo.ps1 -Count 100 -Rate 5 -RestartSpark -Reenviar
```

Este script **publica 100 filas reales del CSV local** con run nuevo, no utiliza fixtures, deja pruebas en `work/entrega_final/` y no borra volúmenes ni checkpoints. Requiere Docker y API funcionando. **No ejecutarlo automáticamente en cada build**. Para ejecución con 1.000 filas `-Count 1000 -Rate 20`.

### Indicadores y límites

El equipo documentó para `100k_a.csv`: 3.051.733 conexiones observadas al chat (Bronze/Silver), 162.625 streamers, 95.887.110 minutos estimados y cero rechazos bajo las reglas implementadas. El usuario confirmó localmente 1.000 eventos únicos, 1.000 válidos y 1.000 conservados tras reiniciar Spark. Son evidencias de contextos distintos; las sentencias de `docs/entrega_final/sql/` sirven para **corroborar en el workspace real**, no sustituyen las ejecuciones.

Nunca describir estas filas como espectadores activos, mensajes, retención intersesión, datos actuales de Twitch o prueba de 124 millones procesados.

## Historial técnico del proyecto

Los avances y decisiones originales se conservan en:
- [I1 arquitectura](docs/arquitectura.md), [pipeline Databricks](docs/ejecucion_i1.md), [calidad](docs/calidad.md), [validación](docs/validacion.md).
- [I3 requerimientos](docs/i3_requerimientos.md), [Gold ampliado](docs/i3_modelo_gold.md), [operación](docs/i3_puesta_en_marcha_desarollo.md).
- [I4 monitor](live_monitor/README.md), [I4 validación](live_monitor/VALIDATION.md).
- [I5 experiencia](audience_experience/README.md), [I5 validación](audience_experience/docs/VALIDATION.md), [Gold→React](docs/integracion_gold_react.md).

Los documentos históricos registran el estado de sus incrementos **en su fecha**. Para el estado actual de la rama integrada, prevalece el dossier `docs/entrega_final`, sin borrar el historial previo.

No subir CSV ni JSON real, SQLite, checkpoints, credenciales o logs originales a Git. No utilizar `docker compose down -v`, no sobrescribir Delta ni modificar/borrar checkpoints para repetir una prueba.
