# I5 — Twitch Audience Live Experience

React + TypeScript + Vite, Tailwind CSS + componentes shadcn/ui, Apache ECharts,
FastAPI y WebSocket, con recuperación por polling. Una nueva capa visual sobre
la prueba funcional I4: no reemplaza Spark, Kafka, productor, SQLite o Streamlit.

## Qué se reutiliza y qué permanece intacto

Base `feat/i4-twitch-live-monitor` (`5afba7e`). Rama I5 independiente.
`live_monitor.storage.snapshot(path, run_id)` conserva lectura SQLite mode=ro,
BEGIN/rollback y consistencia entre widgets. Sin migraciones, índices, tablas
nuevas, modificaciones I1–I4 o acceso a Databricks. La API no publica comandos
para arrancar productor ni controlar Spark. Streamlit sigue en localhost:8501.

Auditoría y propuesta previa: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Contrato API: [docs/API.md](docs/API.md). Evidencia: [docs/VALIDATION.md](docs/VALIDATION.md).

## Ejecutar localmente

Requiere Docker Linux activo, I4 previamente disponible y el volumen existente
`twitch-live-i4_monitor_state` (no se crea ni reemplaza automáticamente). Desde
la raíz del repositorio:

```powershell
$env:TWITCH_CSV = 'C:/Users/TU_USUARIO/Downloads/100k_a.csv'
docker compose -f live_monitor/compose.yaml start kafka spark dashboard
docker compose -f audience_experience/compose.yaml up -d --build
```

Abrir [Audience Live Experience](http://localhost:5173).
Solo se publica 127.0.0.1:5173. FastAPI no publica puerto host: Nginx sirve React
y hace proxy same-origin de HTTP/WS. Sin CORS abierto, hosting cloud ni servicios
de pago. API root filesystem read_only, volumen I4 RO y conexión SQLite mode=ro.
Si el volumen tiene otro nombre, establecer `I4_STATE_VOLUME` explícitamente.
No ejecutar `down -v`, borrar estado ni cambiar el nombre para eludir un error.

SQLite WAL requiere que sus auxiliares -wal/-shm existan y sean legibles en un
montaje RO. La API mantiene una conexión RO sin transacción para su ciclo de
vida; los snapshots usan lecturas breves. Si I4 está completamente frío y faltan
auxiliares, puede mostrar espera hasta que el escritor original procese un lote.
Se devuelve 503 y se reintenta: no se usa immutable, journal_mode ni copia de
archivos activos. No habilitar escritura del volumen como arreglo automático.

## Sustentación: una ejecución de 0 a 100

Preparar un UUID real y abrirlo antes de publicar:

```powershell
$runId = [guid]::NewGuid().ToString()
"http://localhost:5173/?run_id=$runId"
docker compose -f live_monitor/compose.yaml run --rm --no-deps producer python3 -m live_monitor.producer --csv /input/100k_a.csv --run-id $runId --count 100 --rate 5
```

La selección nueva devuelve cero porque no tiene filas; después muestra solo
eventos confirmados por Spark para ese run_id. Mostrar Visión general, explorar
Audiencia y abrir Procesamiento para verificar lote y offsets. Un run_id nuevo
representa otra reproducción; no elimina resultados previos. Los minutos son
estimación por intervalos de chat, no tiempo exacto visto. La fecha del gráfico
es procesamiento real del replay, no calendario histórico de Twitch.

Para detener, usar el comando I4 en otra terminal:

```powershell
docker compose -f live_monitor/compose.yaml run --rm --no-deps tools python3 -m live_monitor.control stop --run-id $runId
```

Las reglas de reintento, resume y checkpoint permanecen en live_monitor/README.md.
No detener o reiniciar I4 para actualizar solamente la interfaz I5.
Apagar solo I5: `docker compose -f audience_experience/compose.yaml stop`.

## Estados y actualizaciones

WebSocket transmite snapshots completos de SQLite cada 2s. Nunca suma deltas en
React ni incrementa contadores por reloj. ECharts interpola cambios entre datos
recibidos; KPI conserva el entero exacto. Anima solo si cambia la opción y respeta
prefers-reduced-motion. Revisión y observed_at impiden aceptar datos atrasados;
al cambiar run_id se cancelan conexiones y respuestas anteriores.

Si WS falla, polling cada 2s y reconexión con espera 1..10s. Si API también falla,
mantiene último resultado con aviso; sin un resultado previo muestra «—», no
ceros ficticios. Carga, vacío real y almacenamiento indisponible son distintos.
Conexión UI activa no implica Spark sano: heartbeat >15s se marca sin señal.
Último micro-lote, offsets y consumidor son globales; métricas/gráficos son de
la selección. No promediar usuarios ni inventar retención: no son métricas I5.

## Desarrollo y pruebas

Python aislado en `audience_experience/.venv`:

```powershell
python -m venv audience_experience/.venv
audience_experience/.venv/Scripts/python -m pip install -r audience_experience/api/requirements.txt
audience_experience/.venv/Scripts/python -m pytest audience_experience/tests -q
```

Frontend desde audience_experience/web:

```powershell
npm ci
npm test
npm run build
npm run test:e2e
```

Playwright usa Chrome ya instalado, sin descargar otro navegador. `PW_CHANNEL`
puede cambiarse a msedge. Las pruebas de navegador usan servicios I5 disponibles
en 127.0.0.1:5173, o `I5_BASE_URL`. Prueba de protocolo puede interrumpir WS o
HTTP en el navegador de test, sin tocar Spark ni datos. Fixtures unitarios son
ARTIFICIALES y no se importan en el producto.

Para la prueba E2E con eventos reales, activar explícitamente en esa carpeta:

```powershell
$env:I5_REAL_E2E = '1'
$env:TWITCH_CSV = 'C:/Users/TU_USUARIO/Downloads/100k_a.csv'
npm run test:e2e
```

Usa productor I4 original, nuevo run_id y primeras 100 filas reales. Exige 100
acks, identidades según hash/ordinal, métricas API=Spark=100, minutos conciliados,
UI 0→parcial→100 y frames WS con 0 y 100. Guarda capturas antes/después, logs,
snapshots y reporte bajo `work/i5-evidence/<run_id>/`, ignorado por Git.
La ejecución real no está activada por defecto: evitar nuevos replays accidentales.

Diseño responsive probado en 1440 y 390 px, con navegación móvil, teclado,
estados de conexión y respaldo Streamlit. Los componentes UI provienen del
registro oficial shadcn/ui; ECharts usa módulos de línea/barra y renderer canvas.
Límite: el lector I4 original materializa actividad; I5 renderiza últimos 300
puntos y Top 20. No se afirma escalabilidad a 124 millones.


## Navegación unificada de dos áreas (rama `desarollo`)

El menú principal presenta solo **Análisis de audiencia** y **Laboratorio de datos**.
La primera área consume el JSON Gold I3 exportado desde Databricks (ver
`docs/integracion_gold_react.md`) y explica conexiones, recurrencia, minutos,
ranking y distribución. No necesita Kafka disponible y no muestra estado de
WebSocket, selector `run_id` ni indicadores de Spark.

El laboratorio incluye pestañas **Resumen**, **Audiencia del replay**,
**Procesamiento** y **Guía de demostración**, además del acceso a Streamlit.
Reutiliza exactamente los snapshots I4, conserva filtros por `run_id`,
WebSocket/polling y estados de recuperación. Al cambiar al histórico se cierra
el WebSocket del laboratorio; al regresar se conecta de nuevo.

Un enlace con `?run_id=UUID` abre directamente el laboratorio para conservar
las demostraciones compartidas. Sin `run_id`, la pantalla inicial es el
análisis de audiencia. Ningún resultado batch se suma al replay local.

### Actualizar en Windows

Desde la raíz del repositorio:

```powershell
git checkout desarollo
git pull origin desarollo
docker compose -f audience_experience/compose.yaml up -d --build
```

La API histórica necesita que siga existiendo
`audience_experience/data/historical_snapshot.json` en el computador.
La ruta se monta de solo lectura en Docker. No es necesario recrear Kafka ni
volver a exportar I3 cuando el JSON sigue vigente.

**Evidencia previa proporcionada por el usuario:** prueba local de 1000
eventos válidos, ninguno inválido, 1000 únicos conservados tras un reinicio
controlado de Spark. No es una prueba de pérdida de Kafka ni de alto volumen;
no se ejecutó desde CI.
