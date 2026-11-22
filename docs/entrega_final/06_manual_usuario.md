# 06. Manual de usuario — dos áreas claras

## Ingreso

Iniciar Docker según `07_manual_instalacion.md` y abrir `http://localhost:5173`. Si no se especifica `run_id`, empieza **Análisis de audiencia**. Enlace `http://localhost:5173/?run_id=<UUID>` abre **Laboratorio de datos** para esa ejecución.

### A. Análisis de audiencia (histórico Gold I3)

Es una **fotografía agregada** generada por Databricks y exportada mediante notebook 10. No necesita conexión activa a Databricks ni Kafka mientras exista JSON local válido.

| Elemento | Significa | No significa |
| --- | --- | --- |
| Conexiones observadas | Filas Silver válidas únicas | Número de espectadores simultáneos |
| Streamers | Nombres distintos en Gold | Usuarios globales |
| Minutos estimados | Suma de intervalos `(stop-start)*10` | Tiempo verificado de atención |
| Rechazados | Filas Bronze que no pasan el contrato | Todos los errores posibles |
| Ranking Top 20 | Streamers con más conexiones observadas | Personas con más mensajes o dinero |
| Usuarios únicos | Distintos `user_id` por streamer | Personas únicas en toda Twitch |
| Recurrentes | Usuarios con ≥2 filas con un streamer | Retención en otros días |
| Concentración Top 10/100 | Porción global de conexiones | Audiencia concurrente |
| Duración 0/(0,10]/(10,30]/(30,60]/(60,∞) | Cinco grupos excluyentes | Horas de emisiones |

Comprobar `release`, `exported_at` y `source_namespace`; para resultados nuevos ejecutar/exportar notebook 10 y sustituir archivo local **con control de versiones y evidencia**. Si la API indica que no hay exportación, no inventa ceros ni vuelve a calcular cifras.

### B. Laboratorio de datos (replay por Kafka)

Pestañas internas: **Resumen** (KPIs por run y progresión de micro-lotes); **Audiencia del replay** (Top streamers de la selección); **Procesamiento** (etapas Kafka/Spark, offsets y heartbeat globales); **Guía de demostración** (comandos y límites). Streamlit: `http://localhost:8501`.

Pasos normales: elegir un run existente o abrir enlace con UUID nuevo, observar cero real antes de enviar, iniciar productor desde PowerShell, observar 0→N y revisar la igualdad `total=valid+invalid` al terminar. El selector `Todas las ejecuciones` suma el ledger local de todos los runs; no lo confundir con Silver Databricks.

**Estados:** «WebSocket activo» significa que React recibe snapshots desde FastAPI; no garantiza Kafka saludable. Polling de respaldo permite HTTP si falla WS. «Sin conexión» conserva último resultado sin generar incrementos; «Spark sin señal reciente» depende del heartbeat del consumidor (>15 s). Los offsets globales pueden aumentar aunque el total único del run se mantenga.

## FAQ de demostración

- **¿Esto lee Twitch en directo?** No: reproducimos observaciones históricas desde CSV, pero Kafka/Spark las procesan conforme llegan al sistema ahora.
- **¿Por qué React tiene dos cantidades distintas?** Una es el dataset histórico consolidado (millones), otra la ejecución local del laboratorio (100/1.000/...); no son poblaciones comparables directamente.
- **¿Cómo sabemos que no se inventan resultados?** Métricas: Delta/SQL/quality para Gold, SQLite/ledger/API/WS para laboratorio. Sentencias y hashes en `05_pruebas_evidencias.md`.
- **¿Por qué reiniciar Spark no borra todo?** Se conservan DB, volúmenes y checkpoint. El reinicio aislado sin pérdida de estado es el escenario probado.
- **¿Puedo lanzar el replay desde React?** No; la API es de solo lectura. Se inicia desde PowerShell.
- **¿Qué pasa si falla el JSON histórico?** El análisis histórico indica indisponibilidad. Laboratorio puede seguir activo, sin mezclar resultados.
