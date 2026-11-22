# Conexión del Lakehouse Gold I3 con la aplicación React

La integración es mediante **exportación estática, verificable y manual**. No habilita tokens de Databricks ni consultas remotas desde el navegador. La API FastAPI mantiene acceso de solo lectura a SQLite I4 y al JSON I3 por separado. No mezcla KPIs ni rangos del flujo batch con los del replay histórico.

## En tu workspace Databricks

1. Abrir Git folder en la rama `desarollo` y actualizarla; reiniciar Python tras incorporar cambios de módulos.
2. Comprobar las tablas I3 y el PASS del notebook `09_i3_release_preflight.py`.
3. Ejecutar **`notebooks/10_i3_export_snapshot.py`**, release `r01` o la que validaste. Produce un JSON pequeño con KPIs Gold, Top 20, concentración Top 10/100 y 5 intervalos de duración.
4. El notebook crea un volumen independiente `workspace.twitch_lakehouse.historical_exports` si falta y escribe `historical_r01.json` sin sobrescribir archivos. Desde **Catalog Explorer → Volumes → historical_exports** descargar ese JSON.

## En Windows (raíz del repositorio)

```powershell
git pull origin desarollo
New-Item -ItemType Directory -Force audience_experience/data | Out-Null
Copy-Item "$env:USERPROFILE\Downloads\historical_r01.json" "audience_experience/data/historical_snapshot.json"
docker compose -f audience_experience/compose.yaml up -d --build
```

Después abrir `http://localhost:5173/api/v1/historical` y la página **Histórico Gold I3** en React. Las cifras mostradas provienen del JSON, no de fixtures ni de una solicitud continua a Databricks. Cuando cambie la versión de Gold, repetir validación, exportación y copia del archivo; no sustituir snapshots sin verificar fuente y release.

## Privacidad y seguridad

Solo se exportan agregados e identificadores públicos de streamers del ranking; **no** las conexiones individuales ni los `user_id` originales. El directorio `audience_experience/data/` queda excluido de Git por el `.gitignore` del proyecto. No versionar archivos reales ni credenciales.

## Interpretación

`silver_rows`: conexiones observadas al chat, no visualizaciones activas. `estimated_minutes`: intervalo estimado de permanencia en chat, no horas vistas verificadas. Recurrencia: un usuario con al menos dos filas del mismo streamer, no retención en transmisiones distintas. El monitor Kafka I4 representa eventos reproducidos localmente; Gold I3 representa la población histórica completa que se haya publicado en Databricks.

## Evidencia y límites

El CI verifica lector y contratos con fixtures. **Hasta descargar el JSON real, el endpoint devuelve 503**, y React debe indicar que falta la exportación; no inventa datos ni usa 0 como sustituto. Esta conexión no prueba sincronización automática en vivo ni procesamiento de 124 millones de registros.
