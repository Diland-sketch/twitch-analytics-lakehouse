# Integración histórica I3 → React (sin secretos)

## Flujo de datos

Databricks (tablas Gold I3) → notebook `10_i3_export_for_react.py`
→ exportación JSON versionada en el volumen `raw_data/exports`
→ descarga manual desde Catalog Explorer
→ copia local `audience_experience/data/i3_historical_r01.json`
→ FastAPI (lectura de archivo RO, validación estricta)
→ `/api/v1/historical` → React (página «Histórico»).

Esta integración es **por snapshot**, no una conexión en tiempo real a Databricks. No mezcla los 3.051.733 registros históricos con los 100 eventos reproducidos en I4. No requiere PAT, contraseña ni credenciales de Databricks en React. Los datos exportados son **agregados**: no se incluyen IDs de usuarios ni CSV original.

## Pasos

1. Confirmar `PASS` en `09_i3_release_preflight.py`.
2. Ejecutar `notebooks/10_i3_export_for_react.py` (por defecto `release = "r01"`). El archivo de salida aparece bajo `/Volumes/workspace/twitch_lakehouse/raw_data/exports/i3_historical_r01.json`. El notebook **rechaza sobrescribir** un archivo existente.
3. Descargar el JSON desde Catalog Explorer y guardarlo **fuera de Git**. Crear carpeta local `audience_experience/data` y copiar allí el archivo, conservando nombre exacto `i3_historical_r01.json`. Comprobar que no se descargó como .txt.
4. Desde raíz del repo en PowerShell:
```powershell
git pull origin desarollo
docker compose -f audience_experience/compose.yaml up -d --build
```
5. Abrir `http://localhost:5173/api/v1/historical`. Si el archivo falta: HTTP 404 `historical_export_missing`. Si es inválido: HTTP 422 `historical_export_invalid`. Si está presente y es válido, retorna los agregados I3.
6. Abrir `http://localhost:5173` y seleccionar **Histórico** en navegación. Los eventos procesados siguen en las vistas del monitor original; no se suman.

## Límites y precauciones

- No añadir el JSON exportado ni archivos CSV a commits. El directorio está excluido con `.gitignore`.
- La exportación muestra Top 20 streamers y hasta 120 intervalos de actividad histórica agregada de 60 minutos; si se recortan, se declara `activity_truncated`. No afirmar que la serie contiene todo el histórico.
- La recurrencia se define como usuarios con al menos dos conexiones observadas con un streamer, no retención entre transmisiones. Las ventanas son tiempos **relativos**, no fechas calendáricas.
- La API nunca escribe al volumen SQLite, ni modifica Gold/Delta ni ejecuta consultas Databricks.
- Si el notebook 10 falla, conservar el diagnóstico; no reemplazar el archivo exportado ni modificar tablas originales.
