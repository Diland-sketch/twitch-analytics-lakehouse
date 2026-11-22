# Exportación Gold I3 hacia React — procedimiento único

La integración consolidada en `desarollo` usa exclusivamente el contrato
`i3-historical-v1` y el notebook **`notebooks/10_i3_export_snapshot.py`**.
La fuente es Gold I3 de Databricks; no se realizan consultas del navegador a Databricks.

## En Databricks

1. Confirmar `PASS` en `09_i3_release_preflight.py` para la release `r01`.
2. Ejecutar `10_i3_export_snapshot.py` una sola vez sobre esa release.
3. Descargar `historical_r01.json` desde Catalog Explorer:
   `workspace.twitch_lakehouse.historical_exports`.
   El notebook no sobreescribe exportaciones existentes.

## En Windows

Desde la raíz del repositorio, en PowerShell:

```powershell
git pull origin desarollo
New-Item -ItemType Directory -Force audience_experience/data | Out-Null
Copy-Item "$env:USERPROFILE\Downloads\historical_r01.json" "audience_experience/data/historical_snapshot.json"
docker compose -f audience_experience/compose.yaml up -d --build
```

Comprobar `http://localhost:5173/api/v1/historical` y seleccionar
**Análisis histórico** en la navegación de React. Sin archivo válido la API
devuelve HTTP 503 `historical_unavailable`; no deben mostrarse datos ficticios.

## Contrato y alcance

La exportación contiene solamente KPIs agregados, concentración Top 10/100,
Top 20 streamers y cinco buckets de duración. No incluye los eventos originales,
el CSV, identificadores de usuarios ni series de actividad temporal.
La actividad detallada se consulta en el dashboard Databricks del equipo.
La recurrencia describe 2+ filas Silver de un usuario por streamer, no
retención entre transmisiones.

El sistema I4 (Kafka → Spark → SQLite) permanece independiente. Sus eventos
reproducidos no se suman con la población histórica I3. No hay sincronización
automática en vivo con Databricks ni se necesitan tokens en React.

El directorio `audience_experience/data` ignora los JSON en Git.
No versionar el archivo real ni contraseñas.
