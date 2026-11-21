# Matriz de trazabilidad I3

Prefijo de tabla `workspace.twitch_lakehouse.gold_i3_`; sufijo `_r01`.
Consultas bajo `sql/i3/`; transformaciones bajo `twitch_lakehouse/analytics.py`.

| Requisito | Transformación / control | Tabla | Consulta | Gráfico / página | Prueba |
| --- | --- | --- | --- | --- | --- |
| R01 | gold I1 + audience_by_streamer; row_number SQL | streamer | 02_streamers | ranking, Visión General | alpha primero; empates por nombre |
| R02 | unique_users I1 + validate_sources | streamer | 02_streamers | tabla, ambas páginas | alpha=2; distintos globales no aditivos |
| R03 | audience_by_streamer | streamer | 02_streamers | tabla tasa, Comportamiento | alpha=1/2; beta=0/2; mismo stream cuenta |
| R04 | duration_distribution | duration | 04_duration | barras duración, Comportamiento | 0/10/30/60/70 en buckets separados; suma=5, minutos=170 |
| R05 | activity_by_step | activity | 05_activity | línea relativa, Comportamiento | inicios 0/1/2/5/10; no expansión; sumas |
| R06 | row_number y suma condicional SQL | streamer | 03_concentration | tarjetas, Visión General | Top 10/100 con 2 streamers=100%; desempate |
| R07 | unique_streams I1 + validate_sources | streamer | 02_streamers | tabla transmisiones, ambas | alpha=2 pese a 3 interacciones |
| R08 | validate_sources, validate_aggregates, assert_equal_frames, build_analytics | quality y tres agregados | 01_overview, 06_reconciliation | KPI y tabla calidad, Visión General | contrato, clave, conteos, etiquetas cambiadas con mismas sumas, destinos existentes |

Fixture compartido: `tests/analytics_checks.py` y notebook 07. Guardas:
`tests/test_analytics.py`, `tests/test_analytics_pipeline.py` y suite existente de storage.
Comparación completa persistida se ejecuta en notebook 08. SQL se prueba con
fixtures locales cuando Spark está disponible; UI/Delta/Spark Connect real
requieren completar la lista de aceptación en Databricks.
