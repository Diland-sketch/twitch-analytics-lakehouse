# Diseño funcional: Twitch Audience Intelligence

Estado: propuesta implementable con SQL; **no construido ni validado en Databricks**.
Texto permanente: «Conexiones observadas al chat. No son espectadores activos
comprobados. Duración estimada por pasos de 10 minutos. Solo subconjunto I1.»
Mostrar release y población del marcador quality en ambas páginas.

## Página 1: Visión General

| Posición | Widget / dataset | Configuración |
| --- | --- | --- |
| Cabecera | Nombre, alcance y release / overview | sin fechas ficticias; PASS y población procesada |
| Fila 1 | KPI conexiones, streamers, minutos estimados / overview | enteros con separador; nunca llamar «tiempo visto» |
| Fila 2 izquierda | Ranking horizontal / streamers | y=streamer, x=interactions; orden position asc; Top N default 10 |
| Fila 2 derecha | Tabla detalle / streamers | position, streamer, interactions, unique_users, unique_streams; sin totales de distintos |
| Fila 3 | Tarjetas Top 10 y Top 100 / concentration | top10_share y top100_share en %; denominador global visible |
| Pie | Calidad / reconciliation | rechazos, duplicados inferidos I1, clave Silver duplicada, todos los deltas; color verde solo si PASS y deltas cero |

Controles: Top N numérico y streamer de texto exacto (vacío=todos). Si streamer
tiene valor se muestra su posición global aunque esté fuera del Top N. R06 y
calidad no reciben el filtro: título «Global, subconjunto completo».

## Página 2: Comportamiento de Audiencia

| Posición | Widget / dataset | Configuración |
| --- | --- | --- |
| Cabecera | Streamer exacto, definición de recurrencia | usar el mismo parámetro streamer en streamers, duration y activity |
| Fila 1 | Tabla por streamer / streamers | unique_users, recurring_users, recurrence_rate (%), unique_streams; no promediar tasas |
| Fila 2 izquierda | Barras de duración / duration | x=label orden duration_bucket, y=interactions; tooltip interaction_share y estimated_minutes |
| Fila 2 derecha | Línea de inicios / activity | x=relative_minutes numérico continuo, y=connection_starts; no eje fecha |
| Pie | Nota de interpretación | «Recurrencia: >=2 interacciones por usuario con el streamer; puede ser en una misma transmisión. No mide retención entre sesiones.» |

Controles de actividad: step_from y step_to (enteros, inclusivos), bucket_steps
(entero positivo; default 6 equivale a 60 minutos relativos). El rango aplica
solo a los inicios de R05; los demás widgets usan toda la publicación. Si los
límites cortan un bucket, el primer/último punto es parcial: mostrar esta nota.
No interpolar huecos como observaciones cero ni afirmar ocupación concurrente.
Sin streamer, la tabla de recurrencia muestra Top N streamers, mientras duración
y actividad son globales; títulos deben indicar esos alcances diferentes.

## Configuración y prueba funcional

Crear seis datasets, uno por archivo SQL. Parámetros tipados según comentarios;
conectar el control streamer a los tres datasets que lo usan. No añadir filtros
de campo que recalculen denominadores de distribuciones o concentración.
Configurar ratios con formato porcentaje, no multiplicarlos de nuevo por 100.
Solo usar release cuyo marcador tenga exactamente una fila PASS y conciliación
cero; SQL no bloquea por sí solo una selección incorrecta de release.

Probar: vacío/todos; streamer existente fuera del Top 10; inexistente; Top N 10 y
100; rango completo y acotado de pasos; paso 1 y 6; todos los buckets, incluido
cero. Revisar SQL ejecutado y exportar salidas/capturas del borrador. Guardar
como borrador hasta validar; publicación a usuarios queda fuera de esta entrega.

Fuentes oficiales consultadas: [parámetros](https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters),
[datasets](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/datasets),
[visualizaciones](https://docs.databricks.com/aws/en/dashboards/manage/visualizations/).
Las opciones concretas del workspace Free Edition deberán verificarse allí.
