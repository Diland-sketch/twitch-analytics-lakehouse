# Rama desarollo — integración de incrementos

Esta rama reúne I1/I2/I4/I5 (herencia de feat/i5-react-live-experience) e I3 (analítica avanzada). Conserva en archive/databricks-avances-20261120/ copias separadas de los notebooks del respaldo sin sobrescribir los notebooks operativos.

## Estado confirmado en la documentación
- I1: Bronze, Silver, Gold batch sobre 3.051.733 registros; validación reportada en Databricks.
- I2: Kafka local con productor/consumidor y reproducción histórica.
- I3: analítica y SQL implementados; pruebas locales aprobadas; publicación Delta y dashboard completo de dos páginas requieren nueva validación en Databricks. Diland informó que la primera página del dashboard existe, pero eso no queda verificado por el checkout Git.
- I4: Kafka -> Spark Structured Streaming -> SQLite -> Streamlit, E2E local de 100 eventos documentado.
- I5: React + FastAPI + WebSocket, E2E local de 100 eventos documentado; I5 solo lee el estado I4.

## Integración y límites

Actualización de cierre 22/11: ya existe puente **manual de JSON Gold I3 hacia
React**, con notebook 10 y lector API histórico; no es sincronización continua.
Los pendientes anteriores son históricos. Estado real, bloqueantes y prueba de
la candidata: [cierre de entrega](entrega_final/CIERRE_20261122.md).
Esta consolidación une los archivos de las ramas y preserva la historia; no equivale a una prueba integral conjunta ni implica que I3 alimente la interfaz I5. El streaming I4/I5 usa SQLite local, no Delta Lake en Databricks. Los eventos son reproducción histórica y no actividad en vivo de Twitch. Ningún proceso probado aquí maneja 124 millones de filas.

## Pendientes priorizados sin asignación de personas
1. Ejecutar suites completas en la rama consolidada y resolver cualquier regresión entre I3 e I5.
2. Publicar y validar I3 en Databricks, cerrar las dos páginas del dashboard con evidencias y criterios de aceptación.
3. Diseñar una vista única para analítica batch I3 y streaming I5, identificando origen, intervalo temporal y semántica de cada KPI sin mezclar denominadores.
4. Definir un mecanismo seguro de extracción/publicación de métricas Gold hacia la API (sin credenciales incrustadas ni exponer Databricks).
5. Evaluar el dataset completo por etapas, con pruebas de rendimiento/coste/cuotas; no afirmar que ya se procesó.
6. Probar instalación y operación reproducibles, seguridad local, calidad, reconexiones, idempotencia y recuperación, antes de una entrega final.
7. Actualizar documentación, requerimientos analíticos, diagrama de arquitectura y guion de exposición coherentes con lo realmente probado.

## Precauciones
No sobrescribir tablas Delta I1, checkpoints, estado Kafka, SQLite ni datos originales. No incorporar el respaldo encima de notebooks/ sin revisar sus diferencias. La rama main y las ramas de origen quedan intactas.
