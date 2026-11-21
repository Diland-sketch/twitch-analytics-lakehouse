# Puesta en marcha I3 en la rama `desarollo`

## Estado verificado

La rama `desarollo` incorpora I3, I1–I2 y I4–I5. El CI del 21/11/2026 aprobó 22 pruebas Spark I1/I3 sobre fixtures artificiales; no constituye ejecución de las tablas I3 en el workspace real. Las tablas Gold ampliadas y el dashboard Databricks siguen **pendientes de comprobación in situ**.

## Secuencia segura para Databricks Free Edition

1. En Git folders, seleccionar rama `desarollo`, hacer Pull y **reiniciar Python** con `dbutils.library.restartPython()` en celda separada. Reiniciar antes de importar módulos de la nueva rama.
2. Configurar `notebooks/00_config.py` con ruta del proyecto y esquema existentes. No ejecutar nuevamente el pipeline I1 (notebooks 02–06) ni borrar tablas.
3. Ejecutar `notebooks/07_i3_fixture_checks.py`: debe dar PASS con datos artificiales.
4. Inspeccionar `notebooks/08_i3_analytics.py`, especialmente `release = "r01"`. Comprobar que **ninguna** de las cuatro tablas `gold_i3_*_r01` exista. Si existen, no reejecutar: usar una nueva release válida después de revisar el estado.
5. Ejecutar **una sola vez** la última celda del notebook 08. Esperar la publicación de las tres familias Gold y, al final, una fila quality PASS. Guardar salida y versiones Delta.
6. Ejecutar `notebooks/09_i3_release_preflight.py` con la misma release. Valida **sin escrituras** presencia de las cuatro tablas, una sola fila quality PASS, namespace/versiones, conciliación Bronze/Silver y las tres familias de agregados. Conservar evidencia del resultado.
7. Ejecutar los seis archivos `sql/i3/*.sql`, ajustando namespace/release donde corresponda; validar primero overview y reconciliation. Exigir exactamente una fila quality PASS y todos los deltas cero. Registrar las salidas.
8. En Databricks AI/BI Dashboards, completar las **dos páginas** según `docs/i3_dashboard.md` (Visión General y Comportamiento de Audiencia), con los seis datasets SQL y los parámetros documentados. Verificar Top N, búsquedas de streamer, recurrencia, buckets de duración, intervalos relativos y ausencia de observaciones. Dejar como borrador hasta revisar capturas y valores.

## Evidencia mínima para cerrar I3

- Commit / rama y versión de Spark del entorno.
- Salida PASS del notebook 07 (fixtures), salida de publicación notebook 08 y salida del preflight 09.
- `DESCRIBE DETAIL` y `DESCRIBE HISTORY` de las cuatro nuevas tablas con release seleccionada.
- Resultados SQL de overview/reconciliation sin deltas.
- Capturas de ambas páginas con filtros globales y por streamer.
- Capturas de prueba de selección inexistente, Top 10/100 y rango temporal acotado.

**Criterio de cierre:** indicadores y controles comprobados en tablas reales, dashboard de dos páginas revisado visualmente. No llamar éxito a un notebook con fixtures ni a una propuesta de dashboard. No intentar recrear ni sobrescribir tablas existentes.

## Precaución semántica

Las conexiones son presencia observada en el chat de Twitch, **no** espectadores activos demostrados. Los minutos proceden de pasos de 10 minutos. `time_start` es relativo, no fecha calendario. La recurrencia I3 significa dos o más filas del mismo usuario con un streamer, incluso dentro de la misma transmisión: **no** retención entre sesiones.

## Acceso y ejecución

Este documento y el preflight se prepararon desde GitHub. Su existencia no implica acceso remoto del asistente al workspace ni que los notebooks 08/09 se hayan ejecutado sobre Delta real.
