# Validación de I1

## Estado de ejecución

El equipo reporta un smoke test Spark artificial exitoso previo; no corresponde a
esta suite ni al dataset Twitch. Esta entrega no ha ejecutado notebooks en Databricks.

Entorno local observado: Python 3.13.4, Java 22.0.2, sin PySpark ni pytest instalados.
La suite usa unittest de la biblioteca estándar. No se instala un motor Spark nuevo
ni se modifica el runtime Databricks desde este repositorio.

## Comprobaciones locales

- `python -m unittest discover -s tests -v`: 7 pruebas aprobadas y 1 omitida
  explícitamente porque no está instalado PySpark. Se verificaron configuración,
  rechazo de rutas/identificadores inválidos, exclusión del histórico y protecciones
  de publicación con dobles de prueba (no procesamiento Spark).
- `python -m compileall -q twitch_lakehouse tests notebooks`: compilación sin errores
  de sintaxis; esto no valida la resolución de expresiones Spark ni el runtime Databricks.
- `git diff --check`: sin errores de espacios en el diff.
- No se instalaron dependencias nuevas ni se descargó el dataset.

## Verificaciones que requieren Databricks

- Ejecutar `01_artificial_checks`: fixture artificial, casos de calidad, deduplicación,
  conciliación Gold y reproducibilidad de transformación; sin tablas persistidas.
- Confirmar permisos y bootstrap de esquema/volumen con `02_bootstrap`.
- Cargar y verificar el CSV real; ejecutar 03–05 y conservar sus métricas.
- Verificar bloqueo ante destino existente sin cambios en la tabla previa.
- Ejecutar vista previa de 06 y, opcionalmente, crear candidata con sufijo nuevo;
  comprobar que Silver original permanece y Gold sigue apuntando a ella.

No se dispone todavía de resultados de estas verificaciones. Spark/Delta, permisos,
widgets/importaciones y cuotas del workspace requieren comprobación real.
