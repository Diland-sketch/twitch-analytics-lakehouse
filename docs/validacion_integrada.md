# Validación integrada — rama `desarollo`

La evidencia del 21/11 se conserva sin alteraciones. Para la candidata del 22/11,
pruebas ejecutadas hoy y pendientes reales, consultar [cierre](entrega_final/CIERRE_20261122.md).

## Evidencia comprobada el 21 de noviembre de 2026

GitHub Actions: [ejecución 44958178931](https://github.com/Diland-sketch/twitch-analytics-lakehouse/actions/runs/44958178931), commit `fd13944b814de81d6654254f8bd6bd4e3a6b9d99`, resultado global **success**.

| Trabajo | Resultado y alcance |
| --- | --- |
| Lakehouse I1/I3 (Spark local) | **22 pruebas PASS**, cero omisiones, sobre PySpark 4.0.1 / Java 17. Incluye fixtures artificiales, SQL, validación y guardas de escritura. |
| I4 contratos locales | **10 pruebas PASS**, **una omitida** explícitamente: prueba Spark que requiere imagen Docker I4. No ejecutó Kafka ni Spark I4. |
| API I5 | **8 pruebas PASS**, 1 warning, sobre fixtures SQLite. |
| React I5 | **8 pruebas unitarias PASS** y `npm run build` exitoso. |

**En total: 48 pruebas completadas y aprobadas; una prueba I4 omitida por diseño.** La compilación de React es una comprobación adicional. Se inspeccionaron los resultados de los jobs y sus logs, no se ejecutó el sistema contra datos reales desde esta validación. La ejecución I4/I5 E2E con Kafka-Spark descrita en sus documentos de validación corresponde a las pruebas anteriores del equipo en sus ramas originales.

## Resultado de integración

No se identificaron regresiones en los contratos comprobados después de incorporar I3 en la rama I5. No fue necesario corregir los módulos productivos para superar esta suite.

## Alcance pendiente

1. Correr la prueba I4 Spark con `I4_SPARK_TESTS=1` dentro de su imagen Docker fijada, y verificar Kafka → Spark → SQLite → UI con una nueva ejecución controlada.
2. Validar conjuntamente I3 sobre tablas Delta reales en Databricks y completar el dashboard de dos páginas.
3. Demostrar los límites entre batch Delta/Databricks y monitor SQLite local; todavía no hay transporte de métricas I3 al frontend React.
4. Validar rendimiento y recursos a mayor escala. Estas pruebas no demuestran operación a 124 millones de registros.

**No marcar como completada la prueba integral de infraestructura de todos los servicios.** El estado de este documento es *suite de regresión modular aprobada*.
