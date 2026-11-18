# Twitch Analytics Lakehouse

Proyecto universitario para analizar interacciones y patrones de audiencia mediante
Apache Spark y arquitectura Medallón. Presentación: **18 de noviembre de 2026**.
Presupuesto: **cero pesos**. Plataforma: **Databricks Free Edition**.

## Estado real

- El equipo reportó la ejecución exitosa de `00_spark_smoke_test` en Databricks con
  `groupBy`, `count` y `sum` sobre un DataFrame **artificial**. Ese notebook no estaba
  en el repositorio inspeccionado; no se presenta como evidencia nueva de esta entrega.
- I1 prepara código batch y documentación; **no se ha cargado Twitch ni ejecutado
  este pipeline en Databricks**. No hay conteos reales ni tablas creadas por esta entrega.
- Validaciones realizadas y pendientes: [docs/validacion.md](docs/validacion.md).
- Kafka es obligatorio para el proyecto completo; su integración queda pendiente
  a cargo de Anghela. **I1 no demuestra streaming ni Kafka**.

## Alcance I1

`100k_a.csv` → Bronze (líneas y procedencia) → Silver (tipos, calidad, deduplicación
y minutos estimados) → Gold (métricas por streamer). Incluye cuarentena y reconstrucción
de Silver como tabla candidata independiente. Las escrituras iniciales fallan si existe
el destino; no hay borrado ni reemplazo automático.

## Dataset y tecnologías

La [documentación oficial de UCSD](https://cseweb.ucsd.edu/~jmcauley/datasets.html#twitch)
describe cinco campos y observaciones de usuarios conectados al chat cada diez minutos.
El subconjunto corresponde a 100 mil usuarios y aproximadamente 3 millones de interacciones;
el histórico tiene aproximadamente 124 millones de interacciones **publicadas**, no procesadas aquí.
El [directorio oficial](https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/) ofrece
`100k_a.csv` (~109 MB publicados) y `full_a.csv.gz` (~6.5 GB publicados).
Solo se usará el primero en I1, con carga manual al volumen. No se incluyen datos en Git.

Spark/PySpark procesa DataFrames; Delta Lake persiste tablas en Unity Catalog.
Apache Kafka queda para la integración posterior. No se provisionan servicios de pago.
Free Edition utiliza serverless y cuotas de uso; no se promete procesar el histórico completo.

## Organización

```text
twitch_lakehouse/   configuración, transformaciones y orquestación PySpark
notebooks/         entradas Databricks fuente .py, ordenadas 00–06
tests/             configuración, protección de escritura y fixtures artificiales
docs/              arquitectura, contrato, calidad, ejecución, rúbrica y evidencia
kafka/             espacio independiente de Anghela, pendiente
```

Configuración central: `twitch_lakehouse/config.py`; selección en `notebooks/00_config.py`.
Valores iniciales: catálogo `workspace`, esquema `twitch_lakehouse`, volumen `raw_data`.
No contiene tokens ni credenciales.

| Responsable | Responsabilidad |
| --- | --- |
| Diland | Ingeniería de datos, Spark, Medallón y ejecución de I1 |
| Anghela | Kafka, revisión de calidad y presentación |

## Ejecución y revisión

Seguir [el procedimiento Databricks](docs/ejecucion_i1.md) para importar el proyecto,
probar, crear objetos ausentes, cargar el CSV y ejecutar las capas.
Pruebas locales: `python -m unittest discover -s tests -v`.
La prueba Spark se omite explícitamente si PySpark no está instalado; el notebook 01
ejecuta las mismas comprobaciones en el Spark administrado de Databricks.

Rama: `feat/i1-spark-medallion`. Revisar antes de integrar; sin merge automático.
Ver [decisiones](docs/decisiones.md), [rúbrica provisional](docs/rubrica.md) y
[trazabilidad de IA](docs/trazabilidad_ia.md).
