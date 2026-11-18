# Decisiones técnicas

| ID | Decisión | Motivo / consecuencia |
| --- | --- | --- |
| ADR-001 | Batch sobre `100k_a.csv` | Demostración con presupuesto cero; histórico fuera de I1 |
| ADR-002 | Unity Catalog y volumen | Namespace explícito, sin DBFS legacy ni datos locales del notebook |
| ADR-003 | Bronze texto | Conservar registros malformados y procedencia antes de tipar |
| ADR-004 | Cinco tokens sin comillas | Supuesto del ejemplo oficial; validar con archivo cargado |
| ADR-005 | DataFrames/SQL sin UDF/RDD | Compatibilidad serverless |
| ADR-006 | `errorifexists` | Protección de datos; reintento requiere revisión o esquema aislado |
| ADR-007 | Reconstrucción candidata | Revisar antes de cualquier futura promoción |
| ADR-008 | Minutos por diferencia de pasos | Estimación explícita, sin fechas inventadas |
| ADR-009 | Kafka separado pendiente | Responsabilidad de Anghela fuera de I1 |
| ADR-010 | Sin despliegue adicional | No hacen falta infraestructura, orquestadores ni servicios nuevos |

Referencias consultadas el 17/11/2026:

- [Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations):
  serverless, cuotas y disponibilidad sin SLA. No activar trial ni upgrade.
- [Serverless](https://docs.databricks.com/aws/en/compute/serverless/limitations):
  Spark Connect, ANSI y rutas absolutas; registrar versión del workspace al ejecutar.
- [Volúmenes](https://docs.databricks.com/aws/en/volumes/volume-files): carga en Catalog
  Explorer; el subconjunto publicado cabe en el límite UI de 5 GB.

Pendientes: comprobar parser real, semántica del fin del intervalo, permisos de
catálogo/volumen y conectividad Kafka sin costo. No se garantiza el histórico completo
ni una conexión desde Databricks a un broker local.
