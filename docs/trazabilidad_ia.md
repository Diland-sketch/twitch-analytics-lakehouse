# Trazabilidad de asistencia de IA

Fecha de trabajo: 17/11/2026 (America/Bogota). Asistente: Codex. Responsable humano
de revisión técnica: Diland; revisión de Kafka/calidad/presentación: Anghela.

## Solicitud y alcance

Preparar únicamente I1 de Twitch Analytics Lakehouse en `feat/i1-spark-medallion`:
repositorio mínimo, contrato, Medallón batch, pruebas y guía Free Edition. Sin datos
completos en Git, servicios de pago, cambios a MovieLens, eliminación ni merge automático.

## Intervención

- Inspección del repositorio indicado por el usuario: estado inicial limpio en `main`,
  commit `9e22dc7`, únicamente README y .gitignore; sin AGENTS.md ni código previo.
- Consulta de documentación primaria UCSD y Databricks, enlazada en contrato y decisiones.
- Generación de módulos, notebooks fuente, casos artificiales y documentación I1.
- Ejecución de comprobaciones locales descritas en [validación](validacion.md).

No hubo acceso ni ejecución en el workspace Databricks, descarga del dataset,
creación de tablas, broker Kafka ni cambios en el proyecto MovieLens.
No se ha validado empíricamente el archivo real. Las convenciones no especificadas
por la fuente están declaradas como supuestos; la IA no certifica su semántica.

## Registro humano pendiente para la demostración

Registrar: commit exacto, ejecutor, fecha/hora, versión Spark/entorno Databricks,
SHA-256 y tamaño de `100k_a.csv`, salidas de 01 y 03–05, motivos de rechazo,
capturas de esquemas/tablas y conclusión de revisión de Anghela. No incluir tokens
ni CSV en evidencia versionada. Si una ejecución falla, registrar el error y dejar
su estado como pendiente en lugar de sustituirlo por resultados artificiales.
