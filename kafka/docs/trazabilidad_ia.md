# Trazabilidad I2

Fecha de trabajo: 17 de noviembre de 2026, America/Bogota.

## Solicitud y uso de IA

Anghela pidió inspección antes de modificar, validación real en Compose,
incorporación en rama independiente y PR hacia feat/i1-spark-medallion,
seguida de prueba Spark real sin alterar el pipeline batch ni contratar servicios.

La IA revisó archivos y contrato remoto, propuso comandos y preparó esta copia
de integración. Anghela ejecutó Docker y Python desde PowerShell y aportó
las salidas. La sesión del asistente no tiene acceso al socket Docker ni al
puerto local: no atribuirle una ejecución autónoma del broker.

## Correcciones y decisiones

1. Se retiró un parámetro no admitido por kafka-python 3.0.13.
2. La consulta funcionó forzando IPv4; se eligió anunciar 127.0.0.1.
3. El cambio dinámico fue rechazado en KRaft; se corrigió con configuración
   explícita al crear otro contenedor, conservando los anteriores.
4. El volumen de Compose necesitó propiedad 1000:1000 antes del arranque.
5. El primer consumidor de Compose usó RUN_ID literalmente y falló; se
   conservó la evidencia y se recuperó con el UUID real. No ocultar el intento.
6. Se verificó un lote nuevo con UUID y offsets distintos; no se reutilizaron
   resultados del contenedor anterior como prueba de Compose.
7. Se conservan productor, consumidor y Compose operativos; los cambios de
   formato documental se hacen solo en esta copia. No se cambian módulos I1.
8. Se detectaron diferencias de propuesta local frente a I1: INT frente a
   BIGINT y regla de tiempo estricta frente a igualdad admitida. Se conserva I1.

## Estado reportado por Diland

El equipo reportó Bronze/Silver de 3051733 filas, cero rechazos y duplicados,
Gold de 162625 streamers y 95887110 minutos estimados. Este incremento no
ejecuta ni certifica esas tablas y no cambia su documentación histórica.

La rúbrica oficial sigue sin estar adjunta aquí. docs/rubrica.md es provisional;
no presentar sus criterios como los siete criterios oficiales del docente.

## Pendiente explícito

Prueba Kafka → Spark Structured Streaming, manejo de JSON inválido, identidad
estable y deduplicación entre lotes. No existe prueba Kafka → Databricks.
No se habilitaron túneles, servicios de pago ni exposición pública.
