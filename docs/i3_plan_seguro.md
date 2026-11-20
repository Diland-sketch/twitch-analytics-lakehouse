# I3: auditoría y secuencia previa a implementación

Base auditada: `feat/i1-spark-medallion`, commit `7e663f3`. `main` solo tiene
README. I2 es un PR separado hacia I1; I3 se basa en I1 y no cambia Kafka.
El checkout inicial era MovieLens; Twitch se clonó en una subcarpeta independiente.
No se consultaron tablas del workspace desde este entorno.

## Inventario observado en código

Namespace por defecto: `workspace.twitch_lakehouse`; configurable con Settings.

| Tabla | Granularidad | Columnas y tipos derivados del código |
| --- | --- | --- |
| bronze_interactions | línea fuente | raw_line STRING, source_file STRING, ingested_at TIMESTAMP |
| silver_interactions | clave compuesta I1 | user_id BIGINT, stream_id BIGINT, streamer_username STRING, time_start BIGINT, time_stop BIGINT, estimated_minutes BIGINT |
| silver_rejects | línea rechazada | columnas Bronze, cinco campos normalizados, quality_errors STRING |
| gold_streamer_metrics | streamer_username | streamer_username STRING, interactions BIGINT, unique_users BIGINT, unique_streams BIGINT, estimated_minutes BIGINT |

Clave Silver: usuario, transmisión, streamer, inicio, fin. La candidata de
reconstrucción no es fuente I3. Tipos observados en transformaciones: confirmar
con `printSchema()` en Databricks antes de ejecutar.

Gold I1 resuelve R01/R02/R06/R07; no permite recuperar usuarios recurrentes,
distribución de duraciones ni inicios por intervalo. R08 necesita leer conteos
de origen y validar agregados, no copiar constantes del informe.

## Riesgos y dependencias

- I1 debe estar integrado o disponible como base; no apuntar el PR a main todavía.
- Requiere tablas Delta existentes, permisos SELECT y CREATE TABLE en el esquema,
  Spark compatible con I1 y compute ya disponible en Free Edition.
- No hay transacción multitabla: destinos nuevos con sufijo; calidad se escribe
  al final como marcador de publicación. Un fallo deja candidatos para inspección.
- Prohibido reintentar anexando a candidatos parciales. Escoger otro sufijo tras
  diagnosticar, conservar los anteriores. Sin borrado automático.
- Fijar versiones Delta de las cuatro fuentes al inicio. Evita mezclar snapshots
  si cambian durante el trabajo; mantener historia disponible durante ejecución.
- `errorifexists` falló en Spark Connect. Reutilizar CREATE estricto y append de
  `storage.write_new`; no introducir save por defecto, replace ni overwrite.
- Pull no recarga módulos Python. Reiniciar Python y repetir configuración.
- Distintos no aditivos; recurrencia por >=2 interacciones no prueba retorno entre
  sesiones. Duración cero admitida. Tiempos son pasos relativos, sin fecha origen.
- Coste: agregación exacta usuario/streamer para recurrencia; sin expandir rangos,
  sin joins de hechos contra hechos, sin UDF/RDD, sin histórico completo.

## Orden seguro

1. Rama `feat/i3-analytics-dashboard` desde I1, árbol limpio; leer contratos y guardas.
2. Definir métricas, esquema, filtros y aceptación antes de transformaciones.
3. Añadir módulos y notebooks nuevos; fixtures artificiales y guardas locales.
4. Ejecutar pruebas locales posibles y registrar límites; preparar SQL y diseño.
5. Commit y PR hacia I1 para revisión, sin merge.
6. En Databricks, actualizar Git, reiniciar Python, configurar ruta y namespace;
   ejecutar fixtures sin escrituras reales y auditar esquemas/historia.
7. Ejecutar I3 una vez con sufijo nuevo. Validar antes y después de persistir;
   leer marcador de calidad antes de usar datasets. No repetir I1.
8. Crear borrador de dos páginas, conectar parámetros, comprobar consultas/filtros
   y registrar salidas reales antes de declarar dashboard validado.
