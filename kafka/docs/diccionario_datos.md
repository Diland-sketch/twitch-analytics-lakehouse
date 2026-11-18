# Diccionario de datos — Twitch Analytics Lakehouse

## Fuente y formato

- Fuente: https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/

- Archivo analizado: 100k_a.csv.

- Tamaño local: 114.131.306 bytes.

- Registros verificados: 3.051.733.

- Formato: CSV separado por comas, sin encabezados.

- Cada registro contiene cinco campos.

- Los nombres de columnas siguientes son asignados por el proyecto.

## Campos originales

| Posición | Nombre asignado | Tipo propuesto en Silver | Descripción | Rango observado |

|---|---|---|---|---|

| 1 | user_id | BIGINT | Identificador anonimizado del usuario. | 1 a 100000 |

| 2 | stream_id | BIGINT | Identificador de la transmisión; no identifica por sí solo al streamer. | 33801525760 a 34416415136 |

| 3 | streamer_username | STRING | Nombre del streamer. | 162625 valores distintos |

| 4 | time_start | INT | Índice temporal de inicio, en unidades de 10 minutos. | 0 a 6147 |

| 5 | time_stop | INT | Índice temporal de fin, en unidades de 10 minutos. | 1 a 6148 |

Los rangos son resultados del archivo analizado, no restricciones

universales para futuras cargas.

## Interpretación y límites

- Los tiempos son índices relativos, no fechas Unix ni horas UTC.

- No se asignarán fechas de calendario sin verificar el origen temporal.

- La presencia observada en el chat no demuestra atención continua

  ni que el usuario haya enviado mensajes.

- No se ha demostrado una clave primaria de un solo campo.

- La combinación de los cinco campos no presentó duplicados exactos

  en el archivo analizado.

## Cardinalidades verificadas

| Indicador | Resultado |

|---|---:|

| Usuarios distintos | 100000 |

| Transmisiones distintas | 739991 |

| Streamers distintos | 162625 |

## Alcance de la prueba Kafka

Se publicaron y recibieron las primeras diez filas del CSV.

Es una reproducción histórica para comprobar el transporte.

Las diez filas pertenecen al usuario 1 y no constituyen una

muestra representativa del conjunto.

Los campos source_file, source_row, run_id, historical_replay y

produced_at son metadatos añadidos por el productor del proyecto.

produced_at indica la publicación del evento, no la fecha de la

interacción original.

La prueba verificada cubre CSV → productor → Kafka → consumidor local.

La integración con Databricks todavía no se ha verificado.



## Compatibilidad con I1

El contrato de referencia es [docs/contrato_datos.md](../../docs/contrato_datos.md).
I1 almacena ambos tiempos como BIGINT; la propuesta INT de esta inspeccion
no modifica ese contrato. Los JSON usan numeros enteros y son compatibles
con LongType para los cuatro campos numericos.
