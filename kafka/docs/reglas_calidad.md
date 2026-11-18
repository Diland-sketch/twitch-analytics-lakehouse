# Reglas de calidad — Twitch Analytics Lakehouse

## Resultados verificados en 100k_a.csv

Se revisaron los 3.051.733 registros del archivo completo.

| Comprobación | Resultado |

|---|---:|

| Filas con cantidad de campos distinta de cinco | 0 |

| Campos vacíos o compuestos solo por espacios | 0 |

| Valores no convertibles a entero en campos numéricos | 0 |

| Registros con tiempos negativos | 0 |

| Registros con time_stop menor que time_start | 0 |

| Registros con time_stop igual a time_start | 0 |

| Duplicados exactos de los cinco campos | 0 |

Estas comprobaciones no garantizan ausencia de todos los posibles

problemas de calidad. No se verificaron todavía marcadores textuales

de nulos en streamer_username, espacios alrededor de valores ni

consistencia entre transmisiones y nombres de streamers.

## Tratamiento propuesto por capa

### Bronze

- Conservar los datos originales sin limpieza ni deduplicación.

- Leer el CSV sin encabezados.

- Conservar los cinco campos como texto antes de su conversión.

- Registrar el archivo de origen y la fecha de ingesta.

- Conservar o identificar las filas que no puedan interpretarse.

### Silver

| Regla | Acción propuesta |

|---|---|

| La fila debe contener cinco campos. | Separar las filas malformadas y registrar el motivo. |

| Los cinco campos deben tener contenido. | Separar registros con valores vacíos o solo espacios. |

| user_id y stream_id deben convertirse a BIGINT. | Separar registros que fallen en la conversión. |

| time_start y time_stop deben convertirse a INT. | Separar registros que fallen en la conversión. |

| Los tiempos deben ser no negativos. | Separar los registros que incumplan la regla. |

| time_stop debe ser mayor que time_start. | Separar los demás para revisión; no corregirlos automáticamente. |

| No conservar duplicados exactos en la tabla depurada. | Deduplicar por los cinco campos originales y contabilizar las filas retiradas. |

Las filas separadas deben conservarse en una tabla de cuarentena con

su motivo de rechazo. No se deben eliminar silenciosamente.

No aplicar automáticamente cambios de mayúsculas o minúsculas al

nombre del streamer sin justificar y validar esa decisión.

Los rangos observados del archivo se usarán como referencia para

detectar cambios, no como límites universales de aceptación.

### Gold

- Construir agregaciones a partir de Silver.

- Documentar la fórmula y unidad de cada indicador.

- Si se utiliza (time_stop - time_start) * 10, describirlo como

  intervalo temporal estimado en minutos, no como tiempo exacto

  de atención continua del usuario.

- Comprobar la conciliación de conteos entre capas.

## Reproducción mediante Kafka

- Las repeticiones del productor pueden volver a publicar las mismas filas.

- run_id identifica una ejecución; no identifica una interacción única.

- No usar produced_at para decidir si una interacción histórica es nueva.

- Conservar los metadatos de transporte para trazabilidad.

- Diferenciar duplicados del archivo original de reenvíos del productor.

## Estado

Reglas propuestas para acordar con Diland.

Su implementación en Spark y su ejecución en Databricks

todavía no se han verificado en esta prueba local.



## Diferencias con el contrato vigente de I1

Este documento conserva propuestas de la inspeccion local de Anghela; no
sustituye las reglas de Diland. I1 admite time_stop == time_start y almacena
los tiempos como BIGINT, mientras la propuesta anterior separa igualdad
y propone INT. El archivo inspeccionado no contiene casos de igualdad.
Para la integracion se conserva el contrato I1, incluida la duracion cero.
No se modifican modulos ni tablas existentes.
