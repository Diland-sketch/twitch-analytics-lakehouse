# Calidad y conciliación

Anghela revisa las reglas; Diland integra ejecución batch. Reglas propuestas, no
certificación del archivo aún no cargado.

| Regla | Acción |
| --- | --- |
| Cantidad distinta de cinco campos | Cuarentena `field_count` |
| Comillas | Cuarentena `unsupported_quotes` |
| ID vacío, no entero, overflow o no positivo | `invalid_user_id` / `invalid_stream_id` |
| Streamer vacío tras trim | `empty_streamer` |
| Tiempo vacío, no entero, negativo, overflow o fuera de límite técnico | `invalid_start` / `invalid_stop` |
| Fin menor al inicio | `reversed_interval` |
| Tupla válida repetida | Una fila Silver; contabilizar duplicados |
| Fuente vacía o ninguna válida | Detener antes de escribir la capa correspondiente |

Una fila puede tener varios motivos separados por `|`; el número de motivos no es
el número de filas rechazadas. No hay umbral inventado de tolerancia a rechazos.
Cualquier rechazo se revisa antes de declarar I1 aceptado; queda conservado.

Conciliaciones impresas en notebooks:

1. `source_rows = bronze_rows`.
2. `bronze_rows = valid_rows + rejected_rows`.
3. `valid_rows = silver_rows + duplicate_rows` y clave Silver única.
4. Suma de interacciones Gold = filas Silver.
5. Suma de minutos Gold = suma de minutos Silver.
6. Conteos persistidos Bronze/Silver/rechazos coinciden con los anteriores a escritura.

Guardar salidas reales y revisar motivos. No usar valores del fixture como expectativa
del dataset real. Conteos consumen cuota Spark; I1 prima claridad sobre optimización
para 124 millones de filas, que queda fuera de alcance.
