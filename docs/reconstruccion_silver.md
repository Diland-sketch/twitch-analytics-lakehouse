# Reconstrucción de Silver: evidencia y parametrización

## Resultado real reportado por el equipo

Fuente de reconstrucción: Bronze, usando las mismas transformaciones del pipeline.
Namespace: `workspace.twitch_lakehouse`.

| Comprobación | Resultado reportado |
| --- | --- |
| Silver original | `silver_interactions`: 3.051.733 filas |
| Candidata independiente | `silver_interactions_candidate_suffix`: 3.051.733 filas |
| exceptAll original → candidata | 0 diferencias |
| exceptAll candidata → original | 0 diferencias |
| Equivalencia | True |
| Silver original | No se modificó |

La igualdad de conteos por sí sola no demuestra equivalencia. Las dos diferencias
vacías con exceptAll comprueban igualdad de filas y sus multiplicidades para las
columnas comparadas, sin exigir un orden físico. Esta es evidencia reportada por
el usuario; el asistente no volvió a ejecutar la comparación. Conservar el notebook
y sus salidas para documentar las columnas exactas y la versión de cada tabla.
No se aportó un conteo independiente de la cuarentena candidata; no se inventa.

## Incidente de parametrización

En Databricks se pasó accidentalmente `candidate_suffix="suffix"`: es el texto
literal suffix, no el contenido de la variable Python. Por ello se creó el nombre
`silver_interactions_candidate_suffix`. La escritura aislada protegió la Silver original.
No se renombra ni elimina esa candidata: conserva la evidencia de la prueba.

El archivo versionado `notebooks/06_rebuild_silver_candidate.py` **ya es correcto**:

```python
suffix = dbutils.widgets.get("candidate_suffix")
if suffix:
    print(build_silver(spark, cfg, candidate_suffix=suffix))
```

Las comillas son correctas para identificar el widget en get; no se usan alrededor
de suffix al pasar su valor a build_silver. No se modificó el notebook ni el pipeline.

## Uso correcto del widget en una futura reconstrucción

1. No repetir la reconstrucción actual para cambiar su nombre. Ya quedó validada.
2. Cuando se necesite otra candidata, abrir 06 y ejecutar su primera celda de
   configuración/vista previa. Si hubo Pull de módulos Python, reiniciar Python
   primero y ejecutar nuevamente 00_config.
3. Ejecutar la celda del widget inicialmente vacío: muestra solo vista previa y
   no crea candidata. Si el widget conserva un valor anterior, revisarlo antes de
   ejecutar la celda para no intentar escribir con ese sufijo.
4. Escribir en el campo del widget, sin comillas, un sufijo nuevo como `revision_02`.
   Elegir un identificador en minúsculas con letras, números y guiones bajos.
5. Verificar visualmente que la llamada es `candidate_suffix=suffix` y ejecutar
   la celda. El valor revision_02 genera `silver_interactions_candidate_revision_02`
   y `silver_rejects_candidate_revision_02`. Si cualquiera existe, la prevalidación
   bloquea la ejecución. No borrar tablas para reutilizar un nombre.
6. Comparar la candidata con la original en ambos sentidos y conservar las salidas.
   Gold continúa usando la Silver original; no hay promoción automática.

Ejemplo de comprobación de solo lectura para la candidata **ya existente**, después
de 00_config; no se ejecuta automáticamente desde esta documentación:

```python
original = spark.table(cfg.table("silver_interactions"))
candidate = spark.table(cfg.table("silver_interactions_candidate_suffix"))
columns = original.columns
assert set(candidate.columns) == set(columns)
assert candidate.select(*columns).schema.simpleString() == original.schema.simpleString()
candidate = candidate.select(*columns)
forward = original.exceptAll(candidate).count()
backward = candidate.exceptAll(original).count()
print({"original_minus_candidate": forward,
       "candidate_minus_original": backward,
       "equivalencia": forward == 0 and backward == 0})
```

El ejemplo alinea las columnas porque exceptAll compara por posición. No deducir
que cero diferencias valida semánticas o reglas nuevas: ambas tablas usan las mismas reglas I1.
