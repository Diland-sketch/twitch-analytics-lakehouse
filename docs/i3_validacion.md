# Evidencia de validación I3

## Ejecutado localmente

Base I1: `7e663f3`. Entorno observado: Windows, Python 3.13.4, PySpark 4.0.1,
Java 22.0.2. No se identifica este JDK como recomendación de compatibilidad;
para reproducir en otro entorno usar JDK soportado por Spark (17/21).

- Suite `python -m unittest discover -s tests -v` con el Python del entorno
  aislado: **22 pruebas aprobadas, cero omisiones**, 62,460 segundos.
- Incluye fixtures I1 y nuevos I3, seis archivos SQL reales ejecutados sobre
  vistas de fixture, guardas de destinos y decisiones de publicación con dobles.
- `python -m compileall -q twitch_lakehouse tests notebooks`: correcto.
- `git diff --check`: correcto.

En este Windows fue necesario poner TEMP/TMP en una carpeta temporal del proyecto
y permitir la conexión local Python–Java. La creación inicial de fixtures mediante
listas Python falló en trabajadores locales (EOF/socket); también se probó Python
3.12 portátil sin resolver ese transporte. Se cambió únicamente la construcción
de fixtures a `VALUES` tipados con `tests/fixture_frames.py`, compatible con Spark
Connect y sin trabajadores Python. Las expectativas I1 no cambiaron y las
transformaciones productivas I1 permanecen intactas. La suite final sí ejecutó
las transformaciones PySpark y SQL sobre Spark real, sin UDF/RDD.

Comando de la ejecución final desde raíz del repositorio (PowerShell, tras instalar
PySpark 4.0.1 en `.venv`; carpeta `work/tmp` local ignorada por Git):

```powershell
New-Item -ItemType Directory -Force work/tmp | Out-Null
$env:TEMP = (Resolve-Path work/tmp).Path
$env:TMP = $env:TEMP
$env:PYSPARK_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
$env:SPARK_LOCAL_IP = '127.0.0.1'
./.venv/Scripts/python -m unittest discover -s tests -v
```

Advertencias locales: Hadoop nativo/winutils ausentes, sockets sin cerrar al
recoger resultados y ventanas globales. La suite terminó OK. Las ventanas del
ranking/concentración operan sobre streamers agregados, no sobre 124M hechos;
no se ha medido su rendimiento real en Databricks.

## Alcance de los casos

- Fixture I3 de cinco interacciones: 170 minutos estimados, alpha con recurrencia
  1/2 y beta con 0/2. Valores **ARTIFICIALES**, no métricas observadas de Twitch.
- Duraciones 0/10/30/60/70, inicios 0/1/2/5/10, usuario compartido entre streamers.
- Comparación exacta detecta etiquetas cambiadas aunque las sumas coincidan.
- Errores: clave duplicada, Silver vacía, duración inconsistente, conteos
  imposibles, agregado truncado, destinos existentes, fallo parcial de escritura
  y contenido persistido inconsistente (estos dos últimos con dobles).
- SQL: búsqueda fuera de Top N conserva posición; búsqueda inexistente, tasas
  vacías NULL, todos los buckets; rango de inicio y tamaños de bucket; conciliación.
- R06 con 105 streamers artificiales: total 5.565, Top 10=1.005 y Top 100=5.550;
  denominador completo probado. Caso de menos de 10/100 streamers también probado.

## No ejecutado en esta entrega

- Acceso a tablas reales, DESCRIBE de esquemas/versiones del workspace y publicación
  Delta I3. `write_new` conserva su suite con dobles; no se probó Delta local.
- Las cifras I1 de 3.051.733 filas, 162.625 streamers y 95.887.110 minutos son
  resultados reportados previamente por el equipo, no nuevos resultados I3.
- Prueba real en Spark Connect/serverless, permisos y comportamiento de append.
- Creación, interacción, publicación o validación visual del dashboard AI/BI.

Completar pasos de `i3_ejecucion.md` y conservar evidencia del servicio antes
de marcar I3 desplegado o dashboard funcionando. Ninguna tabla real fue modificada.
