# Ejecución I1 en Databricks Free Edition

Responsable de ejecución: Diland; Anghela revisa calidad y evidencia. No se requiere
tarjeta, upgrade, trial, Kafka alojado ni clúster nuevo. Mantener Free Edition.
Este procedimiento **no ha sido ejecutado por esta entrega**.

## 1. Abrir el proyecto completo

1. Revisar la rama `feat/i1-spark-medallion` del repositorio
   `https://github.com/Diland-sketch/twitch-analytics-lakehouse`.
2. En Workspace, crear una **Git folder**, indicar esa URL y seleccionar esa rama.
   Si requiere vincular GitHub, hacerlo en la cuenta del usuario sin poner tokens en código.
   Alternativa sin conexión Git: subir el árbol completo como archivos de Workspace,
   conservando `twitch_lakehouse/`, `tests/` y `notebooks/`; importar los `.py` de
   `notebooks/` como notebooks Databricks Source. No subir solo un notebook aislado.
3. Copiar la ruta absoluta de la raíz del proyecto mediante la UI de Workspace.
   Tiene forma `/Workspace/Users/<usuario>/twitch-analytics-lakehouse`; usar la ruta
   real, que puede diferir. No agregar `/notebooks` al final.
4. Abrir `notebooks/00_config`, asignar la ruta anterior a la variable `project_root`
   en su primera celda, conectar al cómputo **Serverless** y ejecutar. La ruta vacía
   detiene la ejecución con un mensaje explícito. No se crean tablas aquí.
5. Revisar la salida: namespace `workspace.twitch_lakehouse` y ruta
   `/Volumes/workspace/twitch_lakehouse/raw_data/100k_a.csv`.

Todos los demás notebooks llaman `%run ./00_config` y reutilizan esa ruta.
No se usa un widget para ella, porque `%run` ejecuta los widgets del notebook
llamado con sus valores por defecto. La configuración de catálogo/esquema/volumen
se selecciona solo en `00_config`.
Si se cambia código importado, reiniciar la sesión Python y repetir configuración
para evitar módulos antiguos en memoria.

## 2. Probar sin datos reales

Ejecutar `01_artificial_checks` completo. No lee archivos reales ni escribe tablas.
Debe terminar con `fixture: ARTIFICIAL` y `status: PASS`. Sus valores esperados,
exclusivos del fixture, son 12 filas de entrada, 3 Silver, 8 rechazadas, 1 duplicada
y 50 minutos estimados. También evalúa seis filas adicionales de borde.
Si falla, detener I1 y guardar el error, versión Spark (`print(spark.version)`) y entorno.
No instalar PySpark encima del runtime administrado de Databricks.

## 3. Preparar objetos y cargar solo el subconjunto

1. Ejecutar `02_bootstrap`: crea únicamente esquema y volumen ausentes. Requiere
   permisos para crear esquema en `workspace` y volumen en el esquema. Si no hay
   permisos, guardar el error y revisar con el propietario del workspace; no cambiar de plan.
2. Descargar **manualmente y fuera del checkout Git** solo `100k_a.csv` desde el
   [directorio oficial](https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/).
   No descargar `full_a.csv.gz` para I1. No usar las estadísticas publicadas como conteo real.
3. En Catalog Explorer → `workspace` → `twitch_lakehouse` → Volumes → `raw_data`,
   usar Upload para subir `100k_a.csv` directamente a la raíz del volumen. Si el
   archivo ya existe, revisar su procedencia antes de cargar; no sobrescribirlo a ciegas.
4. Verificar ruta/nombre y previsualizar las primeras líneas. Deben corresponder al
   contrato de cinco campos sin cabecera. Si hay comillas, BOM, otra codificación o
   estructura diferente, detener y ajustar el contrato antes de declarar éxito.
5. Registrar tamaño y SHA-256 local del archivo para trazabilidad; por ejemplo en
   PowerShell: `Get-FileHash -Algorithm SHA256 -LiteralPath '<ruta fuera del repo>/100k_a.csv'`.
   Conservar el archivo del volumen sin cambios hasta terminar todas las etapas.

No hay descarga automática ni acceso Spark directo a la URL HTTP.

## 4. Ejecutar capas por orden

1. `03_bronze`: carga texto original, procedencia y fecha de ingesta en Delta;
   imprime `source_rows` y `bronze_rows`, que deben coincidir y ser positivos.
2. `04_silver`: tipa, clasifica, crea `silver_rejects` y `silver_interactions`.
   Guardar los cinco conteos; revisar todos los motivos de rechazo. Las rechazadas
   no entran en Gold. Si no hay filas válidas, se detiene sin crear Silver/rechazos.
3. `05_gold`: agrega por streamer y verifica sumas de interacciones y minutos.
   El ranking mostrado es resultado real solo si esta ejecución termina con evidencia.
4. En Catalog Explorer verificar nombres y esquemas de tablas creadas y conservar
   capturas/salidas con fecha, versión Spark, hash de fuente y commit Git.

Si una etapa falla, no seguir a la siguiente. Las salidas son métricas reales solo
después de ejecutar. Reconciliar como indica [calidad](calidad.md).
La aceptación requiere revisar rechazos: no basta con que el notebook termine.

## 5. Reejecución y reconstrucción segura

Las escrituras usan `errorifexists`; volver a ejecutar una etapa completada da un
error de destino existente. Es deliberado: no hay append duplicado, overwrite, DROP,
DELETE, TRUNCATE ni sustitución de Silver.

- Si se desea revisar Silver desde Bronze, ejecutar `06_rebuild_silver_candidate`.
  Por defecto solo calcula métricas y muestra filas, **sin escritura**.
- Para guardar esa reconstrucción, llenar `candidate_suffix` con un identificador
  nuevo como `revision_01` y volver a ejecutar. Se crearán dos tablas nuevas con
  sufijo `_candidate_revision_01`; si existen, se bloqueará. Comparar conteos y
  contenido con Silver. Gold sigue leyendo la Silver original. No se implementa promoción.
- Si hubo un fallo parcial (por ejemplo cuarentena creada y Silver no), detener,
  inspeccionar tablas y conservar evidencia. No borrarlas como paso automático.
- Para una segunda demostración completa, seleccionar en `00_config` otro esquema
  aislado, por ejemplo `twitch_lakehouse_i1_retry`, volver a ejecutar bootstrap,
  subir el CSV al volumen de ese esquema y ejecutar 03–05. Conserva los objetos anteriores.

Free Edition aplica cuotas. Si se agotan, registrar el bloqueo y retomar al restablecerse;
no contratar servicios para resolverlo. Kafka no forma parte de esta secuencia.
