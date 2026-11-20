# Validación I5

## Resultado real

**E2E Kafka → Spark I4 → SQLite → FastAPI → WebSocket → React: PASS.**
Ejecutado localmente el 20 de noviembre de 2026, sobre CSV real ya disponible.
No se sustituyeron Kafka, Spark, API o datos por dobles en esa demostración.

| Comprobación | Resultado |
| --- | --- |
| run_id nuevo | `07d2cfdb-9e03-4810-bebc-2ae70f422f00` |
| Fuente | primeras 100 filas reales de 100k_a.csv |
| Kafka | 100 confirmaciones y 100 identidades según SHA256/ordinal/run |
| Spark/SQLite | 100 eventos únicos, 100 válidos, 0 inválidos |
| API | mismos conteos; suma de agregados válidos=100; minutos=2.070 |
| Frontend | KPI 0→valores intermedios→100; captura antes y después |
| WebSocket | frames reales con 0, 8, 33, 58, 63, 69, 79, 86, 91, 96 y 100 (entre otras lecturas) |
| Seguridad de montaje durante E2E | I4 volumen Mode=ro, RW=false; root filesystem API read_only=true |
| Respaldo | Streamlit original permanece disponible en localhost:8501 |

La ejecución I4 previa se conserva. Ahora hay dos reproducciones deliberadas de
100 únicos cada una: vista «Todas»=200. Los 300 offsets globales incluyen el
reenvío I4 anterior; la selección I5 muestra 100, sin sumar otras ejecuciones.
No se modificaron estructuras, módulos, productor, Spark o checkpoints de I4.
El productor original únicamente publicó el lote nuevo autorizado por la prueba.

SHA256 del CSV:
`11d3bdb49683f651eb6b0ba806bf317db68646efc0c7de2d02d750201fd8ef69`.

## Pruebas ejecutadas

- API: **8 aprobadas**, incluyendo contrato filtrado/global, UUID inválido,
  ausencia de DB sin creación, equivalencia HTTP/WS, Origin ajeno rechazado y
  ausencia de rutas de escritura/DB intacta. Fixtures ARTIFICIALES.
- Frontend unitario: **8 aprobadas**: enteros exactos, cero real/vacío, señal
  histórica, cambio de selección, respuesta atrasada, polling y dato conservado
  ante pérdida de conexión. Fixtures solo en test, no importados por producto.
- Navegador sobre API real: **4 aprobadas** en ejecución final: render/navegación,
  1440px y 390px sin overflow, polling con WS interrumpido, indisponibilidad
  HTTP+WS sin ceros ficticios, y reconexión WS tras polling sin cambiar conteos.
- E2E real: **1 aprobada** en ejecución opt-in separada (56,2s incluyendo replay).
  En la repetición de las cuatro pruebas de lectura quedó omitida explícitamente
  para no publicar otros 100 eventos sin necesidad.
- Compilación TypeScript/Vite, compileall y git diff --check correctos.
- npm audit después de actualizar el ejecutor Vitest: cero vulnerabilidades en
  la instalación. No se cambió el Node ni el Python global del equipo.

Se detectó y corrigió un overflow móvil de canvas (min-width del contenedor).
La primera corrida de navegador registró ese fallo; la repetición final pasó.
El E2E de negocio ya había pasado y no se falseó su reporte por el fallo visual.

## SQLite WAL y revisión de permisos

El montaje RO inicial devolvió 503 al faltar archivos auxiliares WAL/SHM. Una
propuesta de montar RW para coordinación fue rechazada por revisión automática
porque ampliaba el acceso del contenedor al estado I4. Un despliegue ya iniciado
tomó brevemente esa configuración intermedia; fue detectado y restaurado a RO.
No hay rutas ni código DML/DDL en la API; no se ejecutaron escrituras sobre tablas.

Solución final comprobada: volumen RO + conexión persistente mode=ro, sin
transacción de lectura abierta, para conservar el ciclo de vida WAL; snapshots
con el lector I4 original y sus transacciones breves. No se cambió journal_mode,
no se usó immutable ni se copiaron archivos activos. Prueba real y captura de
montaje se registraron después de confirmar RW=false.

Límite de arranque frío: si no hay auxiliares legibles, API devuelve 503 y espera
al escritor I4; puede necesitar un primer micro-lote. No habilita escritura como
recuperación automática. No se afirma que sea un lector universal de cualquier
SQLite en cualquier filesystem.

## Evidencia local conservada

`work/i5-evidence/07d2cfdb-9e03-4810-bebc-2ae70f422f00/` en checkout I5:

| Archivo | SHA256 |
| --- | --- |
| report.json | 0427d93db7807051f2124daf33c3e0a952025339eb38bb840fcd428774b45566 |
| snapshots.json | 5bc549b0802c9f4a7d5545e51fbcccefcc24c5b9ba4488e657a169bb9d26912b |
| producer.log | c923b7a2486925cd35d52e5f4905a15c75b61eb01fd2e47b9838083bac25fdd9 |
| spark.log | bb8618c140dd8ea1d0b3ea9c418867a119abff7745d3e0485fd2244cf5578246 |
| before-0.png | 28aaaa2c51da74031bdc5f333bab1ab7530e46c83dfb36520425f72d0d098cdb |
| after-100.png | 3de53734ee49c22f523e18566c483ebad49ddc022aad156e15aaf817f2939b07 |
| readonly-mount.json | 293ca1ea197b2c94ee11a796f80782e08c0e7dc86adfe73a1e897724757d8e40 |

Además `work/i5-evidence/browser/desktop-real.png` y `mobile-real.png` contienen
las capturas de lectura responsive de la corrida final. No se versionan CSV,
DB, estado, logs originales, capturas ni node_modules. Se conserva el resumen
con hashes para identificar evidencia; no sustituye reproducir en otro equipo.

## Límites restantes

No prueba escala de 124M, alta disponibilidad ni SLA de latencia. Bundle incluye
ECharts (~307kB gzip en build observado); uso previsto local. Actividad se limita
a 300 puntos renderizados y ranking Top 20; el lector heredado materializa su
resultado antes de truncar, adecuado para MVP, no para histórico completo.
API/WS no prueba salud Spark: estado stale se evalúa con heartbeat real.
Animaciones son interpolación gráfica; KPI nunca cuenta números intermedios.
No se atribuyen espectadores activos, seguidores, ingresos ni retención.
