# Rúbrica provisional y criterios de aceptación I1

**La rúbrica oficial del curso no fue proporcionada.** Esta matriz es una propuesta
de verificación, sin notas, porcentajes ni requisitos atribuidos al docente.

| Criterio propuesto | Evidencia disponible / requerida | Estado reportado de I1 |
| --- | --- | --- |
| Spark operativo | Smoke test artificial reportado por el equipo | Reportado; notebook original no incluido |
| Repositorio mantenible | Rama, README, configuración y módulos separados | Preparado |
| Contrato verificado | Enlaces primarios, cinco campos y supuestos declarados | Documentado; revisar archivo cargado |
| Bronze real | Archivo cargado, tabla Delta y conteo reconciliado | Confirmado por equipo: 3.051.733 filas; ver validacion.md |
| Silver y calidad | Reglas implementadas y conciliación | Confirmado: 3.051.733 válidos, 0 rechazos, 0 duplicados |
| Gold | Agregado por streamer y sumas conciliadas | Confirmado: 162.625 streamers; 3.051.733 interacciones; 95.887.110 minutos estimados |
| Reconstrucción segura | Candidata independiente; original intacta | Confirmado: 3.051.733 filas por tabla; exceptAll 0 en ambos sentidos |
| Pruebas | Resultados en validacion.md y notebook 01 | Locales registradas; 01 PASS e I1 batch completo reportados por equipo |
| Kafka obligatorio | Integración y evidencia de Anghela en siguiente incremento | Fuera de I1, pendiente |
| Presentación honesta | Separar estado reportado, artificial y real | Guía disponible |

I1 batch está completado y verificado según resultados reales reportados por el equipo;
conservar salidas originales como evidencia. El proyecto completo no está terminado mientras Kafka
y los demás criterios oficiales no estén validados. Actualizar esta matriz cuando
se reciba la rúbrica del curso.
