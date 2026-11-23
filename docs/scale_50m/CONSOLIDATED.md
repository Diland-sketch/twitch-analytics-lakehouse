# Recuperación consolidada

Destino permanente único: `workspace.twitch_bulk_consolidated`.
Siete tablas fijas: `bronze`, `silver`, `gold`, `rejects`, `manifests`, `audits`,
`publications`. Los bloques se identifican como filas de control y particiones,
sin crear objetos del catálogo por bloque. Releases como particiones append-only.

Se conservan intactos los 130 objetos anteriores, los 500 Parquet y sus manifiestos.
El ledger anterior certifica 51 bloques/50M. Se copian desde versiones Delta fijas
a Bronze única, preservando ordinal, línea, SHA, event_id, tabla y versión origen.
Solo esa consolidación vuelve a leer los primeros 10M; no se regeneran ni transfieren.
Silver/Gold reutilizan las funciones de calidad y agregación existentes.

Cada escritura usa txnAppId estable por bloque/etapa y txnVersion=1. Los conteos,
rangos, hashes y recibos persistidos complementan esa protección; un bloque parcial
inconsistente detiene el proceso. Los controles son registros inmutables por clave.
No hay transacción multi-tabla: solo publications CONFIRMED habilita consumo.

Job existente 685945819660775, mismo DAG serial y cero retries. Notebook
33_bulk_consolidated_job.py; nueva release bulk_50m_c001. La primera tarea verify
ejecuta inventory; siguen bronze, silver, gold y publish. Sin clúster pagado nuevo.

Inventario completo y objetos parciales quedan en audits, clave
inventory/bulk_50m_c001. No se ejecuta limpieza. Cualquier candidato posterior
requiere comprobar equivalencia y dependencias y autorización expresa.

Prueba dirigida: `python3 -m unittest tests.test_compact_spark -v` con Spark local;
fixture artificial, almacenamiento transaccional fake, 39,503 segundos, PASS.
No equivale a prueba remota. No se modifica Kafka ni React en esta recuperación.
