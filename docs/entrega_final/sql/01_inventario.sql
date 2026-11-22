-- AUDITORÍA 01 · SOLO LECTURA. Esperado: 8 tablas listadas si I1/I3 r01 completos.
-- Databricks SQL Editor; namespace por defecto workspace.twitch_lakehouse.
SELECT table_catalog, table_schema, table_name, table_type
FROM workspace.information_schema.tables
WHERE table_schema = 'twitch_lakehouse'
  AND table_name IN (
    'bronze_interactions','silver_interactions','silver_rejects','gold_streamer_metrics',
    'gold_i3_streamer_r01','gold_i3_duration_r01','gold_i3_activity_r01','gold_i3_quality_r01'
  )
ORDER BY table_name;
