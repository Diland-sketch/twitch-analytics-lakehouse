-- Dataset: overview. Namespace/release deben coincidir con notebook 08.
-- Solo publicar si quality devuelve UNA fila PASS.
SELECT q.release, q.status, q.bronze_rows, q.silver_rows, q.rejected_rows,
       q.duplicates_removed_inferred, q.silver_duplicate_keys,
       q.gold_streamers, q.estimated_minutes,
       q.silver_rows * 1.0 / NULLIF(q.bronze_rows, 0) AS silver_to_bronze_ratio
FROM workspace.twitch_lakehouse.gold_i3_quality_r01 q
WHERE q.status = 'PASS';
