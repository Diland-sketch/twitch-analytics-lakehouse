-- AUDITORÍA 06 · SOLO LECTURA. UNA FILA; status=PASS y siete deltas=0.
-- NULL, ausencia de filas, valores no cero o release incorrecta => FAIL.
WITH s AS (
 SELECT SUM(interactions) AS n, SUM(estimated_minutes) AS m
 FROM workspace.twitch_lakehouse.gold_i3_streamer_r01
), d AS (
 SELECT SUM(interactions) AS n, SUM(estimated_minutes) AS m
 FROM workspace.twitch_lakehouse.gold_i3_duration_r01
), a AS (
 SELECT SUM(connection_starts) AS n, SUM(started_estimated_minutes) AS m
 FROM workspace.twitch_lakehouse.gold_i3_activity_r01
)
SELECT q.release, q.status, q.scope, q.source_namespace,
       q.bronze_rows, q.silver_rows, q.rejected_rows,
       q.duplicates_removed_inferred, q.silver_duplicate_keys, q.gold_streamers,
       q.estimated_minutes,
       q.bronze_version, q.silver_version, q.rejects_version, q.gold_version,
       s.n-q.silver_rows AS streamer_rows_delta,
       d.n-q.silver_rows AS duration_rows_delta,
       a.n-q.silver_rows AS activity_rows_delta,
       s.m-q.estimated_minutes AS streamer_minutes_delta,
       d.m-q.estimated_minutes AS duration_minutes_delta,
       a.m-q.estimated_minutes AS activity_minutes_delta,
       q.bronze_rows-q.rejected_rows-q.duplicates_removed_inferred-q.silver_rows AS i1_rows_delta
FROM workspace.twitch_lakehouse.gold_i3_quality_r01 q
CROSS JOIN s CROSS JOIN d CROSS JOIN a;
