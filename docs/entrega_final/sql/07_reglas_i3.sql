-- AUDITORÍA 07 · SOLO LECTURA. Resumen de dominios y buckets I3.
-- Esperado: violaciones_streamer=0, buckets_invalidos=0, total_buckets=Silver.
WITH e AS (
 SELECT COUNT(*) AS violaciones_streamer
 FROM workspace.twitch_lakehouse.gold_i3_streamer_r01
 WHERE streamer_username IS NULL OR TRIM(streamer_username)=''
    OR interactions IS NULL OR unique_users IS NULL OR unique_streams IS NULL
    OR recurring_users IS NULL OR estimated_minutes IS NULL
    OR interactions<=0 OR unique_users<=0 OR unique_users>interactions
    OR unique_streams<=0 OR unique_streams>interactions
    OR recurring_users<0 OR recurring_users>unique_users
    OR recurrence_rate IS NULL OR recurrence_rate<0 OR recurrence_rate>1
    OR estimated_minutes<0
), d AS (
 SELECT COUNT(*) AS buckets_invalidos
 FROM workspace.twitch_lakehouse.gold_i3_duration_r01
 WHERE duration_bucket IS NULL OR interactions IS NULL OR estimated_minutes IS NULL
    OR duration_bucket NOT IN (0,1,2,3,4)
    OR interactions<0 OR estimated_minutes<0
), z AS (
 SELECT COUNT(DISTINCT duration_bucket) AS buckets_distintos,
        SUM(interactions) AS total_buckets
 FROM workspace.twitch_lakehouse.gold_i3_duration_r01
), q AS (
 SELECT silver_rows FROM workspace.twitch_lakehouse.gold_i3_quality_r01
)
SELECT e.violaciones_streamer, d.buckets_invalidos, z.buckets_distintos,
       z.total_buckets, q.silver_rows,
       z.total_buckets-q.silver_rows AS delta_buckets
FROM e CROSS JOIN d CROSS JOIN z CROSS JOIN q;
