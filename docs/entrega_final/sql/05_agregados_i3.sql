-- AUDITORÍA 05 · SOLO LECTURA. Tres familias con mismo N y minutos que Silver.
-- Son tres representaciones de la MISMA población: NO sumar las familias.
WITH s AS (
 SELECT COUNT(*) AS n, COALESCE(SUM(estimated_minutes),0) AS minutos
 FROM workspace.twitch_lakehouse.silver_interactions
),
f AS (
 SELECT 'streamer' AS familia, SUM(interactions) AS n, SUM(estimated_minutes) AS minutos
 FROM workspace.twitch_lakehouse.gold_i3_streamer_r01
 UNION ALL
 SELECT 'duration', SUM(interactions), SUM(estimated_minutes)
 FROM workspace.twitch_lakehouse.gold_i3_duration_r01
 UNION ALL
 SELECT 'activity', SUM(connection_starts), SUM(started_estimated_minutes)
 FROM workspace.twitch_lakehouse.gold_i3_activity_r01
)
SELECT f.familia, f.n AS interacciones, s.n AS silver_interacciones,
       f.minutos AS minutos, s.minutos AS silver_minutos,
       f.n-s.n AS delta_interacciones, f.minutos-s.minutos AS delta_minutos
FROM f CROSS JOIN s
ORDER BY f.familia;
