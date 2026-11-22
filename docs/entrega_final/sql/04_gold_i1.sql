-- AUDITORÍA 04 · SOLO LECTURA. CERO FILAS: igualdad exacta por streamer Gold I1 vs Silver.
-- Distintos son POR streamer; no derivar usuarios globales sumando Gold.
WITH s AS (
  SELECT streamer_username,
         COUNT(*) AS interactions,
         COUNT(DISTINCT user_id) AS unique_users,
         COUNT(DISTINCT stream_id) AS unique_streams,
         SUM(estimated_minutes) AS estimated_minutes
  FROM workspace.twitch_lakehouse.silver_interactions
  GROUP BY streamer_username
),
g AS (
  SELECT streamer_username, interactions, unique_users, unique_streams, estimated_minutes
  FROM workspace.twitch_lakehouse.gold_streamer_metrics
)
SELECT COALESCE(s.streamer_username,g.streamer_username) AS streamer,
       s.interactions AS silver_interactions, g.interactions AS gold_interactions,
       s.unique_users AS silver_usuarios, g.unique_users AS gold_usuarios,
       s.unique_streams AS silver_streams, g.unique_streams AS gold_streams,
       s.estimated_minutes AS silver_minutos, g.estimated_minutes AS gold_minutos
FROM s FULL OUTER JOIN g ON s.streamer_username=g.streamer_username
WHERE NOT (s.interactions <=> g.interactions)
   OR NOT (s.unique_users <=> g.unique_users)
   OR NOT (s.unique_streams <=> g.unique_streams)
   OR NOT (s.estimated_minutes <=> g.estimated_minutes)
ORDER BY streamer
LIMIT 30;
