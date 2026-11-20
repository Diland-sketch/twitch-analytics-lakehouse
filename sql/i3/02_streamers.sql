-- Dataset: streamers. Parámetros STRING streamer='' (todos), INTEGER top_n=10.
-- Ranking global antes de filtrar; LIMIT lógico acotado a 1000 filas.
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY interactions DESC, streamer_username ASC) AS position
  FROM workspace.twitch_lakehouse.gold_i3_streamer_r01
)
SELECT streamer_username, position, interactions, unique_users, unique_streams,
       estimated_minutes, recurring_users, recurrence_rate
FROM ranked
WHERE (:streamer = '' OR streamer_username = :streamer)
  AND (:streamer <> '' OR position <= LEAST(1000, GREATEST(1, :top_n)))
ORDER BY position;
