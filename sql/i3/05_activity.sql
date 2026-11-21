-- Dataset: activity. STRING streamer='', INTEGER step_from=0,
-- step_to=2147483647, bucket_steps=6 (pasos de diez minutos).
-- Puntos ausentes NO implican cero observaciones: no se rellenan.
WITH selected AS (
  SELECT CAST(FLOOR(time_start / GREATEST(1, :bucket_steps)) AS BIGINT)
           * GREATEST(1, :bucket_steps) AS relative_step,
         connection_starts, started_estimated_minutes
  FROM workspace.twitch_lakehouse.gold_i3_activity_r01
  WHERE (:streamer = '' OR streamer_username = :streamer)
    AND time_start BETWEEN :step_from AND :step_to
)
SELECT relative_step, relative_step * 10 AS relative_minutes,
       SUM(connection_starts) AS connection_starts,
       SUM(started_estimated_minutes) AS started_estimated_minutes
FROM selected
GROUP BY relative_step
ORDER BY relative_step;
