-- Dataset: duration. STRING streamer=''. Denominador: todas las interacciones
-- del streamer seleccionado, o de la release completa; NO solo un bucket visible.
WITH buckets AS (
  SELECT duration_bucket, SUM(interactions) AS interactions,
         SUM(estimated_minutes) AS estimated_minutes
  FROM workspace.twitch_lakehouse.gold_i3_duration_r01
  WHERE :streamer = '' OR streamer_username = :streamer
  GROUP BY duration_bucket
), labels AS (
  SELECT * FROM VALUES (0, '0 min'), (1, '(0,10] min'), (2, '(10,30] min'),
                       (3, '(30,60] min'), (4, '>60 min') AS t(duration_bucket, label)
)
SELECT l.duration_bucket, l.label, COALESCE(b.interactions, 0) AS interactions,
       COALESCE(b.estimated_minutes, 0) AS estimated_minutes,
       COALESCE(b.interactions, 0) * 1.0 / NULLIF(SUM(b.interactions) OVER (), 0) AS interaction_share
FROM labels l LEFT JOIN buckets b USING (duration_bucket)
ORDER BY l.duration_bucket;
