-- Dataset: concentration. Global, sin filtro de streamer; Top exacto con desempate.
WITH ranked AS (
  SELECT interactions,
         ROW_NUMBER() OVER (ORDER BY interactions DESC, streamer_username ASC) AS position
  FROM workspace.twitch_lakehouse.gold_i3_streamer_r01
), totals AS (
  SELECT SUM(interactions) AS total_interactions,
         SUM(CASE WHEN position <= 10 THEN interactions ELSE 0 END) AS top10_interactions,
         SUM(CASE WHEN position <= 100 THEN interactions ELSE 0 END) AS top100_interactions
  FROM ranked
)
SELECT *, top10_interactions * 1.0 / NULLIF(total_interactions, 0) AS top10_share,
          top100_interactions * 1.0 / NULLIF(total_interactions, 0) AS top100_share
FROM totals;
