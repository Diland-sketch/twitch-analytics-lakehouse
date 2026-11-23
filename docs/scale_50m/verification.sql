-- Solo lectura. Releases confirmadas; cada población es un prefijo independiente.
-- No sumar r01, piloto Kafka, 100k, 1M ni hitos siguientes.
SELECT '100k' AS milestone, report_json
FROM workspace.twitch_bulk_experimental.publication_bulk_100k_r001
UNION ALL
SELECT '1m', report_json
FROM workspace.twitch_bulk_experimental.publication_bulk_1m_r001;

SELECT COUNT(*) AS source_business_rows,
       COUNT(DISTINCT user_id) AS unique_users_global,
       SUM(estimated_minutes) AS estimated_minutes
FROM workspace.twitch_bulk_experimental.silver_bulk_1m_r001;

SELECT COUNT(*) AS streamers, SUM(interactions) AS interactions,
       SUM(estimated_minutes) AS estimated_minutes
FROM workspace.twitch_bulk_experimental.gold_bulk_1m_r001;
-- No sumar unique_users de streamers para obtener usuarios globales.

SELECT streamer_username, interactions, unique_users, unique_streams,
       estimated_minutes
FROM workspace.twitch_bulk_experimental.gold_bulk_1m_r001
ORDER BY interactions DESC, streamer_username
LIMIT 20;

SELECT quality_errors, COUNT(*) AS rejected_rows
FROM workspace.twitch_bulk_experimental.rejects_bulk_1m_r001
GROUP BY quality_errors;

DESCRIBE DETAIL workspace.twitch_bulk_experimental.silver_bulk_1m_r001;
DESCRIBE HISTORY workspace.twitch_bulk_experimental.silver_bulk_1m_r001;
