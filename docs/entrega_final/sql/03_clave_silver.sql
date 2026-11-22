-- AUDITORÍA 03 · SOLO LECTURA. Esperado: CERO FILAS (tuplas Silver duplicadas).
-- No sumar user_id únicamente: la identidad I1 incluye cinco columnas.
SELECT user_id, stream_id, streamer_username, time_start, time_stop,
       COUNT(*) AS apariciones
FROM workspace.twitch_lakehouse.silver_interactions
GROUP BY user_id, stream_id, streamer_username, time_start, time_stop
HAVING COUNT(*) > 1
ORDER BY apariciones DESC
LIMIT 20;
