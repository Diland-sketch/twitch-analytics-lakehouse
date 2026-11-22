-- AUDITORÍA 02 · SOLO LECTURA. Una fila; delta_br_vs_si_rej_dup=0 y diferencias Gold=0.
-- bronze = validas_crudas + rechazadas; validas_crudas = silver + duplicados.
-- "duplicados_inferidos" se calcula por diferencia de población, como quality I3.
WITH b AS (SELECT COUNT(*) AS n FROM workspace.twitch_lakehouse.bronze_interactions),
s AS (SELECT COUNT(*) AS n, COALESCE(SUM(estimated_minutes),0) AS minutos
      FROM workspace.twitch_lakehouse.silver_interactions),
r AS (SELECT COUNT(*) AS n FROM workspace.twitch_lakehouse.silver_rejects),
g AS (SELECT COUNT(*) AS n, COALESCE(SUM(interactions),0) AS conexiones,
              COALESCE(SUM(estimated_minutes),0) AS minutos
      FROM workspace.twitch_lakehouse.gold_streamer_metrics),
q AS (SELECT bronze_rows, silver_rows, rejected_rows, duplicates_removed_inferred
      FROM workspace.twitch_lakehouse.gold_i3_quality_r01)
SELECT b.n AS filas_bronze, s.n AS filas_silver, r.n AS filas_rechazadas,
       g.n AS streamers_gold, g.conexiones AS conexiones_gold,
       s.minutos AS minutos_silver, g.minutos AS minutos_gold,
       q.duplicates_removed_inferred AS duplicados_inferidos_por_i3,
       b.n - r.n - s.n - q.duplicates_removed_inferred AS delta_br_vs_si_rej_dup,
       g.conexiones - s.n AS delta_conexiones_gold_silver,
       g.minutos - s.minutos AS delta_minutos_gold_silver,
       q.bronze_rows - b.n AS delta_quality_bronze,
       q.silver_rows - s.n AS delta_quality_silver,
       q.rejected_rows - r.n AS delta_quality_rechazos
FROM b CROSS JOIN s CROSS JOIN r CROSS JOIN g CROSS JOIN q;
