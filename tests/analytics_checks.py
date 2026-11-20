"""Fixtures ARTIFICIALES, reutilizables en Spark local y Spark Connect."""
from twitch_lakehouse.transforms import gold_by_streamer
from tests.fixture_frames import fixture_frame
from twitch_lakehouse.analytics import (
    audience_by_streamer, duration_distribution, activity_by_step,
    validate_sources, validate_aggregates, assert_equal_frames)


def run_analytics_checks(spark):
    # Mismo usuario/streamer en la misma transmisión dos veces: recurrencia sí,
    # retención entre transmisiones no. Fronteras 0,10,30,60,70 minutos.
    rows = [(1, 11, "alpha", 0, 0, 0), (1, 11, "alpha", 1, 2, 10),
            (2, 12, "alpha", 2, 5, 30), (3, 21, "beta", 5, 11, 60),
            (1, 22, "beta", 10, 17, 70)]
    schema = ("user_id LONG, stream_id LONG, streamer_username STRING, "
              "time_start LONG, time_stop LONG, estimated_minutes LONG")
    silver = fixture_frame(spark, rows, schema)
    gold = gold_by_streamer(silver)
    totals = validate_sources(silver, gold, 7, 1)
    assert totals["duplicates_removed_inferred"] == 1
    assert totals["estimated_minutes"] == 170
    frames = {"streamer": audience_by_streamer(silver, gold),
              "duration": duration_distribution(silver), "activity": activity_by_step(silver)}
    validate_aggregates(frames, totals)
    audience = {r.streamer_username: r for r in frames["streamer"].collect()}
    assert audience["alpha"].interactions == 3
    assert audience["alpha"].unique_users == 2
    assert audience["alpha"].unique_streams == 2
    assert audience["alpha"].recurring_users == 1
    assert audience["alpha"].recurrence_rate == 0.5
    assert audience["beta"].recurrence_rate == 0.0
    assert sum(r.unique_users for r in audience.values()) == 4
    assert silver.select("user_id").distinct().count() == 3  # No aditividad.
    assert {r.duration_bucket: r.interactions for r in frames["duration"].collect()} == {
        0: 1, 1: 1, 2: 1, 3: 1, 4: 1}
    assert {r.time_start for r in frames["activity"].collect()} == {0, 1, 2, 5, 10}
    # Consulta de ventanas para R01/R06: desempate determinista.
    from pyspark.sql import functions as F, Window
    ranked = gold.withColumn("position", F.row_number().over(
        Window.orderBy(F.desc("interactions"), "streamer_username")))
    assert ranked.filter("position = 1").first().streamer_username == "alpha"
    assert ranked.filter("position <= 10").agg(F.sum("interactions")).first()[0] == 5
    assert ranked.filter("position <= 100").agg(F.sum("interactions")).first()[0] == 5
    # Fallos de conciliación y contrato deben detener antes de publicar.
    def fails(action):
        try:
            action()
        except ValueError:
            return
        raise AssertionError("Se esperaba ValueError")
    fails(lambda: validate_sources(silver.union(silver.limit(1)), gold, 8, 1))
    fails(lambda: validate_sources(silver, gold, 4, 0))
    fails(lambda: validate_sources(silver.limit(0), gold, 0, 0))
    fails(lambda: validate_sources(fixture_frame(spark, [(1, 1, "x", 0, 1, 0)], schema), gold, 1, 0))
    # Mismas sumas globales con streamers cambiados: equivalencia debe fallar.
    changed = gold.withColumn("streamer_username", F.concat(F.lit("wrong_"), F.col("streamer_username")))
    fails(lambda: validate_sources(silver, changed, 7, 1))
    fails(lambda: assert_equal_frames(gold, changed, gold.columns))
    broken = dict(frames, activity=frames["activity"].limit(1))
    fails(lambda: validate_aggregates(broken, totals))
    ties = fixture_frame(spark, [("z", 1), ("a", 1)], "streamer_username STRING, interactions LONG")
    assert ties.withColumn("p", F.row_number().over(Window.orderBy(
        F.desc("interactions"), "streamer_username"))).filter("p=1").first().streamer_username == "a"
    from tests.analytics_sql_checks import run_sql_checks
    run_sql_checks(spark, frames, totals)
    return {"fixture": "ARTIFICIAL", "rows": 5, "minutes": 170, "status": "PASS"}
