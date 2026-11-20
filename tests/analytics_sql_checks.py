"""Ejecutar el SQL entregable sobre vistas temporales ARTIFICIALES."""
from pathlib import Path
from tests.fixture_frames import fixture_frame


def run_sql_checks(spark, frames, totals):
    root = Path(__file__).resolve().parents[1] / "sql" / "i3"
    quality = dict(totals, release="fixture", status="PASS")
    quality_frame = fixture_frame(spark, [tuple(quality.values())], ", ".join(
        f"{name} {'STRING' if isinstance(value, str) else 'LONG'}" for name, value in quality.items()))
    for kind, frame in dict(frames, quality=quality_frame).items():
        frame.createOrReplaceTempView(f"i3_fixture_{kind}")

    def query(filename, **args):
        sql = (root / filename).read_text(encoding="utf-8")
        for kind in ("streamer", "duration", "activity", "quality"):
            sql = sql.replace(f"workspace.twitch_lakehouse.gold_i3_{kind}_r01", f"i3_fixture_{kind}")
        return spark.sql(sql, args=args)

    assert query("01_overview.sql").first().silver_rows == 5
    assert query("02_streamers.sql", streamer="", top_n=1).first().streamer_username == "alpha"
    assert query("02_streamers.sql", streamer="beta", top_n=1).first().position == 2
    assert query("02_streamers.sql", streamer="absent", top_n=10).count() == 0
    c = query("03_concentration.sql").first()
    assert c.top10_share == c.top100_share == 1
    d = query("04_duration.sql", streamer="").collect()
    assert len(d) == 5 and sum(r.interactions for r in d) == 5
    assert abs(sum(float(r.interaction_share) for r in d) - 1) < 1e-12
    assert all(r.interactions == 0 and r.interaction_share is None
               for r in query("04_duration.sql", streamer="absent").collect())
    activity = query("05_activity.sql", streamer="", step_from=0,
                     step_to=2147483647, bucket_steps=6).collect()
    assert [(r.relative_step, r.connection_starts) for r in activity] == [(0, 4), (6, 1)]
    assert query("05_activity.sql", streamer="alpha", step_from=1,
                 step_to=2, bucket_steps=1).agg({"connection_starts": "sum"}).first()[0] == 2
    assert query("05_activity.sql", streamer="absent", step_from=0,
                 step_to=100, bucket_steps=6).count() == 0
    r = query("06_reconciliation.sql").first().asDict()
    assert all(value == 0 for name, value in r.items() if name.endswith("_delta"))
    # Población >100 para comprobar ambos cortes contra el denominador completo.
    many = fixture_frame(spark, [(f"s{i:03}", 105 - i) for i in range(105)],
                                 "streamer_username STRING, interactions LONG")
    many.createOrReplaceTempView("i3_fixture_streamer")
    c = query("03_concentration.sql").first()
    assert c.total_interactions == 5565
    assert c.top10_interactions == 1005 and c.top100_interactions == 5550
    assert abs(float(c.top10_share) - 1005 / 5565) < 1e-12
    assert abs(float(c.top100_share) - 5550 / 5565) < 1e-12
