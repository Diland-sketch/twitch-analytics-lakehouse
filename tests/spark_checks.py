"""Casos exclusivamente ARTIFICIALES. No son observaciones de Twitch."""
from twitch_lakehouse.transforms import silver_frames, gold_by_streamer


def run_spark_checks(spark):
    # Dos usuarios, tres interacciones distintas, un duplicado y ocho rechazos.
    lines = ["1,11,alpha,0,2", "1,11,alpha,0,2", "2,11,alpha,2,2",
             "2,12,beta,2,5", "bad,11,a,0,1", "1,11,a,5,2",
             "1,11,,0,1", "1,11,a,0", "1,11,a,0,1,extra", "",
             "9223372036854775808,11,a,0,1", "1,11,a,-1,1"]
    bronze = spark.createDataFrame([(line,) for line in lines], "raw_line STRING")
    silver, rejects, valid = silver_frames(bronze)
    assert bronze.count() == 12
    assert valid.count() == 4
    assert rejects.count() == 8
    assert silver.count() == 3
    assert silver.filter("estimated_minutes < 0").count() == 0
    gold = {r.streamer_username: r.asDict() for r in gold_by_streamer(silver).collect()}
    assert gold["alpha"]["interactions"] == 2
    assert gold["alpha"]["unique_users"] == 2
    assert gold["alpha"]["estimated_minutes"] == 20
    assert gold["beta"]["estimated_minutes"] == 30
    # Bordes adicionales: ANSI, overflow, comillas, ceros y formato decimal.
    edges = ['1,11,"a",0,1', "0,11,a,0,1", "1,0,a,0,1",
             "1,11,a,0,2147483648", "1.5,11,a,0,1", " 1 ,11, alpha ,0,2"]
    s2, r2, _ = silver_frames(spark.createDataFrame([(x,) for x in edges], "raw_line STRING"))
    assert r2.count() == 5
    assert s2.count() == 1
    assert s2.first().streamer_username == "alpha"
    assert s2.exceptAll(silver.filter("user_id = 1")).count() == 0
    # Transformar por segunda vez la misma Bronze produce el mismo contenido.
    repeated, _, _ = silver_frames(bronze)
    assert silver.exceptAll(repeated).count() == repeated.exceptAll(silver).count() == 0
    return {"fixture": "ARTIFICIAL", "input_rows": 12, "silver_rows": 3,
            "rejected_rows": 8, "duplicate_rows": 1, "estimated_minutes": 50,
            "edge_rows": 6, "status": "PASS"}
