"""Inventario previo sin escrituras: no confundir release parcial con ausente."""
from twitch_lakehouse.analytics_pipeline import targets


def inspect_release(spark, cfg, release):
    names = targets(cfg, release)
    present = {key: spark.catalog.tableExists(name) for key, name in names.items()}
    count = sum(present.values())
    state = "ABSENT" if count == 0 else "COMPLETE" if count == len(names) else "PARTIAL"
    return {"release": release, "state": state, "tables": names, "present": present,
            "automatic_write_allowed": False}
