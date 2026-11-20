"""Un escritor Spark; lectores SQLite en snapshot, sin ficheros JSON parciales."""
import json
import sqlite3
from pathlib import Path
from contextlib import contextmanager
from live_monitor.common import state_dir, utc_now

DDL = """
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY, manifest TEXT NOT NULL,
  producer_status TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS events(event_key TEXT PRIMARY KEY, run_id TEXT NOT NULL,
  payload_hash TEXT NOT NULL, valid INTEGER NOT NULL, errors TEXT NOT NULL,
  streamer TEXT, estimated_minutes INTEGER, batch_id INTEGER NOT NULL, completed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transport(topic TEXT, partition INTEGER, offset INTEGER,
  event_key TEXT NOT NULL, batch_id INTEGER NOT NULL, PRIMARY KEY(topic,partition,offset));
CREATE TABLE IF NOT EXISTS batches(pipeline_id TEXT, batch_id INTEGER, completed_at TEXT,
  input_rows INTEGER, new_events INTEGER, valid_events INTEGER, invalid_events INTEGER,
  offsets TEXT NOT NULL, PRIMARY KEY(pipeline_id,batch_id));
CREATE TABLE IF NOT EXISTS streamer_metrics(run_id TEXT, streamer TEXT, interactions INTEGER,
  estimated_minutes INTEGER, PRIMARY KEY(run_id,streamer));
CREATE TABLE IF NOT EXISTS consumer_status(id INTEGER PRIMARY KEY CHECK(id=1),
  state TEXT, heartbeat TEXT, detail TEXT);
"""


def connect(path=None, readonly=False):
    path = path or state_dir() / "monitor.sqlite"
    conn = (sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=15)
            if readonly else sqlite3.connect(str(path), timeout=30))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    if not readonly:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript(DDL)
    return conn


@contextmanager
def writer_lock(path):
    """flock exclusivo liberado por el SO al morir: no locks obsoletos tras crash."""
    import fcntl  # runtime Linux de Compose
    with open(path, "a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Ya hay un proceso con este rol; no iniciar otro") from error
        yield


def status(state, detail="", path=None):
    with connect(path) as conn:
        conn.execute("INSERT INTO consumer_status VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE "
                     "SET state=excluded.state,heartbeat=excluded.heartbeat,detail=excluded.detail",
                     (state, utc_now(), detail))
    conn.close()


def commit_batch(conn, pipeline_id, batch_id, rows, aggregates, offsets, completed_at=None):
    """Llamar bajo BEGIN IMMEDIATE. No hace commit: ledger/agregados son atómicos."""
    if conn.execute("SELECT 1 FROM batches WHERE pipeline_id=? AND batch_id=?",
                    (pipeline_id, batch_id)).fetchone():
        return False
    completed = completed_at or utc_now()
    new_valid = new_invalid = 0
    new_minutes = 0
    for row in rows:
        old = conn.execute("SELECT payload_hash FROM events WHERE event_key=?", (row["event_key"],)).fetchone()
        if old and old[0] != row["payload_hash"]:
            raise ValueError("Colisión de identidad: payload distinto para event_key existente")
        cursor = conn.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?)", (
            row["event_key"], row["run_id"], row["payload_hash"], int(row["valid"]), row["errors"],
            row["streamer_username"], row["estimated_minutes"], batch_id, completed))
        if cursor.rowcount:
            new_valid += int(row["valid"])
            new_invalid += int(not row["valid"])
            if row["valid"]:
                new_minutes += row["estimated_minutes"]
        conn.execute("INSERT OR IGNORE INTO transport VALUES(?,?,?,?,?)", (
            row["topic"], row["partition"], row["offset"], row["event_key"], batch_id))
    if sum(int(a["interactions"]) for a in aggregates) != new_valid:
        raise ValueError("Agregado Spark no concilia con eventos nuevos válidos")
    if sum(int(a["estimated_minutes"]) for a in aggregates) != new_minutes:
        raise ValueError("Minutos Spark no concilian con eventos nuevos válidos")
    for agg in aggregates:
        conn.execute("INSERT INTO streamer_metrics VALUES(?,?,?,?) ON CONFLICT(run_id,streamer) "
                     "DO UPDATE SET interactions=interactions+excluded.interactions, "
                     "estimated_minutes=estimated_minutes+excluded.estimated_minutes",
                     (agg["run_id"], agg["streamer_username"], agg["interactions"], agg["estimated_minutes"]))
    conn.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?)", (
        pipeline_id, batch_id, completed, len(rows), new_valid + new_invalid,
        new_valid, new_invalid, json.dumps(offsets, sort_keys=True)))
    return True


def snapshot(path=None, run_id=None):
    """Una transacción de lectura coherente para TODOS los widgets."""
    conn = connect(path, readonly=True)
    try:
        conn.execute("BEGIN")
        clause, args = (" WHERE run_id=?", (run_id,)) if run_id else ("", ())
        totals = dict(conn.execute("SELECT COUNT(*) AS total, COALESCE(SUM(valid),0) AS valid, "
                                  "COUNT(*)-COALESCE(SUM(valid),0) AS invalid FROM events" + clause, args).fetchone())
        agg_totals = dict(conn.execute("SELECT COALESCE(SUM(interactions),0) AS streamer_interactions_total, "
            "COALESCE(SUM(estimated_minutes),0) AS estimated_minutes_total FROM streamer_metrics" + clause, args).fetchone())
        streamers = [dict(r) for r in conn.execute(
            "SELECT streamer,SUM(interactions) AS interactions,SUM(estimated_minutes) AS estimated_minutes "
            "FROM streamer_metrics" + clause + " GROUP BY streamer ORDER BY interactions DESC,streamer LIMIT 20", args)]
        batches = [dict(r) for r in conn.execute("SELECT * FROM batches ORDER BY batch_id DESC LIMIT 100")]
        activity = [dict(r) for r in conn.execute(
            "SELECT completed_at,COUNT(*) AS events FROM events" + clause + " GROUP BY completed_at ORDER BY completed_at", args)]
        runs = [dict(r) for r in conn.execute("SELECT * FROM runs ORDER BY rowid DESC")]
        heartbeat = conn.execute("SELECT * FROM consumer_status WHERE id=1").fetchone()
        transports = conn.execute("SELECT COUNT(*) FROM transport").fetchone()[0]
        return {**totals, **agg_totals, "streamers": streamers, "batches": batches, "activity": activity,
                "runs": runs, "consumer": dict(heartbeat) if heartbeat else None,
                "transport_total": transports, "read_at": utc_now()}
    finally:
        conn.rollback()
        conn.close()
