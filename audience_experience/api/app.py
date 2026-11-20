"""I5 read adapter: no DDL, DML, migrations, producer or Spark control."""
import asyncio
import hashlib
import json
import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from live_monitor.storage import snapshot


class Metrics(BaseModel):
    total: int
    valid: int
    invalid: int
    streamer_interactions_total: int
    estimated_minutes_total: int


class SnapshotResponse(BaseModel):
    schema_version: str = "1.0"
    revision: str
    observed_at: str
    status: str
    selection: dict
    metrics: Metrics
    streamers: list[dict]
    activity: list[dict]
    runs: list[dict]
    consumer: dict | None
    last_batch: dict | None
    transport_total: int
    scopes: dict


def read_snapshot(path: Path, run_id: str | None = None):
    raw = snapshot(path=path, run_id=run_id)
    metrics = {key: raw[key] for key in Metrics.model_fields}
    cumulative = 0
    activity = []
    for point in raw["activity"]:
        cumulative += point["events"]
        activity.append({**point, "cumulative_events": cumulative})
    runs = []
    for run in raw["runs"]:
        manifest = json.loads(run["manifest"])
        runs.append({"run_id": run["run_id"], "producer_status": run["producer_status"],
                     "acknowledged": run["acknowledged"], "requested": manifest["count"],
                     "source_name": manifest["source_name"]})
    consumer = raw["consumer"]
    if consumer:
        age = max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(consumer["heartbeat"])).total_seconds())
        consumer = {"state": consumer["state"], "heartbeat": consumer["heartbeat"],
                    "stale": age > 15, "heartbeat_age_seconds": round(age, 1)}
    last_batch = raw["batches"][0] if raw["batches"] else None
    if last_batch:
        last_batch = {**last_batch, "offsets": json.loads(last_batch["offsets"])}
    result = {"schema_version": "1.0", "observed_at": raw["read_at"],
        "status": "ready" if raw["total"] else "empty", "selection": {"run_id": run_id},
        "metrics": metrics, "streamers": [{**row, "rank": rank} for rank, row in enumerate(raw["streamers"], 1)],
        "activity": activity[-300:], "runs": runs, "consumer": consumer,
        "last_batch": last_batch, "transport_total": raw["transport_total"],
        "scopes": {"metrics": "selected_run" if run_id else "all_runs", "consumer": "global",
                   "last_batch": "global", "transport_total": "global", "activity": "processing_time"}}
    result["revision"] = hashlib.sha256(json.dumps({k: v for k, v in result.items() if k != "observed_at"},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
    return SnapshotResponse.model_validate(result).model_dump()


def create_app(db_path=None, interval=2.0):
    path = Path(db_path or os.environ.get("I4_SQLITE_PATH", "/state/monitor.sqlite"))
    keeper = None
    keeper_lock = threading.Lock()

    def read(run_id):
        nonlocal keeper
        # Una conexión RO sin transacción mantiene el ciclo de vida WAL. No crea
        # auxiliares en un volumen RO: si faltan, espera al escritor I4 existente.
        with keeper_lock:
            if keeper is None:
                candidate = sqlite3.connect(path.resolve().as_uri() + "?mode=ro",
                    uri=True, check_same_thread=False, isolation_level=None, timeout=3)
                try:
                    cursor = candidate.execute("SELECT name FROM sqlite_master LIMIT 1")
                    cursor.fetchall()
                    cursor.close()
                except BaseException:
                    candidate.close()
                    raise
                keeper = candidate
        return read_snapshot(path, run_id)

    @asynccontextmanager
    async def lifespan(_app):
        yield
        if keeper is not None:
            keeper.close()

    app = FastAPI(title="Twitch Audience Live · read API", version="1.0.0", lifespan=lifespan)

    async def current(run_id=None):
        try:
            return await asyncio.to_thread(read, str(run_id) if run_id else None)
        except (sqlite3.Error, OSError):
            raise HTTPException(503, detail={"code": "storage_unavailable",
                "message": "Resultados persistidos no disponibles. Se conserva I4 sin cambios."})

    @app.get("/api/v1/health")
    def health():
        return {"service": "audience-experience-api", "status": "ok", "read_only": True}

    @app.get("/api/v1/snapshot", response_model=SnapshotResponse)
    async def get_snapshot(response: Response, run_id: UUID | None = None):
        response.headers["Cache-Control"] = "no-store"
        return await current(run_id)

    @app.websocket("/api/v1/stream")
    async def stream(socket: WebSocket, run_id: UUID | None = None):
        origin = socket.headers.get("origin")
        if origin and urlsplit(origin).netloc != socket.headers.get("host"):
            await socket.close(code=1008)
            return
        await socket.accept()
        try:
            while True:
                try:
                    await socket.send_json({"type": "snapshot", "data": await current(run_id)})
                except HTTPException:
                    await socket.send_json({"type": "unavailable", "code": "storage_unavailable"})
                # Espera desconexión o siguiente lectura, sin busy loop.
                try:
                    await asyncio.wait_for(socket.receive_text(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
        except WebSocketDisconnect:
            pass
    return app


app = create_app()
