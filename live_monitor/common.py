import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def state_dir():
    path = Path(os.environ.get("STATE_DIR", "work/i4"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def event_id(run_id, source_sha256, source_row):
    return hashlib.sha256(f"{run_id}|{source_sha256}|{source_row}".encode()).hexdigest()


def source_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
