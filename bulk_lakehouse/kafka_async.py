"""Productor 100k preparado, NO auto-publicación; ACK SQLite y ventana acotada."""
import argparse
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import time
import uuid
from kafka_ingest.contracts import canonical, digest, event_id
from bulk_lakehouse.pipeline import validate_manifest
from bulk_lakehouse.source import save
from twitch_lakehouse.full_dataset_prepare import sha256


def rows(root, m):
    import pyarrow.parquet as pq
    for part in m["parts"]:
        path = root / part["file"]
        if path.stat().st_size != part["bytes"] or sha256(path) != part["sha256"]:
            raise ValueError("Fragmento local alterado")
        expected = part["first_row"]
        for batch in pq.ParquetFile(path).iter_batches(batch_size=1000):
            for r in batch.to_pylist():
                if r["source_row"] != expected:
                    raise ValueError("Ordinal global no concilia con el rango")
                expected += 1
                if hashlib.sha256(r["raw_line"].encode()).hexdigest() != r["raw_sha256"]:
                    raise ValueError("Hash de fila fuente no coincide")
                yield r
        if expected != part["last_row"]+1:
            raise ValueError("Cobertura de fragmento no coincide")


def envelope(m, load_id, row):
    return {"twitch_load_id": load_id, "twitch_source_row": row["source_row"],
        "source_sha256": m["source_sha256"], "event_id": event_id(m["source_sha256"],row["source_row"]),
        "raw_line": row["raw_line"], "raw_sha256": row["raw_sha256"], "historical_replay": True,
        "produced_at": datetime.now(timezone.utc).isoformat()}


def factory():
    from kafka import KafkaProducer
    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    return KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        security_protocol="SASL_SSL", sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM","SCRAM-SHA-256"),
        sasl_plain_username=os.environ["KAFKA_USERNAME"], sasl_plain_password=os.environ["KAFKA_PASSWORD"],
        ssl_cafile=os.environ.get("KAFKA_SSL_CA_FILE") or None, ssl_check_hostname=True,
        acks="all", enable_idempotence=True, retries=5, linger_ms=20, batch_size=65536,
        buffer_memory=8*1024*1024, max_in_flight_requests_per_connection=5,
        compression_type="gzip", key_serializer=lambda v:v.encode(), value_serializer=lambda v:canonical(v).encode())


def estimate(root):
    root=Path(root)
    m=validate_manifest(json.loads((root/"manifest.json").read_text()),100000)
    total,count,maximum=0,0,0
    for row in rows(root,m):
        e=envelope(m,"00000000-0000-4000-8000-000000000000",row)
        size=len(canonical(e).encode())+len(e["event_id"].encode())
        total+=size
        count+=1
        maximum=max(maximum,size)
    return {"scope":"CAPACITY_ESTIMATE_NOT_PUBLICATION","source_sha256":m["source_sha256"],
        "rows":count,"estimated_payload_and_key_bytes":total,"average_bytes":total/count,
        "maximum_bytes":maximum,"excludes":"Kafka protocol overhead, replication, compression and storage amplification"}


def publish(root, state, review, producer_factory=factory, max_pending=256, bytes_per_second=65536, commit_every=64):
    root,state=Path(root),Path(state)
    m=validate_manifest(json.loads((root/"manifest.json").read_text()),100000)
    if not (1<=max_pending<=1024 and 1<=commit_every<=256 and 1<=bytes_per_second<=262144):
        raise ValueError("Ventana/velocidad fuera del piloto revisable")
    if (review.get("status")!="APPROVED" or review.get("rows")!=100000
        or review.get("source_sha256")!=m["source_sha256"]
        or review.get("plan_change_allowed") is not False or review.get("additional_charges_allowed") is not False
        or not review.get("credits_and_storage_checked")
        or not review.get("recovery_reviewed")
        or not str(review.get("topic","")).startswith("twitch.bulk.")
        or review.get("topic")=="twitch.pilot.twitch1000"):
        raise ValueError("Revisión de origen/cuotas/costos/topic/recuperación requerida antes de publicar")
    state.mkdir(parents=True,exist_ok=True)
    lock=state/"producer.lock"
    fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
    os.close(fd)
    conn=None
    producer=None
    started=time.monotonic()
    stop=False
    prior_handlers={}
    def request_stop(*_):
        nonlocal stop
        stop=True
    try:
        identity={"manifest_sha256":digest(canonical(m)),"topic":review["topic"],"source_sha256":m["source_sha256"]}
        binding_file=state/"binding.json"
        if binding_file.exists():
            bound=json.loads(binding_file.read_text())
            if bound["identity"]!=identity: raise ValueError("Estado ligado a otro manifiesto/topic")
        else:
            bound={"identity":identity,"load_id":str(uuid.uuid4())}
            save(binding_file,bound)
        conn=sqlite3.connect(state/"acks.sqlite")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("CREATE TABLE IF NOT EXISTS ack(source_row INTEGER PRIMARY KEY,event_id TEXT NOT NULL,partition_id INTEGER NOT NULL,offset_id INTEGER NOT NULL)")
        conn.commit()
        producer=producer_factory()
        if producer.partitions_for(review["topic"]) != set(review["partitions"]):
            raise ValueError("Topic/particiones no coinciden; no crear ni modificar automáticamente")
        for sig in (signal.SIGINT,signal.SIGTERM):
            prior_handlers[sig]=signal.signal(sig,request_stop)
        pending=deque()
        peak,uncommitted=0,0
        next_send=time.monotonic()
        def durable(future,row,eid):
            nonlocal uncommitted
            result=future.get(timeout=60)
            if result.topic!=review["topic"]: raise ValueError("ACK de otro topic")
            conn.execute("INSERT INTO ack VALUES(?,?,?,?)",(row,eid,result.partition,result.offset))
            uncommitted+=1
            if uncommitted>=commit_every:
                conn.commit()
                uncommitted=0
        for row in rows(root,m):
            if stop or (state/"STOP").exists(): break
            if conn.execute("SELECT 1 FROM ack WHERE source_row=?",(row["source_row"],)).fetchone(): continue
            e=envelope(m,bound["load_id"],row)
            size=len(canonical(e).encode())+len(e["event_id"].encode())
            if size>review["max_message_bytes"]: raise ValueError("Mensaje supera revisión de tamaño")
            delay=next_send-time.monotonic()
            if delay>0: time.sleep(delay)
            next_send=max(next_send,time.monotonic())+size/bytes_per_second
            future=producer.send(review["topic"],key=e["event_id"],value=e)
            pending.append((future,row["source_row"],e["event_id"]))
            peak=max(peak,len(pending))
            if len(pending)>=max_pending: durable(*pending.popleft())
        while pending: durable(*pending.popleft())
        producer.flush(timeout=60)
        conn.commit()
        acknowledged=conn.execute("SELECT COUNT(*) FROM ack").fetchone()[0]
        result={"scope":"KAFKA_ACK_ONLY_NOT_DATABRICKS_PROCESSING","status":"ACKED" if acknowledged==100000 else "STOPPED",
            "load_id":bound["load_id"],"acknowledged_distinct_source_rows":acknowledged,"peak_pending":peak,
            "elapsed_seconds":time.monotonic()-started,"topic":review["topic"],"source_sha256":m["source_sha256"]}
        save(state/f"attempt-{uuid.uuid4().hex}.json",result)
        return result
    finally:
        for sig,handler in prior_handlers.items(): signal.signal(sig,handler)
        if producer is not None: producer.close(timeout=60)
        if conn is not None: conn.close()
        lock.unlink()


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--parts",required=True)
    p.add_argument("--state")
    p.add_argument("--review")
    p.add_argument("--publish",action="store_true")
    args=p.parse_args()
    try:
        result=publish(args.parts,args.state,json.loads(Path(args.review).read_text())) if args.publish else estimate(args.parts)
        print(canonical(result))
    except Exception as error:
        print(canonical({"status":"FAILED","error_type":type(error).__name__,"action":"Conservar estado y revisar; no imprimir excepciones de autenticación"}))
        raise SystemExit(1) from None


if __name__=="__main__": main()
