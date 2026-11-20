"""Reproducción controlada de líneas REALES; reintento con identidad estable."""
import argparse
import json
import os
import signal
import time
import uuid
from pathlib import Path
from live_monitor.common import event_id, source_digest, state_dir, utc_now
from live_monitor.storage import connect, writer_lock


def publish(path, run_id, count=100, rate=5.0, start_row=1):
    from kafka import KafkaProducer
    if not 1 <= count <= 10000 or rate <= 0 or start_row < 1:
        raise ValueError("count 1..10000, rate>0, start_row>=1")
    run_id = str(uuid.UUID(run_id))
    root = state_dir()
    with writer_lock(root / "producer.lock"):
        # Leer solo el rango solicitado; nunca reproducir más por accidente.
        digest = source_digest(path)
        lines = []
        with Path(path).open(encoding="utf-8", newline="") as source:
            for ordinal, line in enumerate(source, 1):
                if ordinal >= start_row and len(lines) < count:
                    lines.append((ordinal, line.rstrip("\r\n")))
                if len(lines) == count:
                    break
        if len(lines) != count:
            raise ValueError("CSV no contiene todas las filas solicitadas; no se publica nada")
        manifest = json.dumps({"source_sha256": digest, "source_name": Path(path).name,
                               "start_row": start_row, "count": count}, sort_keys=True)
        conn = connect()
        try:
            previous = conn.execute("SELECT manifest FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if previous and previous[0] != manifest:
                raise ValueError("run_id ya vinculado a otra fuente/rango; no reutilizarlo")
            with conn:
                conn.execute("INSERT INTO runs VALUES(?,?,'STARTING',0) ON CONFLICT(run_id) "
                             "DO UPDATE SET producer_status='STARTING',acknowledged=0", (run_id, manifest))
            stop_path = root / f"stop-{run_id}"
            # Una señal previa NO se borra: usar resume explícito desde control.
            stopped = False
            def stop(*_):
                nonlocal stopped
                stopped = True
            signal.signal(signal.SIGINT, stop)
            signal.signal(signal.SIGTERM, stop)
            producer = KafkaProducer(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                acks="all", retries=5, key_serializer=lambda x: x.encode(),
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode())
            acknowledged = 0
            log_path = root / f"producer-{run_id}.jsonl"
            try:
                for ordinal, raw_line in lines:
                    if stopped or stop_path.exists():
                        break
                    identity = event_id(run_id, digest, ordinal)
                    event = {"run_id": run_id, "source_sha256": digest, "source_row": ordinal,
                             "event_id": identity, "historical_replay": True,
                             "produced_at": utc_now(), "raw_line": raw_line}
                    result = producer.send(os.environ.get("KAFKA_TOPIC", "twitch.i4.historical"),
                                           key=identity, value=event).get(timeout=30)
                    acknowledged += 1
                    record = {"run_id": run_id, "event_id": identity, "source_row": ordinal,
                              "topic": result.topic, "partition": result.partition,
                              "offset": result.offset, "acknowledged_at": utc_now()}
                    with log_path.open("a", encoding="utf-8") as log:
                        log.write(json.dumps(record) + "\n")
                    print(json.dumps(record), flush=True)
                    with conn:
                        conn.execute("UPDATE runs SET producer_status='PUBLISHING',acknowledged=? WHERE run_id=?",
                                     (acknowledged, run_id))
                    # Espera interrumpible sin promesa de precisión de temporización.
                    deadline = time.monotonic() + 1 / rate
                    while time.monotonic() < deadline and not stopped and not stop_path.exists():
                        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
                producer.flush(timeout=30)
            except BaseException:
                with conn:
                    conn.execute("UPDATE runs SET producer_status='FAILED' WHERE run_id=?", (run_id,))
                raise
            finally:
                producer.close(timeout=30)
            final = "COMPLETED" if acknowledged == count else "STOPPED"
            with conn:
                conn.execute("UPDATE runs SET producer_status=? WHERE run_id=?", (final, run_id))
            return {"run_id": run_id, "acknowledged": acknowledged, "status": final}
        finally:
            conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--start-row", type=int, default=1)
    args = parser.parse_args()
    print(json.dumps(publish(args.csv, args.run_id or str(uuid.uuid4()), args.count, args.rate, args.start_row)))


if __name__ == "__main__":
    main()
