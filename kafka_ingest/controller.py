"""Cursor solo tras PASS remoto; reintentos Jobs con token idempotente por intento."""
import argparse
import json
import os
import time
import urllib.request
import urllib.error
import uuid
from pathlib import Path
from kafka_ingest.contracts import canonical, verify_report
from kafka_ingest.producer import prepare, publish, load_block, save_new


class Jobs:
    def __init__(self):
        from urllib.parse import urlparse
        self.host = os.environ["DATABRICKS_HOST"].rstrip("/")
        parsed = urlparse(self.host)
        if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".databricks.com") or parsed.path or parsed.username:
            raise ValueError("DATABRICKS_HOST debe ser la URL HTTPS raíz del workspace")
        self.token = os.environ["DATABRICKS_TOKEN"]
        self.job_id = int(os.environ["DATABRICKS_BRONZE_JOB_ID"])

    def request(self, route, payload=None):
        request = urllib.request.Request(self.host + "/api/2.2/jobs/" + route,
            data=canonical(payload).encode() if payload is not None else None,
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except (urllib.error.URLError, TimeoutError):
            # Ni cuerpos HTTP ni excepciones externas (pueden contener credenciales).
            raise RuntimeError("Jobs API no disponible; conservar estado y reanudar") from None

    def run(self, manifest, token):
        return self.request("run-now", {"job_id": self.job_id, "idempotency_token": token,
            "job_parameters": {"manifest_json": canonical(manifest)}})["run_id"]

    def wait(self, run_id, timeout=1800):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            run = self.request(f"runs/get?run_id={run_id}")
            state = run["state"]
            if state["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if state.get("result_state") != "SUCCESS":
                    raise RuntimeError("Job no terminó SUCCESS; no avanzar cursor")
                # La salida pertenece al task, no al run padre multi-task.
                task_id = next(t["run_id"] for t in run["tasks"] if t["task_key"] == "bronze")
                output = self.request(f"runs/get-output?run_id={task_id}")
                if output.get("notebook_output", {}).get("truncated"):
                    raise RuntimeError("Salida de auditoría truncada")
                return json.loads(output["notebook_output"]["result"])
            time.sleep(5)
        raise RuntimeError("Tiempo de espera agotado; el Job puede seguir activo. Reanudar sin republicar")


def execute(directory, jobs, publisher=publish):
    root = Path(directory)
    m, _ = load_block(root)
    # Lock conservador: una caída conserva el lock; liberar manualmente tras verificar PID/Job.
    lock = root / "controller.lock"
    descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.close(descriptor)
    try:
        if (root / "cursor.json").exists():
            return json.loads((root / "cursor.json").read_text())
        publisher(root)
        attempt_file = root / "attempt.json"
        if not attempt_file.exists():
            save_new(attempt_file, {"token": str(uuid.uuid4())})
        attempt = json.loads(attempt_file.read_text())
        run_file = root / "job.json"
        if not run_file.exists():
            save_new(run_file, {"run_id": jobs.run(m, attempt["token"])})
        run_id = json.loads(run_file.read_text())["run_id"]
        report = jobs.wait(run_id)
        verify_report(m, report)
        # Conciliar TODOS los ACK persistidos con cobertura de offsets de Bronze.
        coverage = {int(p["partition"]): p for p in report["offsets"]}
        acknowledged_rows = set()
        for line in (root / "acks.jsonl").read_text().splitlines():
            ack = json.loads(line)
            part = coverage.get(ack["partition"])
            if ack["topic"] != m["topic"] or not part or not part["min_offset"] <= ack["offset"] <= part["max_offset"]:
                raise RuntimeError("ACK fuera de cobertura Bronze; cursor conservado")
            from kafka_ingest.contracts import event_id
            if ack["event_id"] != event_id(m["source_sha256"], ack["source_row"]):
                raise RuntimeError("ACK con identidad inválida")
            acknowledged_rows.add(ack["source_row"])
        if acknowledged_rows != set(range(1, m["count"] + 1)):
            raise RuntimeError("No hay ACK de todas las filas fuente")
        if (root / "verified.json").exists():
            if json.loads((root / "verified.json").read_text()) != report:
                raise RuntimeError("Reporte persistido distinto; diagnosticar")
        else:
            save_new(root / "verified.json", report)
        cursor = {"next_source_row": m["count"] + 1, "twitch_load_id": m["twitch_load_id"],
                  "source_sha256": m["source_sha256"], "databricks_run_id": run_id, "status": "PASS"}
        save_new(root / "cursor.json", cursor)
        return cursor
    except Exception as error:
        save_new(root / f"failure-{uuid.uuid4().hex}.json", {"status": "FAILED",
            "error_type": type(error).__name__, "cursor_advanced": False})
        raise
    finally:
        lock.unlink()


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    new = sub.add_parser("prepare")
    new.add_argument("--source", required=True)
    new.add_argument("--block-dir", required=True)
    new.add_argument("--topic", required=True)
    new.add_argument("--cluster-name", required=True)
    run = sub.add_parser("run")
    run.add_argument("--block-dir", required=True)
    run.add_argument("--rate", type=float, default=100.0)
    only_publish = sub.add_parser("publish")
    only_publish.add_argument("--block-dir", required=True)
    only_publish.add_argument("--rate", type=float, default=100.0)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            print(canonical(prepare(args.source, args.block_dir, args.topic, args.cluster_name)))
        elif args.command == "publish":
            lock = Path(args.block_dir) / "controller.lock"
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(descriptor)
            try:
                print(canonical(publish(args.block_dir, rate=args.rate)))
            finally:
                lock.unlink()
        else:
            print(canonical(execute(args.block_dir, Jobs(), lambda root: publish(root, rate=args.rate))))
    except Exception as error:
        print(canonical({"status": "FAILED", "error_type": type(error).__name__,
                         "action": "Revisar evidencia local; no se avanzó automáticamente"}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
