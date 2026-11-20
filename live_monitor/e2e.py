"""Prueba real optativa Docker/CSV: publicación → Spark → SQLite → Streamlit.

Ejecutar desde raíz del repositorio. No usa un consumidor Python para leer eventos.
Conserva evidencia y servicios/volúmenes incluso si falla.
"""
import argparse
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from live_monitor.common import source_digest, event_id, utc_now


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--out", default="work/i4-evidence")
    args = parser.parse_args()
    if args.count != 100:
        parser.error("Esta aceptación inicial exige exactamente 100 eventos")
    root = Path(args.out) / str(uuid.uuid4())
    root.mkdir(parents=True)
    env = dict(os.environ, TWITCH_CSV=str(Path(args.csv).resolve()))
    base = ["docker", "compose", "-f", "live_monitor/compose.yaml"]
    run_id = root.name
    report = {"run_id": run_id, "started_at": utc_now(), "status": "RUNNING"}

    def command(*cmd, check=True):
        result = subprocess.run(base + list(cmd), env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
        with (root / "commands.log").open("a", encoding="utf-8") as log:
            log.write(json.dumps(list(cmd)) + "\n" + result.stdout + result.stderr + "\n")
        if check and result.returncode:
            raise RuntimeError(f"Comando falló ({result.returncode}): {cmd}; ver commands.log")
        return result.stdout

    def tool(module, *arguments):
        output = command("run", "--rm", "--no-deps", "tools", "python3", "-m", module, *arguments)
        return json.loads([line for line in output.splitlines() if line.startswith("{")][-1])

    def snapshot():
        data = tool("live_monitor.control", "status", "--run-id", run_id)
        with (root / "snapshots.jsonl").open("a", encoding="utf-8") as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")
        print(f"run_id={run_id} únicos={data['total']} válidos={data['valid']} inválidos={data['invalid']}", flush=True)
        return data

    def wait(predicate, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = snapshot()
                if (data.get("consumer") or {}).get("state") == "FAILED":
                    raise RuntimeError("Consumidor Spark falló: " + data["consumer"]["detail"])
                if predicate(data):
                    return data
            except (IndexError, json.JSONDecodeError):
                pass
            time.sleep(2)
        raise TimeoutError("No se alcanzó el resultado esperado; conservar estado para diagnóstico")

    try:
        command("up", "-d", "kafka", "spark", "dashboard")
        wait(lambda d: d["consumer"] and d["consumer"]["state"] == "RUNNING")
        baseline_ui = tool("live_monitor.ui_probe")
        report["ui_before"] = baseline_ui
        producer_args = ["run", "--rm", "--no-deps", "producer", "python3", "-m", "live_monitor.producer",
                         "--csv", "/input/100k_a.csv", "--run-id", run_id, "--count", "100", "--rate", str(args.rate)]
        with (root / "producer.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen(base + producer_args, env=env, stdout=log, stderr=subprocess.STDOUT)
            # Dejar evidencia de que se observa incremento antes del total.
            partial = wait(lambda d: 0 < d["total"] < 100)
            report["intermediate_total"] = partial["total"]
            final = wait(lambda d: d["total"] == 100)
            if process.wait(timeout=90) != 0:
                raise RuntimeError("Productor falló; ver producer.log")
        acknowledgements = [json.loads(line) for line in (root / "producer.log").read_text(encoding="utf-8").splitlines()
                            if line.startswith("{") and '"event_id"' in line]
        if len(acknowledgements) != 100:
            raise AssertionError("No hay exactamente 100 confirmaciones de Kafka")
        digest = source_digest(args.csv)
        if {a["event_id"] for a in acknowledgements} != {event_id(run_id, digest, i) for i in range(1, 101)}:
            raise AssertionError("Identidades publicadas no corresponden al rango real")
        if (final["valid"], final["invalid"]) != (100, 0):
            raise AssertionError("Las 100 filas reales no conciliaron como válidas; investigar")
        if final["streamer_interactions_total"] != 100:
            raise AssertionError("Agregado completo de streamers no concilia")
        with Path(args.csv).open(encoding="utf-8") as source:
            expected_minutes = sum((int(fields[4]) - int(fields[3])) * 10
                for fields in [next(source).strip().split(",") for _ in range(100)])
        if final["estimated_minutes_total"] != expected_minutes:
            raise AssertionError("Minutos agregados no concilian con las filas del CSV real")
        ui_after = tool("live_monitor.ui_probe")
        report["ui_after"] = ui_after
        before = int(baseline_ui["dashboard_metrics"]["Eventos únicos procesados por Spark"])
        after = int(ui_after["dashboard_metrics"]["Eventos únicos procesados por Spark"])
        if after - before != 100:
            raise AssertionError("Streamlit no refleja incremento de 100 eventos")
        # Reiniciar de verdad el proceso consumidor, conservar mismo checkpoint y DB.
        command("restart", "spark")
        wait(lambda d: d["consumer"] and d["consumer"]["state"] == "RUNNING")
        after_restart = snapshot()
        if after_restart["total"] != 100:
            raise AssertionError("Reinicio modificó conteo")
        # Reenviar la misma ejecución: nuevos offsets, mismas identidades.
        transport_before = after_restart["transport_total"]
        command(*producer_args)
        retried = wait(lambda d: d["transport_total"] >= transport_before + 100)
        if retried["total"] != 100 or retried["valid"] != 100:
            raise AssertionError("Reenvío infló métricas")
        report.update(status="PASS", kafka_acknowledged=100, spark_unique=100,
                      restart_unique=after_restart["total"], replay_unique=retried["total"],
                      transport_after=retried["transport_total"], source_sha256=digest,
                      finished_at=utc_now(), note="AppTest valida render Streamlit; conservar además captura del navegador")
    except BaseException as error:
        report.update(status="FAILED", error=repr(error), finished_at=utc_now())
        raise
    finally:
        command("logs", "--no-color", "spark", "kafka", "dashboard", check=False)
        (root / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Evidencia conservada en {root}", flush=True)


if __name__ == "__main__":
    main()
