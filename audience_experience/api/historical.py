"""Lector de exportación agregada Gold I3: local, explícito y solo lectura."""
import json
import math
from pathlib import Path

MAX_BYTES = 2_000_000
REQUIRED_METRICS = ("bronze_rows", "silver_rows", "rejected_rows",
                    "gold_streamers", "estimated_minutes")


def load_historical(path: Path):
    if not path.is_file() or path.stat().st_size > MAX_BYTES:
        raise ValueError("Archivo histórico ausente o excesivo")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != "i3-historical-v1":
        raise ValueError("Contrato histórico incorrecto")
    if data.get("status") != "PASS" or not isinstance(data.get("release"), str):
        raise ValueError("Publicación I3 no validada")
    if not isinstance(data.get("exported_at"), str) or not data["exported_at"].endswith("Z"):
        raise ValueError("Falta fecha de exportación UTC")
    metrics = data.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("Métricas ausentes")
    for key in REQUIRED_METRICS:
        value = metrics.get(key)
        if type(value) is not int or value < 0:
            raise ValueError(f"Métrica inválida: {key}")
    if metrics["silver_rows"] == 0 or metrics["gold_streamers"] == 0:
        raise ValueError("Publicación I3 vacía")
    if metrics["silver_rows"] > metrics["bronze_rows"]:
        raise ValueError("Población inconsistente")
    for key in ("top10_share", "top100_share"):
        value = metrics.get(key)
        if type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"Participación inválida: {key}")
    if metrics["top10_share"] > metrics["top100_share"]:
        raise ValueError("Concentración no ordenada")
    ranking = data.get("ranking")
    duration = data.get("duration")
    if not isinstance(ranking, list) or len(ranking) > 20:
        raise ValueError("Ranking inválido")
    if not isinstance(duration, list) or len(duration) != 5:
        raise ValueError("Distribución incompleta")
    for row in ranking:
        if not isinstance(row, dict) or not isinstance(row.get("streamer_username"), str):
            raise ValueError("Fila de ranking inválida")
        if not row["streamer_username"] or type(row.get("interactions")) is not int or row["interactions"] < 0:
            raise ValueError("Ranking numérico inválido")
        # React usa estos campos: no aceptar un PASS que luego renderice NaN.
        for key in ("unique_users", "recurring_users", "estimated_minutes"):
            if type(row.get(key)) is not int or row[key] < 0:
                raise ValueError(f"Ranking {key} ausente o inválido")
        users, recurring = row["unique_users"], row["recurring_users"]
        if users == 0 or users > row["interactions"] or recurring > users:
            raise ValueError("Denominador o numerador de recurrencia inválido")
        rate = row.get("recurrence_rate")
        if (type(rate) not in (int, float) or not math.isfinite(rate) or
                not math.isclose(rate, recurring / users, rel_tol=0, abs_tol=1e-12)):
            raise ValueError("Recurrencia no concilia con su denominador")
    if sorted(row.get("duration_bucket") for row in duration if isinstance(row, dict)) != list(range(5)):
        raise ValueError("Buckets de duración inválidos")
    if any(type(row.get("interactions")) is not int or row["interactions"] < 0 for row in duration):
        raise ValueError("Conteos de duración inválidos")
    if sum(row["interactions"] for row in duration) != metrics["silver_rows"]:
        raise ValueError("Distribución no concilia con Silver")
    if not isinstance(data.get("source_namespace"), str) or not data["source_namespace"]:
        raise ValueError("Origen no identificado")
    return data
