"""Lector estricto y de solo lectura de una exportación Gold I3.

El JSON es producido por notebook 10 en Databricks y transferido manualmente.
Este módulo no conecta con Databricks ni requiere credenciales en el frontend.
"""
import json
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, model_validator

class ExactModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

class Quality(ExactModel):
    status: str
    scope: str
    bronze_rows: int = Field(ge=0)
    silver_rows: int = Field(gt=0)
    rejected_rows: int = Field(ge=0)
    duplicates_removed_inferred: int = Field(ge=0)
    silver_duplicate_keys: int = Field(ge=0)
    gold_streamers: int = Field(gt=0)
    estimated_minutes: int = Field(ge=0)

class Streamer(ExactModel):
    streamer_username: str
    interactions: int = Field(ge=0)
    unique_users: int = Field(ge=0)
    unique_streams: int = Field(ge=0)
    recurring_users: int = Field(ge=0)
    recurrence_rate: float = Field(ge=0, le=1)
    estimated_minutes: int = Field(ge=0)

class Duration(ExactModel):
    duration_bucket: int = Field(ge=0, le=4)
    interactions: int = Field(ge=0)
    estimated_minutes: int = Field(ge=0)

class Activity(ExactModel):
    relative_minutes: int = Field(ge=0)
    connection_starts: int = Field(ge=0)

class HistoricalExport(ExactModel):
    schema_version: str
    kind: str
    release: str
    source_namespace: str
    exported_at: datetime
    source_versions: dict[str, int]
    quality: Quality
    top_streamers: list[Streamer] = Field(max_length=20)
    duration_buckets: list[Duration] = Field(max_length=5)
    activity: list[Activity] = Field(max_length=120)
    activity_truncated: bool
    definitions: dict[str, str]

    @model_validator(mode="after")
    def check_consistency(self):
        if self.schema_version != "1.0" or self.kind != "i3_historical_aggregates":
            raise ValueError("Versión o tipo histórico desconocidos")
        if self.quality.status != "PASS" or self.quality.scope != "I1_COUNTS_AND_I3_EXACT_AGGREGATES":
            raise ValueError("Quality no validado")
        q = self.quality
        if q.bronze_rows - q.rejected_rows - q.duplicates_removed_inferred != q.silver_rows:
            raise ValueError("Bronze y Silver no concilian")
        if q.silver_duplicate_keys:
            raise ValueError("Claves Silver duplicadas")
        if set(self.source_versions) != {"bronze_version","silver_version","rejects_version","gold_version"} or any(
                type(v) is not int or v < 0 for v in self.source_versions.values()):
            raise ValueError("Versiones Delta incorrectas")
        if any(x.recurring_users > x.unique_users or x.interactions > q.silver_rows
               for x in self.top_streamers):
            raise ValueError("Valores de streamer fuera de dominio")
        if len({x.streamer_username for x in self.top_streamers}) != len(self.top_streamers):
            raise ValueError("Streamers duplicados")
        if any(self.top_streamers[i].interactions < self.top_streamers[i + 1].interactions
               for i in range(len(self.top_streamers)-1)):
            raise ValueError("Ranking desordenado")
        if sum(x.interactions for x in self.duration_buckets) != q.silver_rows:
            raise ValueError("Distribución no concilia con Silver")
        if sum(x.estimated_minutes for x in self.duration_buckets) != q.estimated_minutes:
            raise ValueError("Minutos por duración no concilian")
        if len({x.duration_bucket for x in self.duration_buckets}) != len(self.duration_buckets):
            raise ValueError("Buckets duplicados")
        return self

def read_historical(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError("Archivo de exportación Gold I3 no encontrado")
    if path.stat().st_size > 2_000_000:
        raise ValueError("La exportación supera el límite de 2 MB")
    content = json.loads(path.read_text(encoding="utf-8"))
    return HistoricalExport.model_validate(content).model_dump(mode="json")
