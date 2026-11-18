"""Configuración sin dependencias de Spark; nunca contiene credenciales."""
from dataclasses import dataclass
import re


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"Identificador no permitido: {value!r}")
    return value


@dataclass(frozen=True)
class Settings:
    catalog: str = "workspace"
    schema: str = "twitch_lakehouse"
    volume: str = "raw_data"
    filename: str = "100k_a.csv"

    def __post_init__(self):
        for value in (self.catalog, self.schema, self.volume):
            identifier(value)
        # I1 procesa exclusivamente este archivo, sin comodines ni histórico completo.
        if self.filename != "100k_a.csv":
            raise ValueError("I1 admite exclusivamente 100k_a.csv")

    @property
    def namespace(self):
        return f"{self.catalog}.{self.schema}"

    @property
    def raw_path(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.filename}"

    def table(self, name):
        return f"{self.namespace}.{identifier(name)}"
