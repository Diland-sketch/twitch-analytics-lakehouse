# Databricks notebook source
# Completar esta ruta una sola vez antes de ejecutar las etapas.
# Ruta absoluta de la carpeta del proyecto en Workspace, sin /notebooks al final.
project_root = ""  # Ejemplo: /Workspace/Users/<usuario>/twitch-analytics-lakehouse
project_root = project_root.rstrip("/")
if not project_root.startswith("/Workspace/"):
    raise ValueError("Edita project_root en notebooks/00_config y asigna la ruta /Workspace/... real")
import sys
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from twitch_lakehouse.config import Settings
cfg = Settings()  # Cambiar catálogo/esquema/volumen aquí para todas las etapas.
print({"namespace": cfg.namespace, "raw_path": cfg.raw_path})
