# Databricks notebook source
# Ejecutar, completar el widget project_root y volver a ejecutar.
# Ruta absoluta de la carpeta del proyecto en Workspace, sin /notebooks al final.
dbutils.widgets.text("project_root", "")
project_root = dbutils.widgets.get("project_root").rstrip("/")
if not project_root.startswith("/Workspace/"):
    raise ValueError("Completa project_root con /Workspace/.../twitch-analytics-lakehouse y reejecuta")
import sys
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from twitch_lakehouse.config import Settings
cfg = Settings()  # Cambiar catálogo/esquema/volumen aquí para todas las etapas.
print({"namespace": cfg.namespace, "raw_path": cfg.raw_path})
