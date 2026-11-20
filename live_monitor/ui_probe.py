"""Prueba del código Streamlit real (AppTest), sin sustituir datos de SQLite."""
import json
from streamlit.testing.v1 import AppTest


def main():
    app = AppTest.from_file("/app/live_monitor/dashboard.py", default_timeout=30).run()
    if app.exception:
        raise RuntimeError(str(app.exception))
    metrics = {metric.label: metric.value for metric in app.metric}
    if "Eventos únicos procesados por Spark" not in metrics:
        raise RuntimeError("Dashboard todavía no muestra resultados persistidos")
    print(json.dumps({"dashboard_metrics": metrics}, ensure_ascii=False))


if __name__ == "__main__":
    main()
