"""Solo lectura: cada cambio de métrica proviene del sink transaccional Spark."""
from datetime import datetime, timezone
import sqlite3
import pandas as pd
import streamlit as st
from live_monitor.storage import snapshot

st.set_page_config(page_title="Twitch Live Monitor", page_icon="📡", layout="wide")
st.title("Twitch · Monitor de reproducción histórica")
st.caption("CSV histórico → Kafka → Spark Structured Streaming → SQLite. No es actividad actual de Twitch ni prueba espectadores activos.")


@st.fragment(run_every="2s")
def monitor():
    try:
        all_data = snapshot()
    except (sqlite3.Error, OSError):
        st.info("Esperando almacenamiento del consumidor Spark. No hay métricas disponibles.")
        return
    runs = [r["run_id"] for r in all_data["runs"]]
    selected = st.selectbox("Ejecución histórica (run_id)", ["Todas"] + runs, key="run_selector")
    data = all_data if selected == "Todas" else snapshot(run_id=selected)
    consumer = data["consumer"]
    stale = True
    if consumer:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(consumer["heartbeat"])).total_seconds()
        stale = age > 15
        st.write(f"**Consumidor Spark:** {'SIN SEÑAL RECIENTE' if stale else consumer['state']} · última señal: {consumer['heartbeat']}")
    else:
        st.warning("Consumidor Spark sin señal registrada")
    a, b, c = st.columns(3)
    a.metric("Eventos únicos procesados por Spark", data["total"])
    b.metric("Registros válidos", data["valid"])
    c.metric("Registros inválidos", data["invalid"])
    st.caption("Los reenvíos con la misma identidad no incrementan estas métricas. Otra ejecución explícita representa otra reproducción.")
    left, right = st.columns(2)
    with left:
        st.subheader("Conexiones válidas por streamer")
        if data["streamers"]:
            st.bar_chart(pd.DataFrame(data["streamers"]).set_index("streamer")[["interactions"]])
        else:
            st.info("Sin conexiones válidas procesadas para esta selección")
    with right:
        st.subheader("Eventos recibidos y completados por Spark")
        if data["activity"]:
            chart = pd.DataFrame(data["activity"]).set_index("completed_at")
            st.line_chart(chart[["events"]])
        st.caption("Tiempo real de procesamiento de la reproducción; no fecha histórica de la conexión.")
    st.subheader("Último micro-lote completado · global")
    if data["batches"]:
        st.json(data["batches"][0])
        st.caption(f"Última actualización de datos confirmada: {data['batches'][0]['completed_at']}")
    else:
        st.info("Aún no hay micro-lotes confirmados")
    st.caption(f"Offsets únicos registrados (global): {data['transport_total']} · lectura del monitor: {data['read_at']}")
    with st.expander("Ejecuciones y confirmaciones del productor"):
        st.dataframe(pd.DataFrame(data["runs"]), hide_index=True)
    with st.expander("Cómo iniciar y detener una reproducción"):
        st.write("Usa los comandos de live_monitor/README.md. La reproducción se inicia externamente con run_id, cantidad y velocidad; el monitor solo lee resultados.")
        st.code("docker compose -f live_monitor/compose.yaml run --rm producer python3 -m live_monitor.producer --csv /input/100k_a.csv --run-id UUID --count 100 --rate 5", language="text")
        st.code("docker compose -f live_monitor/compose.yaml run --rm tools python3 -m live_monitor.control stop --run-id UUID", language="text")


monitor()
