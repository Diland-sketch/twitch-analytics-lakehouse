import csv
import json
import uuid
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

from kafka import KafkaProducer

RUTA = Path.home() / "Downloads" / "100k_a.csv"
TOPICO = "twitch.interactions.raw"
EJECUCION = str(uuid.uuid4())

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    acks="all",
    value_serializer=lambda evento: json.dumps(evento).encode("utf-8")
)

confirmados = 0

try:
    with RUTA.open(encoding="utf-8", newline="") as archivo:
        for numero, fila in enumerate(islice(csv.reader(archivo), 10), start=1):
            usuario, transmision, streamer, inicio, fin = fila

            evento = {
                "source_file": RUTA.name,
                "source_row": numero,
                "run_id": EJECUCION,
                "historical_replay": True,
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "user_id": int(usuario),
                "stream_id": int(transmision),
                "streamer_username": streamer,
                "time_start": int(inicio),
                "time_stop": int(fin)
            }

            resultado = producer.send(TOPICO, value=evento).get(timeout=20)
            confirmados += 1

            print(
                f"Fila {numero} confirmada | "
                f"particion={resultado.partition} | offset={resultado.offset}"
            )
finally:
    producer.close()

print("Ejecucion:", EJECUCION)
print("Total confirmado por Kafka:", confirmados)