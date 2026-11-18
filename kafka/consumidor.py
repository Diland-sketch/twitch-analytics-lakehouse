import json
import sys

from kafka import KafkaConsumer

if len(sys.argv) != 2:
    raise SystemExit("Uso: python consumidor.py RUN_ID")

EJECUCION = sys.argv[1]

consumer = KafkaConsumer(
    "twitch.interactions.raw",
    bootstrap_servers="127.0.0.1:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,
    consumer_timeout_ms=15000
)

filas_recibidas = set()
mensajes = 0

try:
    for mensaje in consumer:
        evento = json.loads(mensaje.value.decode("utf-8"))

        if evento.get("run_id") != EJECUCION:
            continue

        mensajes += 1
        filas_recibidas.add(evento["source_row"])

        print(
            f"Recibido | particion={mensaje.partition} | "
            f"offset={mensaje.offset}"
        )
        print(json.dumps(evento, ensure_ascii=False))

        if filas_recibidas == set(range(1, 11)):
            break
finally:
    consumer.close()

print("Mensajes de la ejecucion recibidos:", mensajes)
print("Filas distintas recibidas:", sorted(filas_recibidas))

if filas_recibidas != set(range(1, 11)):
    raise RuntimeError("No se recibieron las diez filas esperadas.")

print("VALIDACION CORRECTA: recibidas las diez filas de la ejecucion.")