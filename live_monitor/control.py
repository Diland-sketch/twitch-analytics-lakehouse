import argparse
import json
import uuid
from live_monitor.common import state_dir
from live_monitor.storage import snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["status", "stop", "resume"])
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.action == "status":
        if not (state_dir() / "monitor.sqlite").exists():
            print(json.dumps({"waiting": True, "total": 0, "valid": 0, "invalid": 0, "consumer": None}))
        else:
            print(json.dumps(snapshot(run_id=args.run_id), ensure_ascii=False))
    else:
        if not args.run_id:
            parser.error("--run-id requerido")
        path = state_dir() / f"stop-{str(uuid.UUID(args.run_id))}"
        if args.action == "stop":
            path.touch()
            print("Parada solicitada: finaliza el envío en curso y conserva confirmaciones")
        elif path.exists():
            path.unlink()  # Solo señal de control, nunca datos/checkpoint.


if __name__ == "__main__":
    main()
