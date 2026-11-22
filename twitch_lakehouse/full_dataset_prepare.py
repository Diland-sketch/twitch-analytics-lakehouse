"""Fragmenta GZIP histórico en CSV verificables sin materializarlo en RAM."""
import argparse
import gzip
import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path


def sha256(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def prepare(source, output, rows=1_000_000, part_rows=100_000):
    if rows < 1 or part_rows < 1:
        raise ValueError("rows y part_rows deben ser positivos")
    source = Path(source).resolve(strict=True)
    output = Path(output).resolve()
    if not source.is_file() or source.suffix.lower() != ".gz":
        raise ValueError("Se requiere una fuente GZIP")
    if output.exists():
        raise FileExistsError(f"Destino existente: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.parent / (".pending-" + uuid.uuid4().hex)
    tmp.mkdir()
    parts = []
    count = 0
    try:
        source_hash = sha256(source)
        with gzip.open(source, "rb") as stream:
            while count < rows:
                name = f"part-{len(parts):06d}.csv"
                checksum = hashlib.sha256()
                size = 0
                n = 0
                first = count + 1
                with (tmp / name).open("xb") as dest:
                    while n < part_rows and count < rows:
                        line = stream.readline()
                        if not line:
                            break
                        dest.write(line)
                        checksum.update(line)
                        size += len(line)
                        n += 1
                        count += 1
                if n == 0:
                    (tmp / name).unlink()
                    break
                parts.append(dict(file=name, first_row=first, last_row=count,
                                  rows=n, bytes=size, sha256=checksum.hexdigest()))
        if count != rows:
            raise ValueError(f"Se pidieron {rows} líneas pero la fuente tiene {count}")
        for item in parts:
            path = tmp / item["file"]
            if sha256(path) != item["sha256"] or path.stat().st_size != item["bytes"]:
                raise IOError("Falló la verificación: " + item["file"])
        if sum(item["rows"] for item in parts) != count:
            raise AssertionError("Los fragmentos no concilian")
        result = dict(schema_version="twitch-fragments-v1",
                      source_name=source.name, source_bytes=source.stat().st_size,
                      source_sha256=source_hash, created_at_utc=datetime.now(timezone.utc).isoformat(),
                      requested_rows=rows, extracted_rows=count, rows_per_part=part_rows,
                      parts=parts, semantics="Filas físicas sin validación Silver")
        (tmp / "manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n",
                                           encoding="utf-8")
        if output.exists():
            raise FileExistsError(f"Destino creado durante la extracción: {output}")
        os.replace(tmp, output)
        return result
    except BaseException:
        if tmp.exists():
            shutil.rmtree(tmp)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--rows-per-part", type=int, default=100_000)
    args = parser.parse_args()
    result = prepare(args.source, args.output, args.rows, args.rows_per_part)
    print(json.dumps(dict(status="PASS", extracted_rows=result["extracted_rows"],
                          parts=len(result["parts"]), source_sha256=result["source_sha256"],
                          manifest=str((args.output / "manifest.json").resolve()))))


if __name__ == "__main__":
    main()
