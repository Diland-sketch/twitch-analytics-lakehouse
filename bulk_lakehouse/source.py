"""Inspección GZIP y fragmentos Parquet acotados con ordinal global explícito."""
import argparse
import gzip
import hashlib
import json
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from twitch_lakehouse.full_dataset_prepare import sha256


def save(path, value):
    with Path(path).open("x", encoding="utf-8") as target:
        json.dump(value, target, indent=2, ensure_ascii=False)
        target.flush()
        os.fsync(target.fileno())


def inspect(source, output):
    source = Path(source).resolve(strict=True)
    if Path(output).exists():
        raise FileExistsError(output)
    start = time.monotonic()
    before = source.stat()
    compressed_sha = sha256(source)
    rows, expanded_bytes, last = 0, 0, b""
    expanded_sha = hashlib.sha256()
    with gzip.open(source, "rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            rows += chunk.count(b"\n")
            expanded_bytes += len(chunk)
            expanded_sha.update(chunk)
            last = chunk[-1:]
    if expanded_bytes and last != b"\n":
        rows += 1
    after = source.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError("Fuente cambió durante la inspección")
    report = {"status": "PASS", "source_name": source.name, "source_bytes": before.st_size,
        "source_sha256": compressed_sha, "expanded_bytes": expanded_bytes,
        "expanded_sha256": expanded_sha.hexdigest(), "physical_rows": rows,
        "source_mtime_ns": before.st_mtime_ns, "elapsed_seconds": time.monotonic() - start,
        "observed_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "FULL_LOCAL_GZIP_SCAN_NOT_DATABRICKS_PROCESSING"}
    save(output, report)
    return report


def prepare(source, output, rows, profile, part_rows=100_000, buffer_rows=10_000):
    import pyarrow as pa
    import pyarrow.parquet as pq
    source = Path(source).resolve(strict=True)
    root = Path(output)
    if root.exists():
        raise FileExistsError(output)
    if rows not in (100_000, 1_000_000, 10_000_000, 50_000_000) or not 1 <= buffer_rows <= part_rows <= 1_000_000:
        raise ValueError("Hito/tamaño de bloque no permitido")
    audit = json.loads(Path(profile).read_text(encoding="utf-8"))
    stat = source.stat()
    if (audit.get("status") != "PASS" or audit["source_name"] != source.name
        or audit["source_bytes"] != stat.st_size or audit["source_mtime_ns"] != stat.st_mtime_ns
        or audit["physical_rows"] < rows):
        raise ValueError("Perfil no corresponde a la fuente actual o no cubre el hito")
    # Revalidar bytes fuente, no confiar solamente en nombre/tamaño/mtime.
    if sha256(source) != audit["source_sha256"]:
        raise ValueError("SHA fuente no coincide con inspección")
    root.mkdir(parents=True)
    start = time.monotonic()
    schema = pa.schema([("source_row", pa.int64()), ("raw_line", pa.string()), ("raw_sha256", pa.string())])
    parts, ordinal = [], 0
    try:
        with gzip.open(source, "rb") as stream:
            while ordinal < rows:
                first, local_rows = ordinal + 1, 0
                path = root / f"part-{len(parts):06d}.parquet"
                content = hashlib.sha256()
                with pq.ParquetWriter(path, schema, compression="snappy") as writer:
                    while local_rows < part_rows and ordinal < rows:
                        buffer = []
                        take = min(buffer_rows, part_rows - local_rows, rows - ordinal)
                        while len(buffer) < take:
                            raw = stream.readline()
                            if not raw:
                                raise ValueError("EOF antes del rango autorizado")
                            line = raw.rstrip(b"\r\n").decode("utf-8", errors="strict")
                            ordinal += 1
                            local_rows += 1
                            line_sha = hashlib.sha256(line.encode()).hexdigest()
                            # Digest secuencial verificable del bloque, no métrica de negocio.
                            content.update(f"{ordinal}|{line_sha}\n".encode())
                            buffer.append({"source_row": ordinal, "raw_line": line, "raw_sha256": line_sha})
                        writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                parts.append({"file": path.name, "rows": local_rows, "first_row": first, "last_row": ordinal,
                    "bytes": path.stat().st_size, "sha256": sha256(path), "row_digest": content.hexdigest()})
        after = source.stat()
        if (stat.st_size, stat.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("La fuente cambió durante fragmentación")
        result = {"schema_version": "twitch-bulk-parquet-v1", "source_name": source.name,
            "source_sha256": audit["source_sha256"], "source_bytes": stat.st_size,
            "source_physical_rows": audit["physical_rows"], "requested_rows": rows,
            "extracted_rows": ordinal, "parts": parts, "elapsed_seconds": time.monotonic() - start,
            "semantics": "Observed historical chat connections, not active viewers"}
        save(root / "manifest.json", result)
        return result
    except BaseException as error:
        # No borrar fragmentos parciales: evidencia conservada, sin manifest de éxito.
        save(root / "FAILED.json", {"status": "FAILED", "error_type": type(error).__name__, "last_row": ordinal})
        raise


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["inspect", "prepare"])
    p.add_argument("--source", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--profile")
    p.add_argument("--rows", type=int, default=100_000)
    args = p.parse_args()
    value = inspect(args.source, args.output) if args.command == "inspect" else prepare(args.source, args.output, args.rows, args.profile)
    print(json.dumps(value, ensure_ascii=False))


if __name__ == "__main__":
    main()
