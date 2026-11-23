"""Validación local de solo lectura; no publica ni inicia Jobs."""
import argparse
import hashlib
import json
from pathlib import Path
import time

from bulk_lakehouse.pipeline import validate_manifest
from bulk_lakehouse.source import save
from twitch_lakehouse.full_dataset_prepare import sha256


def validate(root, expected_rows, prefix_root=None, source=None, progress=None):
    import pyarrow.parquet as pq
    root = Path(root)
    started = time.monotonic()
    manifest_path = root / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    m = validate_manifest(json.loads(manifest_bytes), expected_rows)
    prefix = None
    if prefix_root:
        prefix = json.loads((Path(prefix_root) / "manifest.json").read_bytes())
        validate_manifest(prefix, prefix["extracted_rows"])
        if (prefix["source_sha256"] != m["source_sha256"]
            or prefix["parts"] != m["parts"][:len(prefix["parts"])]) :
            raise ValueError("El prefijo no coincide exactamente con el manifiesto previo")
    if source and sha256(Path(source)) != m["source_sha256"]:
        raise ValueError("SHA del GZIP original distinto")
    ordinal, size = 0, 0
    for index, part in enumerate(m["parts"]):
        path = root / part["file"]
        before = path.stat()
        if before.st_size != part["bytes"] or sha256(path) != part["sha256"]:
            raise ValueError(f"Fragmento alterado: {part['file']}")
        content = hashlib.sha256()
        count = 0
        parquet = pq.ParquetFile(path)
        if parquet.schema_arrow.names != ["source_row", "raw_line", "raw_sha256"]:
            raise ValueError("Esquema Parquet inesperado")
        for batch in parquet.iter_batches(batch_size=10000):
            for number, line, line_hash in zip(*(batch.column(i).to_pylist() for i in range(3))):
                ordinal += 1
                count += 1
                if number != ordinal or hashlib.sha256(line.encode("utf-8")).hexdigest() != line_hash:
                    raise ValueError(f"Ordinal/hash inválido en {part['file']}")
                content.update(f"{number}|{line_hash}\n".encode())
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("Fragmento cambió durante validación")
        if count != part["rows"] or content.hexdigest() != part["row_digest"]:
            raise ValueError("Conteo/digest secuencial distinto")
        size += before.st_size
        if progress and ((index + 1) % 25 == 0 or index + 1 == len(m["parts"])):
            progress({"parts_verified":index+1,"rows_verified":ordinal})
    if ordinal != expected_rows or manifest_path.read_bytes() != manifest_bytes:
        raise ValueError("Manifiesto o cobertura cambió")
    reused = len(prefix["parts"]) if prefix else 0
    missing = m["parts"][reused:]
    return {"status":"PASS","scope":"LOCAL_PARQUET_VALIDATION_NOT_REMOTE_PROCESSING",
        "source_sha256":m["source_sha256"],"source_gzip_hash_checked":bool(source),
        "manifest_sha256":hashlib.sha256(manifest_bytes).hexdigest(),
        "rows_verified":ordinal,"parts_verified":len(m["parts"]),"parquet_bytes":size,
        "manifest_bytes":len(manifest_bytes),"prefix_parts_matching":reused,
        "files_to_transfer":len(missing)+1,"parquet_bytes_to_transfer":sum(p["bytes"] for p in missing),
        "total_bytes_to_transfer":sum(p["bytes"] for p in missing)+len(manifest_bytes),
        "max_part_bytes":max(p["bytes"] for p in m["parts"]),
        "elapsed_seconds":time.monotonic()-started}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--parts",required=True)
    p.add_argument("--rows",type=int,required=True)
    p.add_argument("--prefix")
    p.add_argument("--source")
    p.add_argument("--output",required=True)
    a = p.parse_args()
    if Path(a.output).exists():
        raise FileExistsError("Conservar evidencia existente; usar otro nombre")
    report = validate(a.parts,a.rows,a.prefix,a.source,lambda v:print(json.dumps(v),flush=True))
    save(a.output,report)
    print(json.dumps(report),flush=True)


if __name__ == "__main__":
    main()
