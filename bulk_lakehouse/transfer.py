"""Staging local por hard links: no red, upload ni Jobs."""
import argparse
import hashlib
import json
from pathlib import Path
import os
from bulk_lakehouse.pipeline import validate_manifest
from bulk_lakehouse.source import save
from twitch_lakehouse.full_dataset_prepare import sha256


def prepare(parts, preflight, destination, plan_path):
    src, dst = Path(parts).resolve(), Path(destination).resolve()
    report = json.loads(Path(preflight).read_bytes())
    raw = (src/"manifest.json").read_bytes()
    m = validate_manifest(json.loads(raw),50000000)
    if (report.get("status") != "PASS" or report.get("rows_verified") != 50000000
        or report.get("parts_verified") != 500 or report.get("prefix_parts_matching") != 100
        or report.get("source_sha256") != m["source_sha256"]
        or report.get("manifest_sha256") != hashlib.sha256(raw).hexdigest()):
        raise ValueError("Preflight 50M válido y prefijo 10M obligatorios")
    if dst.exists() or Path(plan_path).exists() or dst==src or src in dst.parents:
        raise FileExistsError("Staging/plan deben ser nuevos y fuera de los originales")
    dst.mkdir(parents=True)
    files=[]
    for p in m["parts"][100:]:
        original=src/p["file"]
        if original.stat().st_size != p["bytes"] or sha256(original) != p["sha256"]:
            raise ValueError("Fragmento cambió después del preflight")
        target=dst/p["file"]
        target.hardlink_to(original)
        if not os.path.samefile(original,target):
            raise ValueError("El staging debe compartir datos físicos, sin copiar Parquet")
        files.append({"file":p["file"],"bytes":p["bytes"],"sha256":p["sha256"]})
    with (dst/"manifest.json").open("xb") as f:
        f.write(raw)
    files.append({"file":"manifest.json","bytes":len(raw),"sha256":hashlib.sha256(raw).hexdigest()})
    plan={"status":"PREPARED_NOT_AUTHORIZED_NOT_UPLOADED","files":files,
        "file_count":len(files),"total_transfer_bytes":sum(p["bytes"] for p in files),
        "staging":str(dst),"storage":"400 hard links; only manifest bytes copied. Never edit linked files.",
        "remote_destination":"/Volumes/workspace/twitch_bulk_experimental/source_parts/m50m/",
        "reused_remote_prefix":"/Volumes/workspace/twitch_bulk_experimental/source_parts/m10m/",
        "manifest_sha256":report["manifest_sha256"],"source_sha256":m["source_sha256"],
        "network_operations":0,"jobs_started":0}
    save(plan_path,plan)
    return {k:v for k,v in plan.items() if k!="files"}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--parts",required=True)
    p.add_argument("--preflight",required=True)
    p.add_argument("--destination",required=True)
    p.add_argument("--plan",required=True)
    a=p.parse_args()
    print(json.dumps(prepare(a.parts,a.preflight,a.destination,a.plan)),flush=True)


if __name__=="__main__":
    main()
