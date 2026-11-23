"""Jobs API 2.2: DAG serial, serverless, Git commit fijo, sin secretos ni schedules."""
import argparse
import json
import re
from pathlib import Path
from kafka_ingest.orchestration import STEPS, PARAMETERS


def build_job(commit):
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("Usar SHA completo del commit probado")
    parameters = {"release_id": "", "bronze_version": "", "manifest_table": "", "manifest_version": "",
                  "population": "whole_snapshot", "code_commit": commit}
    tasks = []
    for i, stage in enumerate(STEPS):
        notebook = {"verify_bronze": "22_kafka_native_verify_pilot", "transform": "23_kafka_silver_gold_pilot"}.get(stage, "24_kafka_job_control")
        tasks.append({"task_key": stage, "max_retries": 0, "timeout_seconds": 1200,
            "run_if": "ALL_SUCCESS",
            "depends_on": [{"task_key": STEPS[i-1]}] if i else [],
            "notebook_task": {"notebook_path": f"notebooks/{notebook}.py", "source": "GIT",
                "base_parameters": {"execution_mode": "job", "job_stage": stage,
                    "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}"}}})
    return {"name": "twitch-kafka-medallion-pilot-dag", "max_concurrent_runs": 1,
        "timeout_seconds": 3600, "queue": {"enabled": True},
        "git_source": {"git_url": "https://github.com/Diland-sketch/twitch-analytics-lakehouse.git",
            "git_provider": "gitHub", "git_commit": commit},
        "parameters": [{"name": k, "default": v} for k, v in parameters.items()], "tasks": tasks}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with Path(args.output).open("x", encoding="utf-8") as target:
        json.dump(build_job(args.commit), target, indent=2)


if __name__ == "__main__":
    main()
