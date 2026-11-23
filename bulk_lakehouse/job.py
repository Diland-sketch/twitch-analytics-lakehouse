"""Job batch independiente, Git inmutable, sin conectarse a Kafka ni clúster nuevo."""
import re


def settings(commit):
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("SHA Git completo requerido")
    steps = ("verify", "bronze", "silver", "gold", "publish")
    defaults = {"rows": "", "release": "", "manifest_path": "", "predecessor": "", "max_input_bytes": "134217728"}
    return {"name": "twitch-bulk-batch-milestones", "max_concurrent_runs": 1, "timeout_seconds": 7200,
        "queue": {"enabled": True}, "git_source": {
            "git_url": "https://github.com/Diland-sketch/twitch-analytics-lakehouse.git", "git_provider": "gitHub", "git_commit": commit},
        "parameters": [{"name": k, "default": v} for k, v in defaults.items()],
        "tasks": [{"task_key": stage, "depends_on": [{"task_key": steps[i-1]}] if i else [],
            "run_if": "ALL_SUCCESS", "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True, "timeout_seconds": 3600,
            "notebook_task": {"notebook_path": "notebooks/30_bulk_batch_job.py", "source": "GIT",
                "base_parameters": {"stage": stage}}} for i, stage in enumerate(steps)]}
