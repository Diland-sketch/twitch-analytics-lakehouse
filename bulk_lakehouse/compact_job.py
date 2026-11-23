"""Replace tasks of the existing batch Job; no schedules or paid clusters."""
from bulk_lakehouse.job import settings as original
from bulk_lakehouse.compact import STAGES


def settings(commit):
    job=original(commit)
    job["parameters"]=[{"name":"release","default":"bulk_50m_c001"}]
    for i,t in enumerate(job["tasks"]):
        t["notebook_task"]={"notebook_path":"notebooks/33_bulk_consolidated_job.py","source":"GIT","base_parameters":{"stage":STAGES[i]}}
    return job
