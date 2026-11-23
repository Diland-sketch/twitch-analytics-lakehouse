"""Fallos de autorización y DAG: no requiere Databricks ni credenciales."""
import unittest
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import patch
from kafka_ingest.orchestration import Context, require_pass, authorize, publish, STEPS
from kafka_ingest.job_definition import build_job


def context():
    return Context("kafka_b7_r002", 7, "workspace.twitch_kafka_experimental.native_pilot_binding_v1",
        1, "whole_snapshot", "a" * 40, "123", "456")


class JobGuards(unittest.TestCase):
    def test_exact_versions_release_and_real_job_context_required(self):
        cfg = context()
        for changes in ({"release_id": "kafka_b7_r001"}, {"bronze_version": 8},
                {"manifest_table": "workspace.twitch_lakehouse.binding"}, {"manifest_version": -1},
                {"job_run_id": "{{job.run_id}}"}, {"code_commit": "main"}):
            with self.assertRaises(ValueError):
                replace(cfg, **changes)
        self.assertEqual(replace(cfg, release_id="kafka_b8_r001", bronze_version=8).bronze_version, 8)

    def test_no_transform_authority_on_fail_or_repaired_parameters_changed(self):
        cfg = context()
        evidence = {"status": "PASS", "context": asdict(cfg), "context_sha256": cfg.fingerprint}
        require_pass(evidence, cfg)
        for bad in ({**evidence, "status": "FAIL"}, {**evidence, "context_sha256": "bad"}):
            with self.assertRaises(ValueError):
                require_pass(bad, cfg)
        with self.assertRaises(ValueError):
            require_pass(evidence, replace(cfg, manifest_version=2))
        with self.assertRaises(ValueError):
            require_pass(evidence, replace(cfg, job_run_id="457"))

    def test_dag_all_success_zero_retry_and_immutable_git(self):
        job = build_job("a" * 40)
        self.assertEqual(job["max_concurrent_runs"], 1)
        self.assertEqual(job["git_source"]["git_commit"], "a" * 40)
        for i, task in enumerate(job["tasks"]):
            self.assertEqual(task["task_key"], STEPS[i])
            self.assertEqual(task["max_retries"], 0)
            self.assertEqual(task["run_if"], "ALL_SUCCESS")
            self.assertEqual(task["depends_on"], [{"task_key": STEPS[i-1]}] if i else [])
            self.assertNotIn("new_cluster", task)
            self.assertEqual(task["notebook_task"]["source"], "GIT")
        self.assertIn("22_kafka_native", job["tasks"][0]["notebook_task"]["notebook_path"])
        self.assertIn("23_kafka_silver_gold", job["tasks"][2]["notebook_task"]["notebook_path"])
        self.assertNotIn("schedule", job)

    def test_failed_bronze_authorization_prevents_claim_and_publication(self):
        cfg = context()
        with patch("kafka_ingest.orchestration.verified", side_effect=ValueError("fixture FAIL")), patch("kafka_ingest.orchestration.put_record") as write:
            with self.assertRaises(ValueError):
                authorize(None, cfg)
            write.assert_not_called()
        with patch("kafka_ingest.orchestration.authorized", side_effect=ValueError("No auth")), patch("kafka_ingest.orchestration.put_record") as write:
            with self.assertRaises(ValueError):
                publish(None, cfg)
            write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
