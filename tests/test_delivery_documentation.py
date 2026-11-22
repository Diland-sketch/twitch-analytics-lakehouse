"""Revisión estática de la documentación de entrega.

No consulta Databricks, Kafka ni datos privados. No sustituye sus pruebas E2E.
"""
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "docs" / "entrega_final"


class DeliveryDocumentationTests(unittest.TestCase):
    def test_all_documents_exist(self):
        self.assertTrue((BASE / "README.md").is_file())
        for number in range(1, 9):
            matches = list(BASE.glob(f"{number:02d}_*.md"))
            self.assertEqual(len(matches), 1, f"Documento {number} ausente o duplicado")

    def test_sql_audits_are_read_only_and_separate(self):
        queries = sorted((BASE / "sql").glob("*.sql"))
        self.assertEqual(len(queries), 7)
        forbidden = re.compile(r"\b(CREATE|ALTER|DROP|DELETE|INSERT|UPDATE|MERGE|TRUNCATE|VACUUM|OPTIMIZE)\b", re.I)
        for path in queries:
            with self.subTest(sql=path.name):
                source = path.read_text(encoding="utf-8")
                statements = "\n".join(line.split("--", 1)[0] for line in source.splitlines()).strip()
                self.assertTrue(statements.startswith(("WITH ", "SELECT ")))
                self.assertIsNone(forbidden.search(statements), "Auditoría no debe modificar datos")
                self.assertLessEqual(statements.count(";"), 1)
                self.assertIn("workspace.twitch_lakehouse", statements)

    def test_demo_script_uses_new_run_and_no_destructive_actions(self):
        script = (BASE / "scripts" / "validar_demo.ps1").read_text(encoding="utf-8")
        for token in ("[guid]::NewGuid()", "Get-FileHash", "run-id",
                      "api/v1/snapshot", "valid", "invalid", "RestartSpark",
                      "Reenviar", "work\\entrega_final", "evolucion_api.jsonl"):
            self.assertIn(token, script)
        # Strip lines explicitly containing explanatory comments.
        executable = "\n".join(line.split("#", 1)[0] for line in script.splitlines())
        self.assertNotRegex(executable, r"(?i)\bdown\s+-v\b")
        self.assertNotRegex(executable, r"(?i)\bvolume\s+(rm|prune)\b")

    def test_readme_routes_to_delivery_and_states_limits(self):
        root = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("docs/entrega_final/README.md", root)
        self.assertIn("desarollo", root)
        index = (BASE / "README.md").read_text(encoding="utf-8")
        self.assertIn("100k_a.csv", index)
        self.assertIn("integración automática continua", index)
        self.assertIn("Kafka", index)
        self.assertIn("Databricks", index)


if __name__ == "__main__":
    unittest.main()
