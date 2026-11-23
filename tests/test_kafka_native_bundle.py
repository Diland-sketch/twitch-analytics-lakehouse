"""Contratos de aislamiento; no equivalen a bundle validate ni despliegue remoto."""
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class NativeBundleTests(unittest.TestCase):
    def test_native_binary_bronze_metadata_and_isolation(self):
        bundle = json.loads((ROOT / "bundles/kafka_native/databricks.yml").read_text())
        config = json.loads((ROOT / "bundles/kafka_native/resources/bronze.pipeline.yml").read_text())
        self.assertNotIn("resources", bundle)  # No servicios ni conexiones creadas implícitamente
        pipeline = config["resources"]["pipelines"]["kafka_bronze_native"]
        self.assertTrue(pipeline["serverless"])
        self.assertEqual(pipeline["channel"], "PREVIEW")
        self.assertTrue(pipeline["lifecycle"]["prevent_destroy"])
        self.assertNotIn("libraries", pipeline)
        table = pipeline["ingestion_definition"]["objects"][0]["table"]
        self.assertEqual(table["destination_schema"], "twitch_kafka_experimental")
        self.assertEqual(table["destination_catalog"], "workspace")
        self.assertEqual(table["table_configuration"]["source_metadata_column"], "kafka_source")
        kafka = table["connector_options"]["kafka_options"]
        self.assertEqual(kafka["starting_offset"], "earliest")
        self.assertNotIn("value_transformer", kafka)  # Preserva payload original binario
        self.assertNotIn("failOnDataLoss", kafka)  # No inventar opción Spark en managed connector
        self.assertNotIn("storage", pipeline)  # Estado gestionado, sin reutilizar checkpoint I4


if __name__ == "__main__":
    unittest.main()
