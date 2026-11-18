"""Regresión de orquestación con dobles; no ejecuta Spark, SQL ni Delta reales."""
import unittest
from types import SimpleNamespace
from twitch_lakehouse.storage import require_absent, write_new


class FakeSpark:
    def __init__(self):
        self.tables = {}
        self.events = []
        self.catalog = SimpleNamespace(tableExists=lambda t: t in self.tables)
        self.fail_create = False

    def sql(self, ddl):
        self.events.append(("ddl", ddl))
        table = ddl.split(" (")[0].removeprefix("CREATE TABLE ").replace("`", "")
        if self.fail_create or table in self.tables:
            raise RuntimeError("CREATE falló: destino existente o sin permisos")
        self.tables[table] = []
        return SimpleNamespace(collect=lambda: self.events.append(("ddl_completed", table)))


class FakeFrame:
    def __init__(self, spark, rows, fail_append=False):
        self.spark, self.rows, self.fail_append = spark, rows, fail_append
        def field(name, kind, nullable=True):
            return SimpleNamespace(name=name, nullable=nullable,
                                   dataType=SimpleNamespace(simpleString=lambda: kind))
        self.schema = SimpleNamespace(fields=[field("raw_line", "string"),
                                              field("source_file", "string"),
                                              field("ingested_at", "timestamp", False)])

    @property
    def write(self):
        frame = self
        class Writer:
            def format(self, value):
                frame.spark.events.append(("format", value))
                return self
            def mode(self, value):
                frame.spark.events.append(("mode", value))
                if value != "append":
                    raise AssertionError("Solo append es compatible con esta regresión")
                return self
            def saveAsTable(self, table):
                frame.spark.events.append(("append", table))
                if frame.fail_append:
                    raise RuntimeError("Fallo artificial de append")
                frame.spark.tables[table].extend(frame.rows)
        return Writer()


class WriteGuardTests(unittest.TestCase):
    table = "workspace.demo.bronze_interactions"

    def test_existing_destination_blocks_without_mutation(self):
        spark = FakeSpark()
        spark.tables[self.table] = ["original"]
        with self.assertRaises(RuntimeError):
            write_new(spark, FakeFrame(spark, ["new"]), self.table)
        self.assertEqual(spark.tables[self.table], ["original"])
        self.assertEqual(spark.events, [])

    def test_new_destinations_pass(self):
        require_absent(FakeSpark(), self.table, "workspace.demo.rejects")

    def test_second_existing_destination_also_blocks(self):
        spark = FakeSpark()
        spark.tables["workspace.demo.rejects"] = []
        with self.assertRaises(RuntimeError):
            require_absent(spark, self.table, "workspace.demo.rejects")

    def test_ddl_schema_and_append_order(self):
        spark = FakeSpark()
        write_new(spark, FakeFrame(spark, ["one", "two"]), self.table)
        self.assertEqual(spark.events, [
            ("ddl", "CREATE TABLE `workspace`.`demo`.`bronze_interactions` "
             "(`raw_line` string, `source_file` string, `ingested_at` timestamp NOT NULL) USING DELTA"),
            ("ddl_completed", self.table), ("format", "delta"),
            ("mode", "append"), ("append", self.table)])
        self.assertEqual(spark.tables[self.table], ["one", "two"])

    def test_second_execution_never_duplicates_for_every_destination(self):
        names = ["bronze_interactions", "silver_interactions", "silver_rejects",
                 "gold_streamer_metrics", "silver_interactions_candidate_revision_01",
                 "silver_rejects_candidate_revision_01"]
        for name in names:
            with self.subTest(destination=name):
                spark = FakeSpark()
                table = f"workspace.demo.{name}"
                frame = FakeFrame(spark, ["one"])
                write_new(spark, frame, table)
                events = list(spark.events)
                with self.assertRaises(RuntimeError):
                    write_new(spark, frame, table)
                self.assertEqual(spark.tables[table], ["one"])
                self.assertEqual(spark.events, events)

    def test_create_failure_never_appends(self):
        spark = FakeSpark()
        spark.fail_create = True
        with self.assertRaises(RuntimeError):
            write_new(spark, FakeFrame(spark, ["one"]), self.table)
        self.assertEqual([event[0] for event in spark.events], ["ddl"])
        self.assertEqual(spark.tables, {})

    def test_create_completion_failure_never_appends(self):
        spark = FakeSpark()
        original_sql = spark.sql
        def sql_with_failed_completion(ddl):
            original_sql(ddl)
            def fail():
                raise RuntimeError("CREATE no confirmado por Spark Connect")
            return SimpleNamespace(collect=fail)
        spark.sql = sql_with_failed_completion
        with self.assertRaisesRegex(RuntimeError, "no confirmado"):
            write_new(spark, FakeFrame(spark, ["one"]), self.table)
        self.assertEqual(spark.tables[self.table], [])
        self.assertEqual([event[0] for event in spark.events], ["ddl"])

    def test_racing_creator_cannot_append_to_existing_table(self):
        spark = FakeSpark()
        spark.tables[self.table] = ["other_creator"]
        # Catálogo desactualizado: CREATE debe ser la segunda barrera.
        spark.catalog.tableExists = lambda _: False
        with self.assertRaises(RuntimeError):
            write_new(spark, FakeFrame(spark, ["one"]), self.table)
        self.assertEqual(spark.tables[self.table], ["other_creator"])
        self.assertEqual([event[0] for event in spark.events], ["ddl"])

    def test_failed_append_leaves_table_and_blocks_retry(self):
        spark = FakeSpark()
        with self.assertRaisesRegex(RuntimeError, "Fallo artificial"):
            write_new(spark, FakeFrame(spark, ["one"], fail_append=True), self.table)
        self.assertEqual(spark.tables[self.table], [])
        events = list(spark.events)
        with self.assertRaises(RuntimeError):
            write_new(spark, FakeFrame(spark, ["one"]), self.table)
        self.assertEqual(spark.tables[self.table], [])
        self.assertEqual(spark.events, events)

    def test_empty_frame_table_also_blocks_repeat(self):
        spark = FakeSpark()
        write_new(spark, FakeFrame(spark, []), self.table)
        with self.assertRaises(RuntimeError):
            write_new(spark, FakeFrame(spark, ["one"]), self.table)
        self.assertEqual(spark.tables[self.table], [])

    def test_invalid_destination_fails_before_any_operation(self):
        for table in ("demo.table", "workspace.demo.table; DELETE", "workspace.demo.t.extra"):
            with self.subTest(table=table), self.assertRaises(ValueError):
                spark = FakeSpark()
                write_new(spark, FakeFrame(spark, []), table)
            self.assertEqual(spark.events, [])

    def test_empty_schema_fails_before_ddl(self):
        spark = FakeSpark()
        frame = FakeFrame(spark, [])
        frame.schema.fields = []
        with self.assertRaises(ValueError):
            write_new(spark, frame, self.table)
        self.assertEqual(spark.events, [])
