import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fmc"))
import fmc_lib as lib  # noqa: E402

WS, ITEM = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"

DL_SQL_DB = {
    "name": "Sales",
    "id": "Sales",
    "model": {
        "defaultMode": "directLake",
        "expressions": [{
            "name": "DatabaseQuery", "kind": "m",
            "expression": ["let", '    database = Sql.Database("abc.datawarehouse.fabric.microsoft.com", "ep-guid")',
                           "in", "    database"],
        }],
        "tables": [
            {"name": "Fact Sales",
             "columns": [{"name": "Amount"}, {"name": "Weird]Col", "type": "data"},
                         {"name": "RowNumber-x", "type": "rowNumber"}],
             "partitions": [{"name": "p", "mode": "directLake",
                             "source": {"type": "entity", "entityName": "fact_sales", "schemaName": "sales",
                                        "expressionSource": "DatabaseQuery"}}]},
            {"name": "O'Brien Dim",
             "columns": [{"name": "Key"}],
             "partitions": [{"name": "p",
                             "source": {"type": "entity", "entityName": "dim", "schemaName": "sales",
                                        "expressionSource": "DatabaseQuery"}}]},
            {"name": "Calc", "columns": [{"name": "x", "type": "calculated"}],
             "partitions": [{"name": "p", "mode": "import", "source": {"type": "calculated", "expression": "{1}"}}]},
        ],
    },
}


class TmslTests(unittest.TestCase):
    def test_load_database_unwraps_scripts(self):
        self.assertEqual(lib.load_database({"createOrReplace": {"object": {}, "database": DL_SQL_DB}})["name"], "Sales")
        self.assertEqual(lib.load_database(DL_SQL_DB)["name"], "Sales")

    def test_describe_partitions(self):
        rows = lib.describe_partitions(DL_SQL_DB)
        fact, dim, calc = rows
        self.assertEqual(fact["source_kind"], "sql_endpoint")
        self.assertEqual(fact["source_id2"], "ep-guid")
        self.assertEqual(dim["mode"], "directLake")   # default -> defaultMode
        self.assertEqual(calc["mode"], "import")
        self.assertEqual(lib.model_mode(rows), "directLake")  # calculated tables ignored

    def test_clone_rewrites_to_onelake(self):
        script = lib.clone_database(DL_SQL_DB, "vo_B_Sales", WS, ITEM)
        new = script["createOrReplace"]["database"]
        self.assertEqual(new["name"], "vo_B_Sales")
        self.assertNotIn("id", new)
        text = lib.expression_text(new["model"]["expressions"][0])
        self.assertEqual(lib.classify_expression(text), "onelake")
        self.assertEqual(lib.expression_ids(text), (WS, ITEM))
        self.assertEqual(DL_SQL_DB["name"], "Sales")  # original untouched

    def test_clone_rejects_non_direct_lake(self):
        with self.assertRaises(ValueError):
            lib.clone_database({"name": "x", "model": {"tables": []}}, "y", WS, ITEM)


class DaxTests(unittest.TestCase):
    def test_touch_queries_escape_and_filter(self):
        q = lib.column_touch_queries(DL_SQL_DB)
        self.assertEqual(set(q), {"Fact Sales", "O'Brien Dim"})
        self.assertIn("DISTINCTCOUNT('Fact Sales'[Weird]]Col])", q["Fact Sales"])
        self.assertNotIn("RowNumber", q["Fact Sales"])
        self.assertIn("'O''Brien Dim'[Key]", q["O'Brien Dim"])

    def test_tag_roundtrip(self):
        tagged = lib.tag_query("EVALUATE {1}", "ab12", 3, "C", "cold_touch", "Fact Sales")
        self.assertEqual(lib.parse_tag(tagged),
                         {"run_id": "ab12", "iteration": 3, "variant": "C", "phase": "cold_touch", "key": "Fact Sales"})
        self.assertIsNone(lib.parse_tag("EVALUATE {1}"))


class CostTests(unittest.TestCase):
    def test_reframes_bucket(self):
        t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
        times = [t0, t0 + timedelta(minutes=1), t0 + timedelta(minutes=12), t0 + timedelta(days=1)]
        self.assertEqual(lib.reframes_per_day(times, days=2, bucket_minutes=5), 1.5)
        self.assertEqual(lib.reframes_per_day([], days=0), 0.0)

    def test_daily_delta(self):
        base = {"transcode_cpu_s": {"f": 10.0, "d": 1.0}, "warm_cpu_s_per_query": 0.2}
        cand = {"transcode_cpu_s": {"f": 6.0, "d": 1.0}, "warm_cpu_s_per_query": 0.15}
        d = lib.daily_delta(base, cand, {"f": 24, "d": 1}, 1000,
                            maintenance_cu_s_per_sync=10, syncs_per_day=24, extra_storage_gb=30,
                            cu_per_cpu_s=2.0, price_per_cu_hour=0.18, onelake_price_gb_month=0.03)
        self.assertAlmostEqual(d["transcode_cpu_s_saved"], 96)
        self.assertAlmostEqual(d["warm_query_cpu_s_saved"], 50)
        self.assertAlmostEqual(d["net_cu_s"], 146 * 2 - 240)
        self.assertAlmostEqual(d["net_usd"], 52 / 3600 * 0.18 - 0.03)

    def test_daily_delta_uncalibrated(self):
        p = {"transcode_cpu_s": {}, "warm_cpu_s_per_query": 0.1}
        d = lib.daily_delta(p, p, {}, 10)
        self.assertIsNone(d["net_usd"])
        self.assertEqual(d["query_cpu_s_saved"], 0)


if __name__ == "__main__":
    unittest.main()
