"""
統合テスト: DuckDB Runner + Transformer の連携検証

PySpark コードを BigQuery SQL に変換し、
同じサンプルデータを使って DuckDB で実行・結果を比較する。

テスト戦略:
  - Python でサンプルデータを直接処理した「正解」と DuckDB 結果を比較
  - Spark が不要なのでローカル環境でそのまま実行可能
"""
import unittest
import sys
import os
import csv
import json
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from converter import PySparkToBigQueryTransformer
from validator.duckdb_runner import DuckDBRunner, TableSchema, QueryResult
from validator.comparator import ResultComparator, CompareConfig


# ─────────────────────────────────────────────────────────────────────
# ヘルパー
# ─────────────────────────────────────────────────────────────────────

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "data"


def load_csv_schema(table_name: str, filename: str) -> TableSchema:
    """CSV ファイルをロードして TableSchema を返す"""
    path = FIXTURE_DIR / filename
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))

    schemas_path = FIXTURE_DIR / "schemas.json"
    with open(schemas_path) as f:
        all_schemas = json.load(f)

    schema_def = all_schemas.get(table_name, {})
    columns = [
        (col["name"], col["type"])
        for col in schema_def.get("columns", [])
    ] or [(k, "STRING") for k in (rows[0].keys() if rows else [])]

    return TableSchema(name=table_name, columns=columns, data=rows)


def make_runner() -> DuckDBRunner:
    """テスト用 DuckDB ランナーを初期化してサンプルテーブルを登録する"""
    runner = DuckDBRunner()
    runner.register_table(load_csv_schema("raw.orders", "orders.csv"))
    runner.register_table(load_csv_schema("raw.customers", "customers.csv"))
    runner.register_table(load_csv_schema("raw.products", "products.csv"))
    return runner


def convert_and_run(pyspark_code: str, var_name: str = "result") -> QueryResult:
    """PySpark コードを変換して DuckDB で実行する"""
    transformer = PySparkToBigQueryTransformer()
    result = transformer.convert_single(pyspark_code, var_name=var_name)
    assert "ERROR" not in result.sql, f"変換エラー:\n{result.sql}"
    runner = make_runner()
    return runner.run(result.sql)


# ─────────────────────────────────────────────────────────────────────
# テストケース
# ─────────────────────────────────────────────────────────────────────

class TestBasicQueryConversion(unittest.TestCase):
    """基本クエリの変換と実行テスト"""

    def test_simple_select_all(self):
        """テーブルの全件取得"""
        code = 'df = spark.table("raw.orders")'
        result = convert_and_run(code, "df")
        self.assertEqual(result.row_count, 10)
        self.assertIn("order_id", result.columns)

    def test_filter_active_orders(self):
        """WHERE 句フィルタ"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.filter(F.col("status") == "active")
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 8)   # cancelled を除外した 8 件
        statuses = {row[result.columns.index("status")] for row in result.rows}
        self.assertEqual(statuses, {"active"})

    def test_filter_amount_gt(self):
        """数値比較フィルタ"""
        # SQLite はスキーマなしのため数値比較が文字列比較になる → DuckDB でのみ検証
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では数値型を持たないためスキップ")
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.filter(F.col("amount") > 200.0)
"""
        result = convert_and_run(code)
        amounts = [float(row[result.columns.index("amount")]) for row in result.rows]
        self.assertTrue(all(a > 200.0 for a in amounts))
        self.assertEqual(len(amounts), 3)  # 300, 250, 400

    def test_select_columns(self):
        """カラム射影"""
        code = """
df = spark.table("raw.orders")
result = df.select("order_id", "customer_id", "amount")
"""
        result = convert_and_run(code)
        self.assertEqual(len(result.columns), 3)
        col_names_lower = [c.lower() for c in result.columns]
        self.assertIn("order_id", col_names_lower)
        self.assertIn("customer_id", col_names_lower)
        self.assertIn("amount", col_names_lower)

    def test_limit(self):
        """LIMIT"""
        code = """
df = spark.table("raw.orders")
result = df.limit(3)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 3)

    def test_distinct(self):
        """DISTINCT"""
        code = """
df = spark.table("raw.orders")
result = df.select("region").distinct()
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 3)  # East / West / North


class TestAggregationConversion(unittest.TestCase):
    """集計クエリの変換と実行テスト"""

    def test_count_by_region(self):
        """GROUP BY + COUNT"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.groupBy("region").agg(F.count("order_id").alias("cnt"))
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 3)
        # East: 4件, West: 3件, North: 3件
        rows_dict = {
            row[result.columns.index("region")]: int(row[result.columns.index("cnt")])
            for row in result.rows
        }
        self.assertEqual(rows_dict.get("East"), 4)
        self.assertEqual(rows_dict.get("West"), 3)
        self.assertEqual(rows_dict.get("North"), 3)

    def test_sum_by_region(self):
        """GROUP BY + SUM"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = (
    df
    .filter(F.col("status") == "active")
    .groupBy("region")
    .agg(F.sum("amount").alias("total_amount"))
)
"""
        result = convert_and_run(code)
        rows_dict = {
            row[result.columns.index("region")]: float(row[result.columns.index("total_amount")])
            for row in result.rows
        }
        # East: 150+200+250+175=775, West: 300+400=700, North: 120+90=210
        self.assertAlmostEqual(rows_dict.get("East"), 775.0)
        self.assertAlmostEqual(rows_dict.get("West"), 700.0)
        self.assertAlmostEqual(rows_dict.get("North"), 210.0)

    def test_multiple_aggs(self):
        """複数集計関数"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.groupBy("region").agg(
    F.count("order_id").alias("cnt"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg_amt"),
    F.max("amount").alias("max_amt"),
    F.min("amount").alias("min_amt"),
)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 3)
        col_lower = [c.lower() for c in result.columns]
        for expected_col in ["region", "cnt", "total", "avg_amt", "max_amt", "min_amt"]:
            self.assertIn(expected_col, col_lower, f"カラム '{expected_col}' が見つかりません")

    def test_count_distinct(self):
        """COUNT DISTINCT"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.groupBy("region").agg(
    F.countDistinct("customer_id").alias("uniq_customers")
)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 3)


class TestJoinConversion(unittest.TestCase):
    """JOIN クエリの変換と実行テスト"""

    def test_inner_join(self):
        """INNER JOIN"""
        code = """
import pyspark.sql.functions as F
orders = spark.table("raw.orders")
customers = spark.table("raw.customers")
result = orders.join(customers, on=["customer_id"], how="inner")
"""
        result = convert_and_run(code)
        # orders 10件 × customers (全マッチ) = 10件
        self.assertEqual(result.row_count, 10)
        col_lower = [c.lower() for c in result.columns]
        self.assertIn("name", col_lower)
        self.assertIn("tier", col_lower)

    def test_left_join_with_filter(self):
        """LEFT JOIN + フィルタ"""
        code = """
import pyspark.sql.functions as F
orders = spark.table("raw.orders")
customers = spark.table("raw.customers")
result = (
    orders
    .filter(F.col("status") == "active")
    .join(customers, on=["customer_id"], how="left")
    .select("order_id", "customer_id", "name", "region", "amount")
)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 8)


class TestWindowConversion(unittest.TestCase):
    """Window 関数の変換と実行テスト (sqlite は非対応なのでスキップ可)"""

    def setUp(self):
        # sqlite フォールバック時は Window 系をスキップ
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では Window 関数テストをスキップします")

    def test_row_number(self):
        """ROW_NUMBER"""
        code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window
df = spark.table("raw.orders")
w = Window.partitionBy("region").orderBy(F.col("amount").desc())
result = df.withColumn("rn", F.row_number().over(w))
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 10)
        # rn が 1 始まりになっていること
        rn_col = result.columns.index("rn")
        rn_values = [int(row[rn_col]) for row in result.rows]
        self.assertIn(1, rn_values)
        self.assertTrue(all(v >= 1 for v in rn_values))

    def test_rank(self):
        """RANK"""
        code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window
df = spark.table("raw.orders")
w = Window.partitionBy("region").orderBy(F.col("amount").desc())
result = df.withColumn("rnk", F.rank().over(w))
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 10)

    def test_window_with_filter(self):
        """Window + フィルタ (sample_basic.py パターン)"""
        code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window

orders = spark.table("raw.orders")
customers = spark.table("raw.customers")

active = orders.filter(F.col("status") == "active")
joined = active.join(customers, on=["customer_id"], how="left")

summary = (
    joined
    .groupBy("customer_id", "region")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("order_id").alias("order_count"),
    )
)

w = Window.partitionBy("region").orderBy(F.col("total_amount").desc())
ranked = summary.withColumn("rank", F.row_number().over(w))

result = (
    ranked
    .filter(F.col("rank") <= 5)
    .orderBy("region", "rank")
)
"""
        result = convert_and_run(code)
        # region ごとに最大 5 件、3 region なので最大 15 件
        self.assertLessEqual(result.row_count, 15)
        self.assertGreater(result.row_count, 0)
        rank_col = result.columns.index("rank")
        ranks = [int(row[rank_col]) for row in result.rows]
        self.assertTrue(all(r <= 5 for r in ranks))


class TestFunctionConversion(unittest.TestCase):
    """個別関数の変換と実行テスト"""

    def test_upper_lower(self):
        """文字列関数"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.customers")
result = df.select(
    "customer_id",
    F.upper(F.col("name")).alias("upper_name"),
    F.lower(F.col("tier")).alias("lower_tier"),
)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 5)
        upper_idx = result.columns.index("upper_name")
        # 大文字変換されているか確認
        upper_names = [row[upper_idx] for row in result.rows]
        self.assertTrue(all(n == n.upper() for n in upper_names if n))

    def test_coalesce(self):
        """COALESCE"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.select(
    "order_id",
    F.coalesce(F.col("region"), F.lit("Unknown")).alias("region_coalesced")
)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 10)

    def test_case_when(self):
        """CASE WHEN"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.select(
    "order_id",
    "amount",
    F.when(F.col("amount") >= 300, "high")
     .when(F.col("amount") >= 150, "medium")
     .otherwise("low")
     .alias("amount_tier")
)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 10)
        tier_col = result.columns.index("amount_tier")
        tiers = {row[tier_col] for row in result.rows}
        self.assertTrue(tiers.issubset({"high", "medium", "low"}))

    def test_with_column_arithmetic(self):
        """withColumn + 算術演算"""
        code = """
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = df.withColumn("amount_jpy", F.col("amount") * 150)
"""
        result = convert_and_run(code)
        self.assertEqual(result.row_count, 10)
        col_lower = [c.lower() for c in result.columns]
        self.assertIn("amount_jpy", col_lower)

    def test_drop_column(self):
        """DROP カラム"""
        code = """
df = spark.table("raw.orders")
result = df.drop("order_date")
"""
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では SELECT * EXCEPT が非対応")
        result = convert_and_run(code)
        col_lower = [c.lower() for c in result.columns]
        self.assertNotIn("order_date", col_lower)
        self.assertIn("order_id", col_lower)

    def test_rename_column(self):
        """RENAME カラム"""
        runner = DuckDBRunner()
        if runner._engine == "sqlite":
            self.skipTest("sqlite では SELECT * EXCEPT + alias が非対応")
        code = """
df = spark.table("raw.orders")
result = df.withColumnRenamed("amount", "price")
"""
        result = convert_and_run(code)
        col_lower = [c.lower() for c in result.columns]
        self.assertIn("price", col_lower)
        self.assertNotIn("amount", col_lower)

    def test_union_all(self):
        """UNION ALL"""
        code = """
import pyspark.sql.functions as F
df1 = spark.table("raw.orders").filter(F.col("region") == "East").select("order_id", "region")
df2 = spark.table("raw.orders").filter(F.col("region") == "West").select("order_id", "region")
result = df1.union(df2)
"""
        result = convert_and_run(code)
        # East 4件 + West 3件 = 7件
        self.assertEqual(result.row_count, 7)


class TestComparatorUnit(unittest.TestCase):
    """ResultComparator 単体テスト"""

    def _make_result(self, columns, rows, engine="test"):
        return QueryResult(
            columns=columns,
            rows=[tuple(r) for r in rows],
            row_count=len(rows),
            engine=engine,
        )

    def test_equal_results(self):
        """同一データは一致と判定"""
        left = self._make_result(["a", "b"], [[1, "x"], [2, "y"]])
        right = self._make_result(["a", "b"], [[1, "x"], [2, "y"]])
        cmp = ResultComparator()
        res = cmp.compare(left, right)
        self.assertTrue(res.is_equal)

    def test_row_order_ignored(self):
        """行順序が異なっても一致と判定"""
        left = self._make_result(["a", "b"], [[1, "x"], [2, "y"]])
        right = self._make_result(["a", "b"], [[2, "y"], [1, "x"]])
        cmp = ResultComparator(CompareConfig(ignore_row_order=True))
        res = cmp.compare(left, right)
        self.assertTrue(res.is_equal)

    def test_column_order_ignored(self):
        """カラム順序が異なっても一致と判定"""
        left = self._make_result(["a", "b"], [[1, "x"]])
        right = self._make_result(["b", "a"], [["x", 1]])
        cmp = ResultComparator(CompareConfig(ignore_column_order=True))
        res = cmp.compare(left, right)
        self.assertTrue(res.is_equal)

    def test_row_count_mismatch(self):
        """行数が違うと不一致"""
        left = self._make_result(["a"], [[1], [2], [3]])
        right = self._make_result(["a"], [[1], [2]])
        cmp = ResultComparator()
        res = cmp.compare(left, right)
        self.assertFalse(res.is_equal)
        self.assertFalse(res.row_count_match)

    def test_column_mismatch(self):
        """カラムが違うと不一致"""
        left = self._make_result(["a", "b"], [[1, 2]])
        right = self._make_result(["a", "c"], [[1, 2]])
        cmp = ResultComparator()
        res = cmp.compare(left, right)
        self.assertFalse(res.is_equal)
        self.assertFalse(res.column_match)

    def test_float_tolerance(self):
        """浮動小数点の許容誤差内は一致"""
        left = self._make_result(["v"], [[1.000000001]])
        right = self._make_result(["v"], [[1.000000002]])
        cmp = ResultComparator(CompareConfig(float_tolerance=1e-6))
        res = cmp.compare(left, right)
        self.assertTrue(res.is_equal)

    def test_summary_output(self):
        """summary() が文字列を返す"""
        left = self._make_result(["a"], [[1], [2]])
        right = self._make_result(["a"], [[1]])
        cmp = ResultComparator()
        res = cmp.compare(left, right)
        summary = res.summary()
        self.assertIsInstance(summary, str)
        self.assertIn("MISMATCH", summary)


class TestSQLAdaptation(unittest.TestCase):
    """SQL アダプター (BQ → DuckDB/SQLite 変換) のテスト"""

    def test_backtick_conversion(self):
        """バッククォートがダブルクォートに変換される"""
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="test.tbl",
            columns=[("id", "INT64"), ("val", "STRING")],
            data=[{"id": 1, "val": "hello"}, {"id": 2, "val": "world"}],
        ))
        result = runner.run("SELECT `id`, `val` FROM `test.tbl`")
        self.assertEqual(result.row_count, 2)

    def test_table_name_with_dot(self):
        """ドットを含むテーブル名が安全な名前に変換される"""
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="mydb.mytable",
            columns=[("x", "INT64")],
            data=[{"x": 42}],
        ))
        result = runner.run("SELECT `x` FROM `mydb.mytable`")
        self.assertEqual(result.row_count, 1)
        self.assertEqual(int(result.rows[0][0]), 42)  # sqlite は文字列で返す場合あり

    def test_where_clause(self):
        """WHERE 句が正しく動作する"""
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="t",
            columns=[("id", "INT64"), ("status", "STRING")],
            data=[
                {"id": 1, "status": "active"},
                {"id": 2, "status": "inactive"},
                {"id": 3, "status": "active"},
            ],
        ))
        result = runner.run("SELECT `id` FROM `t` WHERE `status` = 'active'")
        self.assertEqual(result.row_count, 2)

    def test_group_by_aggregate(self):
        """GROUP BY + 集計が動作する"""
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="sales",
            columns=[("region", "STRING"), ("amount", "FLOAT64")],
            data=[
                {"region": "East", "amount": 100},
                {"region": "East", "amount": 200},
                {"region": "West", "amount": 150},
            ],
        ))
        result = runner.run(
            "SELECT `region`, SUM(`amount`) AS `total` "
            "FROM `sales` GROUP BY `region`"
        )
        self.assertEqual(result.row_count, 2)

    def test_join_query(self):
        """JOIN が動作する"""
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="a",
            columns=[("id", "INT64"), ("val", "STRING")],
            data=[{"id": 1, "val": "foo"}, {"id": 2, "val": "bar"}],
        ))
        runner.register_table(TableSchema(
            name="b",
            columns=[("id", "INT64"), ("extra", "STRING")],
            data=[{"id": 1, "extra": "baz"}],
        ))
        result = runner.run(
            "SELECT `a`.`id`, `val`, `extra` "
            "FROM `a` LEFT JOIN `b` USING (id)"
        )
        self.assertEqual(result.row_count, 2)

    def test_cte_query(self):
        """WITH (CTE) が動作する"""
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="orders",
            columns=[("id", "INT64"), ("amount", "FLOAT64")],
            data=[
                {"id": 1, "amount": 100},
                {"id": 2, "amount": 200},
                {"id": 3, "amount": 50},
            ],
        ))
        result = runner.run("""
WITH big_orders AS (
  SELECT `id`, `amount`
  FROM `orders`
  WHERE `amount` >= 100
)
SELECT COUNT(*) AS cnt FROM big_orders
""")
        self.assertEqual(int(result.rows[0][0]), 3)  # 100, 200, 150 全て >= 100


if __name__ == "__main__":
    unittest.main(verbosity=2)
